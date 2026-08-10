package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"html"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"picoman/internal/agent"
	"picoman/internal/config"
	"picoman/internal/outbox"
	"picoman/internal/tg"
)

func runDaemon() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if cfg.TelegramToken == "" {
		log.Fatal("tg_token is required; run picoman setup")
	}
	if len(cfg.AllowedUsers) == 0 {
		log.Fatal("tg_allowed_users is required; run picoman setup")
	}
	bot := tg.New(cfg.TelegramToken)
	if err := config.LoadHostDB(cfg); err != nil {
		criticalNotifyUsers(cfg, bot, "hostdb", err)
		log.Fatalf("load host db: %v", err)
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		criticalNotifyUsers(cfg, bot, "workdir", err)
		log.Fatalf("create work dir: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := agent.New(cfg.KeyPath, config.MaxTTL(cfg))
	st.CleanStart()
	cleanup := agent.CleanLegacy(cfg.LegacyAgentSocket)
	out, err := outbox.Open(config.DBPath(), bot)
	if err != nil {
		criticalNotifyUsers(cfg, bot, "outbox", err)
		log.Fatalf("open outbox: %v", err)
	}
	defer out.Close()
	out.SetAlertSink(func(text string) {
		go func() {
			for _, uid := range cfg.AllowedUsers {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_ = bot.SendMessage(ctx, uid, text)
				cancel()
			}
		}()
	})

	outboxCtx, stopOutbox := context.WithCancel(context.Background())
	defer stopOutbox()
	go out.Run(outboxCtx)
	audit := newAuditState(cfg.LogLevel)
	go runControl(ctx, cfg, st, out, bot, audit)
	go watchUnlockExpiry(ctx, st, out, cfg, bot)

	marker := readRestartMarker()
	if marker.Reason == "update" {
		notify(out, cfg, bot, false, infoText(updateLifecycleText(marker)))
	} else {
		notify(out, cfg, bot, false, infoText(lifecycleText("started", cleanup)))
	}

	// Auto-unseal in a goroutine so startup is not blocked on a potentially
	// slow or interactive unseal command. Notifies separately when done.
	go startupAutoUnseal(ctx, cfg, st, out, bot)

	// Menu is decorative; publish it in the background so a slow setMyCommands
	// never delays the Telegram control loop.
	go func() {
		if err := bot.SetMyCommands(ctx, menuCommands); err != nil {
			log.Printf("set commands: %v", err)
		}
	}()

	offset := out.TelegramOffset()
	backoff := time.Second
	for {
		updates, err := bot.GetUpdates(ctx, offset)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("getUpdates: %v (retry in %s)", err, backoff)
			if !sleepWithCtx(ctx, backoff) {
				break
			}
			backoff *= 2
			if backoff > time.Minute {
				backoff = time.Minute
			}
			continue
		}
		backoff = time.Second
		for _, upd := range updates {
			offset = upd.UpdateID + 1
			if upd.Message.Text == "" {
				continue
			}
			handleMessage(ctx, out, cfg, st, audit, bot, upd.Message)
		}
		if len(updates) > 0 {
			// SetTelegramOffset emits its own alert (via outbox sink) on error.
			_ = out.SetTelegramOffset(offset)
		}
	}

	cleanup = st.CleanStart()
	notify(out, cfg, bot, false, infoText(lifecycleText("stopped", cleanup)))
	// Stop the Run goroutine first so it doesn't race Flush on next().
	stopOutbox()
	<-out.Done()
	flushOutbox(out)
}

func lifecycleText(event string, cleanup agent.CleanResult) string {
	text := "picoman " + version + " " + event
	if !cleanup.OK() {
		text += "\n\n" + cleanup.String()
	}
	return text
}

func updateLifecycleText(marker restartMarker) string {
	if marker.From != "" && marker.To != "" {
		return "picoman updated " + marker.From + " -> " + marker.To
	}
	return "picoman " + version + " updated"
}

// notify broadcasts text to all allowed users through the outbox.
// On enqueue failure (outbox itself broken) falls back to direct send.
func notify(out *outbox.Store, cfg *config.Config, bot *tg.Client, html bool, text string) {
	enqueue := out.Enqueue
	if html {
		enqueue = out.EnqueueHTML
	}
	for _, userID := range cfg.AllowedUsers {
		if err := enqueue(userID, text); err != nil {
			log.Printf("enqueue notify user=%d: %v", userID, err)
			go criticalNotifyUser(userID, bot, "outbox", err)
		}
	}
}

type actionMessage struct {
	chatID    int64
	messageID int64
	replyToID int64
}

func sendActionStart(cfg *config.Config, bot *tg.Client, html bool, text string) []actionMessage {
	sent := make([]actionMessage, 0, len(cfg.AllowedUsers))
	for _, userID := range cfg.AllowedUsers {
		msg, err := sendDirectMessage(bot, userID, 0, html, text)
		if err != nil {
			log.Printf("send action start user=%d: %v", userID, err)
			continue
		}
		sent = append(sent, actionMessage{chatID: userID, messageID: msg.MessageID})
	}
	return sent
}

func sendActionReplyStart(bot *tg.Client, msg tg.Message, html bool, text string) []actionMessage {
	sent, err := sendDirectMessage(bot, msg.Chat.ID, msg.MessageID, html, text)
	if err != nil {
		log.Printf("send action reply start chat=%d: %v", msg.Chat.ID, err)
		return nil
	}
	return []actionMessage{{chatID: msg.Chat.ID, messageID: sent.MessageID, replyToID: msg.MessageID}}
}

func editActionMessages(out *outbox.Store, bot *tg.Client, messages []actionMessage, html bool, text string) {
	if len(messages) == 0 {
		return
	}
	for _, msg := range messages {
		if err := editDirectMessage(bot, msg.chatID, msg.messageID, html, text); err != nil {
			log.Printf("edit action message chat=%d message=%d: %v", msg.chatID, msg.messageID, err)
			if msg.replyToID > 0 {
				enqueueReplyTo(out, bot, msg.chatID, msg.replyToID, cmdReply{text: text, html: html})
			} else {
				enqueueNotify(out, bot, msg.chatID, html, text)
			}
		}
	}
}

func sendDirectMessage(bot *tg.Client, chatID, replyToID int64, html bool, text string) (tg.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	switch {
	case html && replyToID > 0:
		return bot.SendHTMLReplyResult(ctx, chatID, replyToID, text)
	case html:
		return bot.SendHTMLResult(ctx, chatID, text)
	case replyToID > 0:
		return bot.SendReplyResult(ctx, chatID, replyToID, text)
	default:
		return bot.SendMessageResult(ctx, chatID, text)
	}
}

func editDirectMessage(bot *tg.Client, chatID, messageID int64, html bool, text string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if html {
		return bot.EditHTMLMessage(ctx, chatID, messageID, text)
	}
	return bot.EditMessage(ctx, chatID, messageID, text)
}

func enqueueNotify(out *outbox.Store, bot *tg.Client, userID int64, html bool, text string) {
	if text == "" {
		return
	}
	var err error
	if html {
		err = out.EnqueueHTML(userID, text)
	} else {
		err = out.Enqueue(userID, text)
	}
	if err != nil {
		log.Printf("enqueue notify user=%d: %v", userID, err)
		go criticalNotifyUser(userID, bot, "outbox", err)
	}
}

func criticalNotifyUsers(cfg *config.Config, bot *tg.Client, name string, err error) {
	for _, userID := range cfg.AllowedUsers {
		criticalNotifyUser(userID, bot, name, err)
	}
}

// criticalNotifyUser sends a direct (non-outbox) message and retries forever.
// Used only when the outbox itself failed and we need to bypass it.
func criticalNotifyUser(userID int64, bot *tg.Client, name string, err error) {
	text := "❌ " + name + " error: " + shortError(err)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		sendErr := bot.SendMessage(ctx, userID, text)
		cancel()
		if sendErr == nil {
			return
		}
		log.Printf("critical notify failed user=%d: %v", userID, sendErr)
		time.Sleep(5 * time.Second)
	}
}

// defaultAskpassCommand is used when neither key_passphrase nor
// key_passphrase_command is configured. It prompts on /dev/tty with echo
// disabled and prints the passphrase to stdout. Works for `picoman unseal`
// invoked from a terminal; fails when no tty is attached (daemon under
// systemd-user). Users can override by setting key_passphrase_command.
const defaultAskpassCommand = `stty -echo </dev/tty && ` +
	`trap 'stty echo </dev/tty' EXIT INT TERM && ` +
	`printf 'picoman passphrase: ' >/dev/tty && ` +
	`IFS= read -r p </dev/tty && ` +
	`printf '\n' >/dev/tty && ` +
	`printf %s "$p"`

// configuredUnseal returns the passphrase from config: key_passphrase if set,
// otherwise key_passphrase_command's stdout. Both fields populated at once is
// rejected at config-load time, so at most one source is consulted here.
// Returns ("", nil) when neither is set — the caller decides whether to fall
// back to an interactive prompt.
func configuredUnseal(ctx context.Context, cfg *config.Config) (string, error) {
	if cfg.KeyPassphrase != "" {
		return cfg.KeyPassphrase, nil
	}
	cmdline := strings.TrimSpace(cfg.KeyPassphraseCommand)
	if cmdline == "" {
		return "", nil
	}
	return runUnsealCommand(ctx, cmdline)
}

// interactiveUnseal returns the passphrase from configuredUnseal, and if
// nothing is configured runs the default tty-prompt command. Used by CLI
// when no arg/pipe is supplied. If the default prompt fails because no tty
// is available, returns a plain "no controlling terminal" error instead of
// the raw shell output.
func interactiveUnseal(ctx context.Context, cfg *config.Config) (string, error) {
	if p, err := configuredUnseal(ctx, cfg); err != nil || p != "" {
		return p, err
	}
	p, err := runUnsealCommand(ctx, defaultAskpassCommand)
	if err != nil {
		return "", fmt.Errorf("no controlling terminal for default unseal prompt; set key_passphrase or key_passphrase_command")
	}
	return p, nil
}

// startupAutoUnseal performs the configured auto-unseal in the background.
// Daemon-startup has no tty, so the default-askpass fallback is intentionally
// skipped here — if nothing is configured the daemon stays sealed silently.
func startupAutoUnseal(ctx context.Context, cfg *config.Config, st *agent.State, out *outbox.Store, bot *tg.Client) {
	passphrase, err := configuredUnseal(ctx, cfg)
	if err != nil {
		notify(out, cfg, bot, false, errorText("unseal failed: "+err.Error()))
		return
	}
	if passphrase == "" {
		return
	}
	if err := st.Unseal(passphrase); err != nil {
		notify(out, cfg, bot, false, errorText("unseal failed: "+err.Error()))
		return
	}
	notify(out, cfg, bot, false, unsealText())
	startupDeveloperAutoUnlock(cfg, st, out, bot)
}

func startupDeveloperAutoUnlock(cfg *config.Config, st *agent.State, out *outbox.Store, bot *tg.Client) {
	ttl, ok := developerAutoUnlockTTL(cfg)
	if !ok {
		return
	}
	if err := st.Unlock(ttl); err != nil {
		notify(out, cfg, bot, false, errorText("unlock failed: "+err.Error()))
		return
	}
	notify(out, cfg, bot, false, unlockedText(st))
}

func developerAutoUnlockTTL(cfg *config.Config) (time.Duration, bool) {
	if strings.TrimSpace(cfg.DeveloperDir) == "" {
		return 0, false
	}
	return config.MaxTTL(cfg), true
}

func runUnsealCommand(ctx context.Context, cmdline string) (string, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdline)
	// Inherit stdin so interactive helpers (askpass-tty, pinentry,
	// systemd-ask-password) can talk to the user's terminal when one exists.
	cmd.Stdin = os.Stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return "", fmt.Errorf("unseal command: %w: %s", err, msg)
		}
		return "", fmt.Errorf("unseal command: %w", err)
	}
	return strings.TrimRight(stdout.String(), "\r\n"), nil
}

// sleepWithCtx waits for d or until ctx is cancelled. Returns false if cancelled.
func sleepWithCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func shortError(err error) string {
	text := strings.TrimSpace(fmt.Sprint(err))
	runes := []rune(text)
	if len(runes) > 300 {
		text = string(runes[:300]) + "..."
	}
	return text
}

func flushOutbox(out *outbox.Store) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out.Flush(ctx)
}

func watchUnlockExpiry(ctx context.Context, st *agent.State, out *outbox.Store, cfg *config.Config, bot *tg.Client) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var activeUntil time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		until := st.Until()
		if until.IsZero() {
			activeUntil = time.Time{}
			continue
		}
		if time.Now().Before(until) {
			activeUntil = until
			continue
		}
		if activeUntil.IsZero() || !activeUntil.Equal(until) {
			continue
		}
		if err := st.Lock(); err != nil {
			notify(out, cfg, bot, false, errorText("lock failed: "+err.Error()))
		} else {
			notify(out, cfg, bot, false, "🔒 locked")
		}
		activeUntil = time.Time{}
	}
}

type cmdCtx struct {
	ctx   context.Context
	cfg   *config.Config
	st    *agent.State
	audit *auditState
	bot   *tg.Client
}

type cmdReply struct {
	text string
	html bool
}

type cmdHandler func(c cmdCtx, fields []string) (cmdReply, error)

type cmdEntry struct {
	fn    cmdHandler
	async bool
}

const builtinAllGroup = "all"
const maxParallelGroupRuns = 16

var commands = map[string]cmdEntry{
	"start":    {fn: cmdHelp},
	"help":     {fn: cmdHelp},
	"status":   {fn: cmdStatus},
	"hosts":    {fn: cmdHosts},
	"host":     {fn: cmdHost},
	"groups":   {fn: cmdGroupList},
	"group":    {fn: cmdGroup},
	"unseal":   {fn: cmdUnseal, async: true},
	"unlock":   {fn: cmdUnlock},
	"seal":     {fn: cmdSeal},
	"lock":     {fn: cmdLock},
	"update":   {fn: cmdUpdate, async: true},
	"run":      {fn: cmdRun, async: true},
	"get":      {fn: cmdGet, async: true},
	"put":      {fn: cmdPut, async: true},
	"loglevel": {fn: cmdLogLevel},
	"menu":     {fn: cmdMenu, async: true},
}

func handleMessage(ctx context.Context, out *outbox.Store, cfg *config.Config, st *agent.State, audit *auditState, bot *tg.Client, msg tg.Message) {
	if !config.AllowedSet(cfg)[msg.From.ID] {
		// Silent drop: replying "denied" leaks bot existence to anyone who
		// pings the chat. Journal record is enough for forensics.
		log.Printf("deny user=%d username=%s command=%q", msg.From.ID, msg.From.Username, logCommandName(msg.Text))
		return
	}

	fields := strings.Fields(strings.TrimSpace(msg.Text))
	if len(fields) == 0 {
		return
	}
	name, fields := normalizeCommandFields(fields)

	if isVersionCommand(name) {
		go handleInstallVersionMessage(out, bot, msg, tagFromVersionCommand(name))
		return
	}

	entry, ok := commands[name]
	if !ok {
		enqueueReply(out, bot, msg, cmdReply{text: warningText("unknown command\n\n" + botHelpText())})
		return
	}

	c := cmdCtx{ctx: ctx, cfg: cfg, st: st, audit: audit, bot: bot}
	run := func() {
		reply, err := entry.fn(c, fields)
		logCommand(msg, err)
		if reply.text == "" && err != nil {
			reply.text = errorText(err.Error())
		}
		enqueueReply(out, bot, msg, reply)
	}
	if entry.async {
		if start := commandStartText(audit, name, fields); start != "" {
			go func() {
				startedCh := make(chan []actionMessage, 1)
				go func() {
					startedCh <- sendActionReplyStart(bot, msg, true, start)
				}()
				reply, err := entry.fn(c, fields)
				logCommand(msg, err)
				if reply.text == "" && err != nil {
					reply.text = errorText(err.Error())
				}
				started := <-startedCh
				if len(started) > 0 {
					editActionMessages(out, bot, started, reply.html, reply.text)
					return
				}
				enqueueReply(out, bot, msg, reply)
			}()
			return
		}
		go run()
		return
	}
	run()
}

func commandStartText(audit *auditState, name string, fields []string) string {
	switch name {
	case "run":
		if len(fields) < 3 {
			return ""
		}
		return runStartAuditText(audit, fields[1], strings.Join(fields[2:], " "))
	case "get":
		if len(fields) < 3 {
			return ""
		}
		localName := defaultTransferName(fields[2])
		if len(fields) >= 4 {
			localName = fields[3]
		}
		return transferStartAuditText(audit, "⬅️ get", fields[1], fields[2], localName)
	case "put":
		if len(fields) < 3 {
			return ""
		}
		remoteName := defaultTransferName(fields[2])
		if len(fields) >= 4 {
			remoteName = fields[3]
		}
		return transferStartAuditText(audit, "➡️ put", fields[1], fields[2], remoteName)
	default:
		return ""
	}
}

func logCommand(msg tg.Message, err error) {
	name := logCommandName(msg.Text)
	if err != nil {
		log.Printf("command error user=%d command=%q err=%v", msg.From.ID, name, err)
		return
	}
	log.Printf("command ok user=%d command=%q", msg.From.ID, name)
}

func logCommandName(text string) string {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 {
		return ""
	}
	name, _ := normalizeCommandFields(fields)
	return name
}

func enqueueReply(out *outbox.Store, bot *tg.Client, msg tg.Message, r cmdReply) {
	enqueueReplyTo(out, bot, msg.Chat.ID, msg.MessageID, r)
}

func enqueueReplyTo(out *outbox.Store, bot *tg.Client, chatID, replyToID int64, r cmdReply) {
	if r.text == "" {
		return
	}
	var err error
	if r.html {
		err = out.EnqueueHTMLReply(chatID, replyToID, r.text)
	} else {
		err = out.EnqueueReply(chatID, replyToID, r.text)
	}
	if err != nil {
		log.Printf("enqueue reply: %v", err)
		go criticalNotifyUser(chatID, bot, "outbox", err)
	}
}

func cmdHelp(_ cmdCtx, _ []string) (cmdReply, error) {
	return cmdReply{text: infoText(botHelpText())}, nil
}

func cmdStatus(c cmdCtx, _ []string) (cmdReply, error) {
	return cmdReply{text: infoText(statusText(c.st))}, nil
}

func cmdHostList(c cmdCtx, _ []string) (cmdReply, error) {
	return cmdReply{text: infoText(hostsText(c.cfg)), html: true}, nil
}

func cmdHosts(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) == 1 || fields[1] == "list" {
		return cmdHostList(c, fields)
	}
	return cmdReply{}, errors.New("usage: hosts")
}

func cmdHost(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) < 2 {
		return cmdReply{}, errors.New(hostCommandsHelp())
	}
	switch fields[1] {
	case "list":
		if len(fields) != 2 {
			return cmdReply{}, errors.New("usage: host list")
		}
		return cmdHostList(c, fields)
	case "info":
		return cmdHostInfo(c, fields[2:])
	case "note":
		text, err := setHostNote(fields[2:], c.cfg)
		if err != nil {
			return cmdReply{html: true}, err
		}
		return cmdReply{text: text, html: true}, nil
	case "set":
		text, err := setHostField(fields[2:], c.cfg)
		if err != nil {
			return cmdReply{html: true}, err
		}
		return cmdReply{text: text, html: true}, nil
	case "add":
		return hostAdd(c.cfg, fields[2:])
	case "rm", "remove":
		text, err := removeHost(fields[2:], c.cfg)
		if err != nil {
			return cmdReply{html: true}, err
		}
		return cmdReply{text: text, html: true}, nil
	default:
		return cmdReply{}, errors.New(hostCommandsHelp())
	}
}

func cmdHostInfo(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) != 1 {
		return cmdReply{}, errors.New("usage: host info <name>")
	}
	name := fields[0]
	target, ok := c.cfg.Target(name)
	if !ok {
		return cmdReply{text: "❌ unknown host " + hostNameText(name), html: true}, fmt.Errorf("unknown host %q", name)
	}
	return cmdReply{text: infoText(hostText(name, target)), html: true}, nil
}

func cmdGroupList(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) == 1 || fields[1] == "list" {
		return cmdReply{text: infoText(groupsText(c.cfg)), html: true}, nil
	}
	return cmdReply{}, errors.New("usage: groups")
}

func cmdGroup(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) < 2 {
		return cmdReply{}, errors.New(groupCommandsHelp())
	}
	switch fields[1] {
	case "list":
		if len(fields) != 2 {
			return cmdReply{}, errors.New("usage: group list")
		}
		return cmdGroupList(c, fields)
	case "info":
		return cmdGroupInfo(c, fields[2:])
	case "add":
		return cmdGroupModify(c, "add", fields[2:])
	case "rm", "remove":
		return cmdGroupModify(c, "remove", fields[2:])
	}
	return cmdReply{}, errors.New(groupCommandsHelp())
}

func cmdGroupInfo(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) != 1 {
		return cmdReply{}, errors.New("usage: group info <target-expression>")
	}
	text, err := groupInfoText(c.cfg, fields[0], true)
	if err != nil {
		return cmdReply{}, err
	}
	return cmdReply{text: infoText(text), html: true}, nil
}

func cmdGroupModify(c cmdCtx, action string, fields []string) (cmdReply, error) {
	if len(fields) != 2 {
		return cmdReply{}, errors.New("usage: group " + action + " @<group> <host>")
	}
	group, err := parseGroupSelector(fields[0])
	if err != nil {
		return cmdReply{}, err
	}
	if group == builtinAllGroup {
		return cmdReply{}, errors.New("cannot modify built-in group @all")
	}
	host := fields[1]
	switch action {
	case "add":
		if _, err := c.cfg.AddHostGroup(host, group); err != nil {
			return cmdReply{}, err
		}
		return cmdReply{text: successText("group @" + html.EscapeString(group) + "\nadded " + hostNameText(host)), html: true}, nil
	case "remove":
		if _, err := c.cfg.RemoveHostGroup(host, group); err != nil {
			return cmdReply{}, err
		}
		return cmdReply{text: successText("group @" + html.EscapeString(group) + "\nremoved " + hostNameText(host)), html: true}, nil
	default:
		return cmdReply{}, errors.New("usage: group add @<group> <host> | group remove @<group> <host>")
	}
}

func hostAdd(cfg *config.Config, args []string) (cmdReply, error) {
	switch len(args) {
	case 0:
		line, err := hostBootstrapLine(cfg, "")
		if err != nil {
			return cmdReply{}, err
		}
		return cmdReply{text: "<pre><code>" + html.EscapeString(line) + "</code></pre>", html: true}, nil
	case 1:
		if !config.ValidName(args[0]) {
			return cmdReply{}, fmt.Errorf("bad host name %q", args[0])
		}
		line, err := hostBootstrapLine(cfg, args[0])
		if err != nil {
			return cmdReply{}, err
		}
		return cmdReply{text: "<pre><code>" + html.EscapeString(line) + "</code></pre>", html: true}, nil
	default:
		text, err := addHostFromFields(args, cfg)
		if err != nil {
			return cmdReply{html: true}, err
		}
		return cmdReply{text: text, html: true}, nil
	}
}

var menuCommands = []tg.BotCommand{
	{Command: "status", Description: "\u0441\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435"},
	{Command: "unlock", Description: "\u0440\u0430\u0437\u0431\u043b\u043e\u043a\u0438\u0440\u043e\u0432\u0430\u0442\u044c (5m)"},
	{Command: "unlock_max", Description: "\u0440\u0430\u0437\u0431\u043b\u043e\u043a\u0438\u0440\u043e\u0432\u0430\u0442\u044c (max)"},
	{Command: "lock", Description: "\u0437\u0430\u0431\u043b\u043e\u043a\u0438\u0440\u043e\u0432\u0430\u0442\u044c"},
}

func cmdMenu(c cmdCtx, _ []string) (cmdReply, error) {
	if err := c.bot.SetMyCommands(c.ctx, menuCommands); err != nil {
		return cmdReply{}, err
	}
	return cmdReply{text: "\u2705 menu set"}, nil
}

func cmdUnlock(c cmdCtx, fields []string) (cmdReply, error) {
	text, err := handleUnlock(fields, c.st, config.MaxTTL(c.cfg))
	return cmdReply{text: text}, err
}

func cmdUnseal(c cmdCtx, _ []string) (cmdReply, error) {
	// Telegram has no tty for prompting, so fall back to configuredUnseal
	// only — no default askpass-tty.
	passphrase, err := configuredUnseal(c.ctx, c.cfg)
	if err != nil {
		return cmdReply{}, err
	}
	if passphrase == "" {
		return cmdReply{}, errors.New("no key_passphrase or key_passphrase_command configured")
	}
	if err := c.st.Unseal(passphrase); err != nil {
		return cmdReply{}, err
	}
	return cmdReply{text: unsealText()}, nil
}

func cmdSeal(c cmdCtx, _ []string) (cmdReply, error) {
	if err := c.st.Lock(); err != nil {
		return cmdReply{}, err
	}
	c.st.Seal()
	return cmdReply{text: "⚪ sealed"}, nil
}

func cmdLock(c cmdCtx, _ []string) (cmdReply, error) {
	if err := c.st.Lock(); err != nil {
		return cmdReply{}, err
	}
	return cmdReply{text: "🔒 locked"}, nil
}

func cmdUpdate(_ cmdCtx, _ []string) (cmdReply, error) {
	text, err := updateText()
	if err != nil {
		return cmdReply{}, err
	}
	return cmdReply{text: infoText(text), html: true}, nil
}

func cmdRun(c cmdCtx, fields []string) (cmdReply, error) {
	output, command, target, exitCode, err := handleRun(c.ctx, c.cfg, c.st, fields)
	if command == "" {
		return cmdReply{html: true}, err
	}
	// Transport-level failures: ssh couldn't run at all, or returned 255
	// (its own signal for connect/auth/protocol error). Everything else is
	// the remote command's own exit code — propagate transparently.
	if err != nil {
		return cmdReply{
			text: runAuditText(c.audit, target, command, output, err.Error()),
			html: true,
		}, err
	}
	if exitCode == 255 {
		return cmdReply{
			text: runAuditText(c.audit, target, command, output, "ssh: exit status 255 (connect/auth/protocol)"),
			html: true,
		}, errors.New("ssh: exit status 255")
	}
	return cmdReply{text: runAuditText(c.audit, target, command, output, ""), html: true}, nil
}

func cmdGet(c cmdCtx, fields []string) (cmdReply, error) {
	text, err := handleGet(c.ctx, c.cfg, c.st, fields)
	if err == nil {
		if len(fields) >= 2 && !auditFull(c.audit) {
			return cmdReply{text: actionText("⬅️ get", fields[1]), html: true}, nil
		}
		return cmdReply{text: text, html: true}, nil
	}
	if len(fields) < 3 {
		return cmdReply{html: true}, err
	}
	localName := defaultTransferName(fields[2])
	if len(fields) >= 4 {
		localName = fields[3]
	}
	return cmdReply{
		text: transferAuditText(c.audit, "⬅️ get", fields[1], fields[2], localName, err.Error()),
		html: true,
	}, err
}

func cmdPut(c cmdCtx, fields []string) (cmdReply, error) {
	text, err := handlePut(c.ctx, c.cfg, c.st, fields)
	if err == nil {
		if len(fields) >= 2 && !auditFull(c.audit) {
			return cmdReply{text: actionText("➡️ put", fields[1]), html: true}, nil
		}
		return cmdReply{text: text, html: true}, nil
	}
	if len(fields) < 3 {
		return cmdReply{html: true}, err
	}
	remoteName := defaultTransferName(fields[2])
	if len(fields) >= 4 {
		remoteName = fields[3]
	}
	return cmdReply{
		text: transferAuditText(c.audit, "➡️ put", fields[1], fields[2], remoteName, err.Error()),
		html: true,
	}, err
}

func cmdLogLevel(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) != 2 {
		return cmdReply{}, errors.New("usage: loglevel <chat|all>")
	}
	level := fields[1]
	if err := setLogLevel(c.cfg, c.audit, level); err != nil {
		return cmdReply{}, err
	}
	return cmdReply{text: "⚙️ loglevel " + level}, nil
}

func setLogLevel(cfg *config.Config, audit *auditState, level string) error {
	if level != "chat" && level != "all" {
		return errors.New("bad loglevel")
	}
	if cfg != nil {
		if err := cfg.SetLogLevel(level); err != nil {
			return fmt.Errorf("save loglevel: %w", err)
		}
	}
	if audit != nil {
		audit.SetLogLevel(level)
	}
	return nil
}

func commandName(s string) string {
	return strings.TrimLeft(strings.ToLower(s), "/")
}

func normalizeCommandFields(fields []string) (string, []string) {
	name := commandName(fields[0])
	if isVersionCommand(name) {
		return name, fields
	}
	parts := strings.Split(name, "_")
	if len(parts) == 1 {
		return name, fields
	}
	if _, ok := commands[parts[0]]; !ok {
		return name, fields
	}
	normalized := make([]string, 0, len(parts)+len(fields)-1)
	normalized = append(normalized, "/"+parts[0])
	normalized = append(normalized, parts[1:]...)
	normalized = append(normalized, fields[1:]...)
	return parts[0], normalized
}

func botHelpText() string {
	return strings.TrimSpace(`
commands:
/unseal
/unlock 5m
/unlock
/unlock_1h
/unlock_max
/seal
/lock
/status
/update
/host list
/host info <name>
/host note <name> [note]
/host set <name> remote_work_dir [path]
/host add
/host remove <name>
/groups
/group list
/group info <target-expression>
/group add @<group> <host>
/group remove @<group> <host>
/run <target> <command>
/get <target> <remote-file> [local-file]
/put <target> <local-file> [remote-file]
/loglevel <chat|all>
/menu
`)
}

func handleUnlock(fields []string, st *agent.State, maxTTL time.Duration) (string, error) {
	ttl := 5 * time.Minute
	if len(fields) > 2 {
		return "", errors.New("usage: /unlock [5m]")
	}
	if len(fields) == 2 {
		if fields[1] == "max" {
			ttl = maxTTL
		} else {
			var err error
			ttl, err = time.ParseDuration(fields[1])
			if err != nil {
				return "", fmt.Errorf("bad ttl: %w", err)
			}
		}
	}

	if err := st.Unlock(ttl); err != nil {
		return "", err
	}
	return unlockedText(st), nil
}

func unlockedText(st *agent.State) string {
	return "🟡 unlocked (" + leftText(st.Until()) + ")"
}

func statusText(st *agent.State) string {
	var lines []string
	lines = append(lines, "picoman "+version)
	if st.Sealed() {
		lines = append(lines, "⚪ sealed")
	} else {
		lines = append(lines, "🟡 unsealed")
	}
	if st.IsUnlocked() {
		lines = append(lines, "🟡 unlocked ("+leftText(st.Until())+")")
	} else {
		lines = append(lines, "🔒 locked")
	}
	return strings.Join(lines, "\n")
}

func leftText(until time.Time) string {
	left := time.Until(until)
	if left <= 0 {
		return "0s left"
	}
	seconds := int((left + time.Second - time.Nanosecond) / time.Second)
	if seconds <= 60 {
		return fmt.Sprintf("%ds left", seconds)
	}
	minutes := seconds / 60
	seconds %= 60
	if minutes < 60 {
		if seconds == 0 {
			return fmt.Sprintf("%dm left", minutes)
		}
		return fmt.Sprintf("%dm %ds left", minutes, seconds)
	}
	hours := minutes / 60
	minutes %= 60
	if minutes == 0 {
		return fmt.Sprintf("%dh left", hours)
	}
	return fmt.Sprintf("%dh %dm left", hours, minutes)
}

func hostsText(cfg *config.Config) string {
	targets := cfg.AllTargets()
	if len(targets) == 0 {
		return "host list empty"
	}
	names := make([]string, 0, len(targets))
	for name := range targets {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := []string{"host list"}
	for _, name := range names {
		lines = append(lines, hostListLine(name, targets[name]))
	}
	return strings.Join(lines, "\n")
}

func hostListLine(name string, target config.Target) string {
	text := "- " + hostNameText(name)
	if target.Note != "" {
		text += " (" + html.EscapeString(target.Note) + ")"
	}
	return text
}

func hostText(name string, target config.Target) string {
	port := target.Port
	if port == 0 {
		port = 22
	}
	state := ""
	if target.Disabled {
		state = "\ndisabled"
	}
	note := ""
	if target.Note != "" {
		note = "\n" + html.EscapeString(target.Note)
	}
	groups := ""
	if len(target.Groups) > 0 {
		escaped := make([]string, 0, len(target.Groups))
		for _, group := range target.Groups {
			escaped = append(escaped, "@"+html.EscapeString(group))
		}
		groups = "\ngroups: " + strings.Join(escaped, ", ")
	}
	remoteWorkDir := ""
	if target.RemoteWorkDir != "" {
		remoteWorkDir = "\nremote_work_dir: " + html.EscapeString(target.RemoteWorkDir)
	}
	return fmt.Sprintf("%s\n%s@%s:%d%s",
		hostNameText(name),
		html.EscapeString(target.User),
		html.EscapeString(target.Host),
		port,
		state+note+groups+remoteWorkDir,
	)
}

func groupsText(cfg *config.Config) string {
	names := cfg.GroupNames()
	lines := []string{"groups list", "- @all"}
	for _, name := range names {
		if name == builtinAllGroup {
			continue
		}
		lines = append(lines, "- @"+html.EscapeString(name))
	}
	return strings.Join(lines, "\n")
}

func groupText(cfg *config.Config, group string) string {
	names := groupHosts(cfg, group)
	if len(names) == 0 {
		return "group @" + html.EscapeString(group) + " empty"
	}
	lines := []string{"group @" + html.EscapeString(group)}
	for _, name := range names {
		lines = append(lines, "- "+hostNameText(name))
	}
	return strings.Join(lines, "\n")
}

func groupInfoText(cfg *config.Config, selector string, htmlOutput bool) (string, error) {
	if strings.HasPrefix(selector, "@") && !strings.ContainsAny(selector, ",+^") {
		group, err := parseGroupSelector(selector)
		if err != nil {
			return "", err
		}
		if htmlOutput {
			return groupText(cfg, group), nil
		}
		return plainGroupText(cfg, group), nil
	}
	names, err := hostsForTargetExpr(cfg, selector)
	if err != nil {
		return "", err
	}
	header := "group expression " + selector
	if htmlOutput {
		header = "group expression " + html.EscapeString(selector)
	}
	if len(names) == 0 {
		return header + " empty", nil
	}
	lines := []string{header}
	for _, name := range names {
		if htmlOutput {
			lines = append(lines, "- "+hostNameText(name))
		} else {
			lines = append(lines, "- "+name)
		}
	}
	return strings.Join(lines, "\n"), nil
}

func plainGroupText(cfg *config.Config, group string) string {
	names := groupHosts(cfg, group)
	if len(names) == 0 {
		return "group @" + group + " empty"
	}
	lines := []string{"group @" + group}
	for _, name := range names {
		lines = append(lines, "- "+name)
	}
	return strings.Join(lines, "\n")
}

func groupHosts(cfg *config.Config, group string) []string {
	if group != builtinAllGroup {
		return cfg.HostsInGroup(group)
	}
	targets := cfg.AllTargets()
	names := make([]string, 0, len(targets))
	for name, target := range targets {
		if !target.Disabled {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func hostBootstrapLine(cfg *config.Config, name string) (string, error) {
	data, err := os.ReadFile(cfg.KeyPath + ".pub")
	if err != nil {
		return "", err
	}
	pub := strings.TrimSpace(string(data))
	if pub == "" {
		return "", errors.New("empty public key")
	}
	if name == "" {
		name = "HOSTNAME"
	}
	return strings.Join([]string{
		"pub=" + shellQuote(pub) + " && \\",
		"mkdir -p ~/.ssh && \\",
		"chmod 700 ~/.ssh && \\",
		"touch ~/.ssh/authorized_keys && \\",
		"(grep -qxF \"$pub\" ~/.ssh/authorized_keys || printf '%s\\n' \"$pub\" >> ~/.ssh/authorized_keys) && \\",
		"chmod 600 ~/.ssh/authorized_keys && \\",
		"ip=$(hostname -I 2>/dev/null | awk '{print $1}') && \\",
		"([ -n \"$ip\" ] || { echo 'IPv4 address not found' >&2; exit 1; }) && \\",
		"user=$(id -un) && \\",
		"key_file=/etc/ssh/ssh_host_ed25519_key.pub && \\",
		"([ -r \"$key_file\" ] || key_file=$(ls /etc/ssh/ssh_host_*_key.pub 2>/dev/null | head -n 1)) && \\",
		"([ -r \"$key_file\" ] || { echo 'SSH host public key not found' >&2; exit 1; }) && \\",
		"key=$(awk '{print $1\" \"$2}' \"$key_file\") && \\",
		"fingerprint=$(ssh-keygen -lf \"$key_file\" | awk '{print $2}') && \\",
		"printf '# host key %s\\nhost add " + shellQuote(name) + " %s@%s:22 %s\\n' \"$fingerprint\" \"$user\" \"$ip\" \"$key\"",
	}, "\n"), nil
}

func addHostFromFields(fields []string, cfg *config.Config) (string, error) {
	if len(fields) != 4 {
		return "", errors.New("usage: host add <name> <user>@<host>:<port> <keytype> <key>")
	}
	name := fields[0]
	user, host, port, err := parseTargetAddress(fields[1])
	if err != nil {
		return "", err
	}
	publicKey := fields[2] + " " + fields[3]
	if _, err := publicKeyFingerprint(publicKey); err != nil {
		return "", err
	}
	existing, _ := cfg.Target(name)
	target := config.Target{
		User:          user,
		Host:          host,
		Port:          port,
		PublicKey:     publicKey,
		RemoteWorkDir: existing.RemoteWorkDir,
		Note:          existing.Note,
		Groups:        existing.Groups,
	}
	if err := cfg.UpsertTarget(name, target); err != nil {
		return "", err
	}
	fp, _ := publicKeyFingerprint(publicKey)
	return successText("host added\n" +
		hostNameText(name) + "\n" +
		fmt.Sprintf("%s@%s:%d\n%s",
			html.EscapeString(user),
			html.EscapeString(host),
			port,
			html.EscapeString(fp),
		)), nil
}

func setHostNote(fields []string, cfg *config.Config) (string, error) {
	if len(fields) < 1 {
		return "", errors.New("usage: host note <name> [note]")
	}
	name := fields[0]
	target, err := cfg.SetHostNote(name, strings.Join(fields[1:], " "))
	if err != nil {
		return "", err
	}
	if target.Note == "" {
		return successText("host note cleared\n" + hostNameText(name)), nil
	}
	return successText("host note\n" + hostNameText(name) + "\n" + html.EscapeString(target.Note)), nil
}

func setHostField(fields []string, cfg *config.Config) (string, error) {
	if len(fields) < 2 || len(fields) > 3 {
		return "", errors.New("usage: host set <name> remote_work_dir [path]")
	}
	name := fields[0]
	field := fields[1]
	value := ""
	if len(fields) == 3 {
		value = fields[2]
	}
	switch field {
	case "remote_work_dir":
		target, err := cfg.SetHostRemoteWorkDir(name, value)
		if err != nil {
			return "", err
		}
		if target.RemoteWorkDir == "" {
			return successText("host remote_work_dir cleared\n" + hostNameText(name)), nil
		}
		return successText("host remote_work_dir\n" + hostNameText(name) + "\n" + html.EscapeString(target.RemoteWorkDir)), nil
	default:
		return "", errors.New("usage: host set <name> remote_work_dir [path]")
	}
}

func removeHost(fields []string, cfg *config.Config) (string, error) {
	if len(fields) != 1 {
		return "", errors.New("usage: host remove <name>")
	}
	name := fields[0]
	if err := cfg.RemoveTarget(name); err != nil {
		return "", err
	}
	return successText("host removed\n" + hostNameText(name)), nil
}

func hostNameText(name string) string {
	return "<b>" + html.EscapeString(name) + "</b>"
}

func parseGroupSelector(value string) (string, error) {
	group := strings.TrimPrefix(value, "@")
	if group == value {
		return "", errors.New("group must start with @")
	}
	if !config.ValidName(group) {
		return "", fmt.Errorf("bad group name %q", group)
	}
	return group, nil
}

func parseTargetAddress(value string) (string, string, int, error) {
	at := strings.LastIndex(value, "@")
	if at <= 0 || at == len(value)-1 {
		return "", "", 0, errors.New("target must be user@host:port")
	}
	user := value[:at]
	hostPort := value[at+1:]
	host, portText, err := net.SplitHostPort(hostPort)
	if err != nil {
		host = hostPort
		portText = "22"
		if i := strings.LastIndex(hostPort, ":"); i > 0 && strings.Count(hostPort, ":") == 1 {
			host = hostPort[:i]
			portText = hostPort[i+1:]
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return "", "", 0, errors.New("bad port")
	}
	if user == "" || host == "" {
		return "", "", 0, errors.New("target must be user@host:port")
	}
	return user, host, port, nil
}

func publicKeyFingerprint(publicKey string) (string, error) {
	parts := strings.Fields(publicKey)
	if len(parts) < 2 {
		return "", errors.New("bad public key")
	}
	raw, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "SHA256:" + strings.TrimRight(base64.StdEncoding.EncodeToString(sum[:]), "="), nil
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

// handleRun returns combined remote output, command, target, and exit code.
// err is non-nil only for transport-level failures (target unknown, key
// locked, ssh couldn't run). A remote non-zero exit is not an error — it's
// just the remote command's own signal, surfaced via exitCode.
func handleRun(ctx context.Context, cfg *config.Config, st *agent.State, fields []string) (output, command, target string, exitCode int, err error) {
	if len(fields) < 3 {
		return "", "", "", 0, errors.New("usage: /run <target> <command>")
	}
	target = fields[1]
	command = strings.Join(fields[2:], " ")
	output, exitCode, err = runTargetSelector(ctx, cfg, st, target, command)
	return output, command, target, exitCode, err
}

func handleGet(ctx context.Context, cfg *config.Config, st *agent.State, fields []string) (string, error) {
	if len(fields) < 3 || len(fields) > 4 {
		return "", errors.New("usage: get <target> <remote-file> [local-file]")
	}
	localName := defaultTransferName(fields[2])
	if len(fields) == 4 {
		localName = fields[3]
	}
	if err := copyFromTargetSelector(ctx, cfg, st, fields[1], fields[2], localName); err != nil {
		return "", err
	}
	return transferText("⬅️ get", fields[1], fields[2], localName), nil
}

func handlePut(ctx context.Context, cfg *config.Config, st *agent.State, fields []string) (string, error) {
	if len(fields) < 3 || len(fields) > 4 {
		return "", errors.New("usage: put <target> <local-file> [remote-file]")
	}
	remoteName := defaultTransferName(fields[2])
	if len(fields) == 4 {
		remoteName = fields[3]
	}
	if err := copyToTargetSelector(ctx, cfg, st, fields[1], fields[2], remoteName); err != nil {
		return "", err
	}
	return transferText("➡️ put", fields[1], fields[2], remoteName), nil
}

func defaultTransferName(name string) string {
	clean := path.Clean(strings.ReplaceAll(name, "\\", "/"))
	return path.Base(clean)
}

func runTarget(ctx context.Context, cfg *config.Config, st *agent.State, name, command string) (output string, exitCode int, err error) {
	t, err := targetForSSH(cfg, st, name)
	if err != nil {
		return "", 0, err
	}
	return runSSH(ctx, cfg, st, t, command)
}

func runTargetSelector(ctx context.Context, cfg *config.Config, st *agent.State, selector, command string) (output string, exitCode int, err error) {
	return runTargetSelectorWithRunner(ctx, cfg, selector, command, st.IsUnlocked, func(ctx context.Context, host, command string) (string, int, error) {
		return runTarget(ctx, cfg, st, host, command)
	})
}

type runTargetResult struct {
	output   string
	exitCode int
	err      error
}

func runTargetSelectorWithRunner(ctx context.Context, cfg *config.Config, selector, command string, isUnlocked func() bool, runner func(context.Context, string, string) (string, int, error)) (output string, exitCode int, err error) {
	if !isTargetExpr(selector) {
		return runner(ctx, selector, command)
	}
	hosts, err := hostsForTargetExpr(cfg, selector)
	if err != nil {
		return "", 0, err
	}
	if len(hosts) == 0 {
		return "", 0, fmt.Errorf("target expression %q is empty", selector)
	}
	if !isUnlocked() {
		return "", 0, errors.New("key is locked")
	}
	results := make([]runTargetResult, len(hosts))
	sem := make(chan struct{}, maxParallelGroupRuns)
	var wg sync.WaitGroup
	for i, host := range hosts {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, host string) {
			defer wg.Done()
			defer func() { <-sem }()
			hostOutput, code, runErr := runner(ctx, host, command)
			results[i] = runTargetResult{output: hostOutput, exitCode: code, err: runErr}
		}(i, host)
	}
	wg.Wait()

	var b strings.Builder
	finalCode := 0
	for i, host := range hosts {
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString("== ")
		b.WriteString(host)
		b.WriteString(" ==\n")
		hostOutput, code, runErr := results[i].output, results[i].exitCode, results[i].err
		if hostOutput != "" {
			b.WriteString(hostOutput)
			b.WriteString("\n")
		}
		if runErr != nil {
			b.WriteString(runErr.Error())
			if finalCode == 0 {
				finalCode = 255
			}
			continue
		}
		if code != 0 {
			b.WriteString("exit status ")
			b.WriteString(strconv.Itoa(code))
			if finalCode == 0 {
				finalCode = code
			}
		} else if hostOutput == "" {
			b.WriteString("ok")
		}
	}
	return strings.TrimRight(b.String(), "\n"), finalCode, nil
}

func targetForSSH(cfg *config.Config, st *agent.State, name string) (config.Target, error) {
	t, ok := cfg.Target(name)
	if !ok {
		return config.Target{}, fmt.Errorf("unknown target %q", name)
	}
	if t.Disabled {
		return config.Target{}, fmt.Errorf("target %q is disabled", name)
	}
	if !st.IsUnlocked() {
		return config.Target{}, errors.New("key is locked")
	}
	return t, nil
}

func copyFromTarget(ctx context.Context, cfg *config.Config, st *agent.State, targetName, remoteName, localName string) error {
	t, err := targetForSSH(cfg, st, targetName)
	if err != nil {
		return err
	}
	localPath, err := localWorkPath(cfg, localName)
	if err != nil {
		return err
	}
	remotePath, err := remoteWorkPath(cfg, t, remoteName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(localPath), 0o700); err != nil {
		return err
	}
	return getSFTP(ctx, cfg, st, t, remotePath, localPath)
}

func copyFromTargetSelector(ctx context.Context, cfg *config.Config, st *agent.State, selector, remoteName, localName string) error {
	if !isTargetExpr(selector) {
		return copyFromTarget(ctx, cfg, st, selector, remoteName, localName)
	}
	return errors.New("get on group is not supported")
}

func copyToTarget(ctx context.Context, cfg *config.Config, st *agent.State, targetName, localName, remoteName string) error {
	t, err := targetForSSH(cfg, st, targetName)
	if err != nil {
		return err
	}
	localPath, err := localWorkPath(cfg, localName)
	if err != nil {
		return err
	}
	if info, err := os.Stat(localPath); err != nil {
		return err
	} else if info.IsDir() {
		return errors.New("local file is directory")
	}
	remotePath, err := remoteWorkPath(cfg, t, remoteName)
	if err != nil {
		return err
	}
	return putSFTP(ctx, cfg, st, t, localPath, remotePath)
}

func copyToTargetSelector(ctx context.Context, cfg *config.Config, st *agent.State, selector, localName, remoteName string) error {
	if !isTargetExpr(selector) {
		return copyToTarget(ctx, cfg, st, selector, localName, remoteName)
	}
	hosts, err := hostsForTargetExpr(cfg, selector)
	if err != nil {
		return err
	}
	if len(hosts) == 0 {
		return fmt.Errorf("target expression %q is empty", selector)
	}
	if !st.IsUnlocked() {
		return errors.New("key is locked")
	}
	localPath, err := localWorkPath(cfg, localName)
	if err != nil {
		return err
	}
	if info, err := os.Stat(localPath); err != nil {
		return err
	} else if info.IsDir() {
		return errors.New("local file is directory")
	}
	var errs []string
	for _, host := range hosts {
		if err := copyToTarget(ctx, cfg, st, host, localName, remoteName); err != nil {
			errs = append(errs, host+": "+err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func hostsForGroupSelector(cfg *config.Config, selector string) ([]string, error) {
	group, err := parseGroupSelector(selector)
	if err != nil {
		return nil, err
	}
	hosts := groupHosts(cfg, group)
	if len(hosts) == 0 {
		return nil, fmt.Errorf("group %q is empty", selector)
	}
	return hosts, nil
}

func isTargetExpr(selector string) bool {
	return strings.HasPrefix(selector, "@") || strings.ContainsAny(selector, ",+^")
}

func hostsForTargetExpr(cfg *config.Config, expr string) ([]string, error) {
	if !strings.ContainsAny(expr, ",+^") {
		if strings.HasPrefix(expr, "@") {
			return hostsForGroupSelector(cfg, expr)
		}
		if _, ok := cfg.Target(expr); !ok {
			return nil, fmt.Errorf("unknown target %q", expr)
		}
		return []string{expr}, nil
	}

	union := map[string]bool{}
	for _, clause := range strings.Split(expr, ",") {
		hosts, err := evalTargetClause(cfg, clause)
		if err != nil {
			return nil, err
		}
		for host := range hosts {
			union[host] = true
		}
	}
	return orderHosts(cfg, union), nil
}

func evalTargetClause(cfg *config.Config, clause string) (map[string]bool, error) {
	if clause == "" {
		return nil, fmt.Errorf("bad target expression")
	}
	parts, ops, err := splitTargetClause(clause)
	if err != nil {
		return nil, err
	}
	current, err := targetSet(cfg, parts[0])
	if err != nil {
		return nil, err
	}
	for i, op := range ops {
		next, err := targetSet(cfg, parts[i+1])
		if err != nil {
			return nil, err
		}
		switch op {
		case '+':
			for host := range current {
				if !next[host] {
					delete(current, host)
				}
			}
		case '^':
			for host := range next {
				delete(current, host)
			}
		}
	}
	return current, nil
}

func splitTargetClause(clause string) ([]string, []byte, error) {
	var parts []string
	var ops []byte
	start := 0
	for i := 0; i < len(clause); i++ {
		switch clause[i] {
		case '+', '^':
			if i == start {
				return nil, nil, fmt.Errorf("bad target expression")
			}
			parts = append(parts, clause[start:i])
			ops = append(ops, clause[i])
			start = i + 1
		}
	}
	if start == len(clause) {
		return nil, nil, fmt.Errorf("bad target expression")
	}
	parts = append(parts, clause[start:])
	return parts, ops, nil
}

func targetSet(cfg *config.Config, selector string) (map[string]bool, error) {
	if selector == "" {
		return nil, fmt.Errorf("bad target expression")
	}
	var hosts []string
	if strings.HasPrefix(selector, "@") {
		resolved, err := hostsForGroupSelector(cfg, selector)
		if err != nil {
			return nil, err
		}
		hosts = resolved
	} else {
		if _, ok := cfg.Target(selector); !ok {
			return nil, fmt.Errorf("unknown target %q", selector)
		}
		hosts = []string{selector}
	}
	out := make(map[string]bool, len(hosts))
	for _, host := range hosts {
		out[host] = true
	}
	return out, nil
}

func orderHosts(cfg *config.Config, set map[string]bool) []string {
	targets := cfg.AllTargets()
	names := make([]string, 0, len(targets))
	for name := range targets {
		if set[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func localWorkPath(cfg *config.Config, name string) (string, error) {
	if badWorkName(name) || filepath.IsAbs(name) {
		return "", errors.New("bad local file")
	}
	base, err := filepath.Abs(cfg.WorkDir)
	if err != nil {
		return "", err
	}
	full := filepath.Join(base, filepath.Clean(name))
	rel, err := filepath.Rel(base, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("bad local file")
	}
	return full, nil
}

func remoteWorkPath(cfg *config.Config, target config.Target, name string) (string, error) {
	if badWorkName(name) || strings.HasPrefix(name, "/") {
		return "", errors.New("bad remote file")
	}
	base := cfg.RemoteWorkDir
	if target.RemoteWorkDir != "" {
		base = target.RemoteWorkDir
	}
	if base == "" {
		base = "~/picoman"
	}
	if strings.ContainsAny(base, "\r\n") {
		return "", errors.New("bad remote work dir")
	}
	if strings.ContainsAny(base, " \t") {
		return "", errors.New("remote work dir with spaces is unsupported")
	}
	return path.Join(base, path.Clean(name)), nil
}

func badWorkName(name string) bool {
	if strings.TrimSpace(name) == "" {
		return true
	}
	clean := path.Clean(strings.ReplaceAll(name, "\\", "/"))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return true
	}
	return strings.ContainsAny(name, " \t*?[]{}\r\n")
}

func targetPort(t config.Target) int {
	if t.Port == 0 {
		return 22
	}
	return t.Port
}

func successText(s string) string { return "✅ " + s }
func errorText(s string) string   { return "❌ " + s }
func warningText(s string) string { return "⚠️ " + s }
func infoText(s string) string    { return "ℹ️ " + s }
func unsealText() string          { return "🟡 unsealed" }

// Per-field byte budgets for HTML-escaped content. Sum stays well below
// Telegram's 4096 limit (≈ 600 + 3000 + wrapper tags + headers).
const (
	maxCommandBytes = 600
	maxOutputBytes  = 3000
	maxPathBytes    = 400
)

func runText(target, command, output string) string {
	text := actionText("▶️ run", target) +
		"\n" + htmlBlock(command, maxCommandBytes)
	return text + outputBlock(output)
}

func runStartText(target, command string) string {
	return actionStartText("▶️ run", target) +
		"\n" + htmlBlock(command, maxCommandBytes)
}

func runErrorText(target, command, output, reason string) string {
	text := actionErrorText("▶️ run", target, reason) +
		"\n" + htmlBlock(command, maxCommandBytes)
	return text + outputBlock(output)
}

func runStartAuditText(a *auditState, target, command string) string {
	if !auditFull(a) {
		return actionStartText("▶️ run", target)
	}
	return runStartText(target, command)
}

func runAuditText(a *auditState, target, command, output, reason string) string {
	if !auditFull(a) {
		return actionText("▶️ run", target)
	}
	if reason != "" {
		return runErrorText(target, command, output, reason)
	}
	return runText(target, command, output)
}

func outputBlock(output string) string {
	if output == "" {
		return "\n(no output)"
	}
	return "\n" + htmlBlock(output, maxOutputBytes)
}

func htmlBlock(s string, budget int) string {
	return "<pre>" + escapedCodeBlock(s, budget) + "</pre>"
}

func actionText(action, target string) string {
	return html.EscapeString(action) + " <b>" + html.EscapeString(target) + "</b>"
}

func actionStartText(action, target string) string {
	return "⏳ " + actionText(action, target)
}

func actionErrorText(action, target, reason string) string {
	return "❌ " + actionText(action, target) + " (" + html.EscapeString(reason) + ")"
}

func transferText(op, target, source, destination string) string {
	text := actionText(op, target) +
		"\n" + htmlBlock(source, maxPathBytes)
	if destination != source {
		text += "\n" + htmlBlock(destination, maxPathBytes)
	}
	return text
}

func transferStartText(op, target, source, destination string) string {
	text := actionStartText(op, target) +
		"\n" + htmlBlock(source, maxPathBytes)
	if destination != source {
		text += "\n" + htmlBlock(destination, maxPathBytes)
	}
	return text
}

func transferErrorText(op, target, source, destination, reason string) string {
	text := actionErrorText(op, target, reason) +
		"\n" + htmlBlock(source, maxPathBytes)
	if destination != source {
		text += "\n" + htmlBlock(destination, maxPathBytes)
	}
	return text
}

func transferStartAuditText(a *auditState, op, target, source, destination string) string {
	if !auditFull(a) {
		return actionStartText(op, target)
	}
	return transferStartText(op, target, source, destination)
}

func transferAuditText(a *auditState, op, target, source, destination, reason string) string {
	if !auditFull(a) {
		return actionText(op, target)
	}
	if reason != "" {
		return transferErrorText(op, target, source, destination, reason)
	}
	return transferText(op, target, source, destination)
}

func auditFull(a *auditState) bool {
	return a != nil && a.LogLevel() == "all"
}
