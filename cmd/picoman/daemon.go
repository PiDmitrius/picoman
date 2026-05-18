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
	if err := writeKnownHosts(cfg); err != nil {
		log.Printf("write known_hosts: %v", err)
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		criticalNotifyUsers(cfg, bot, "workdir", err)
		log.Fatalf("create work dir: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := agent.New(cfg.AgentSocket, cfg.KeyPath, config.MaxTTL(cfg))
	cleanup := st.CleanStart()
	if err := st.PrepareAskpass(); err != nil {
		log.Printf("prepare askpass: %v", err)
	}
	out, err := outbox.Open(config.DBPath(), bot)
	if err != nil {
		criticalNotifyUsers(cfg, bot, "outbox", err)
		log.Fatalf("open outbox: %v", err)
	}
	defer out.Close()

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
			handleMessage(ctx, out, cfg, st, bot, upd.Message)
		}
		if len(updates) > 0 {
			if err := out.SetTelegramOffset(offset); err != nil {
				log.Printf("persist tg offset: %v", err)
			}
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
	ticker := time.NewTicker(5 * time.Second)
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
	ctx context.Context
	cfg *config.Config
	st  *agent.State
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

var commands = map[string]cmdEntry{
	"start":  {fn: cmdHelp},
	"help":   {fn: cmdHelp},
	"status": {fn: cmdStatus},
	"hosts":  {fn: cmdHostList},
	"host":   {fn: cmdHost},
	"unseal": {fn: cmdUnseal, async: true},
	"unlock": {fn: cmdUnlock},
	"seal":   {fn: cmdSeal},
	"lock":   {fn: cmdLock},
	"update": {fn: cmdUpdate, async: true},
	"run":    {fn: cmdRun, async: true},
	"get":    {fn: cmdGet, async: true},
	"put":    {fn: cmdPut, async: true},
}

func handleMessage(ctx context.Context, out *outbox.Store, cfg *config.Config, st *agent.State, bot *tg.Client, msg tg.Message) {
	if !config.AllowedSet(cfg)[msg.From.ID] {
		log.Printf("deny user=%d username=%s text=%q", msg.From.ID, msg.From.Username, msg.Text)
		enqueueReply(out, bot, msg, cmdReply{text: errorText("denied")})
		return
	}

	fields := strings.Fields(strings.TrimSpace(msg.Text))
	if len(fields) == 0 {
		return
	}
	name := commandName(fields[0])

	if isVersionCommand(name) {
		go handleInstallVersionMessage(out, bot, msg, tagFromVersionCommand(name))
		return
	}

	entry, ok := commands[name]
	if !ok {
		enqueueReply(out, bot, msg, cmdReply{text: warningText("unknown command\n\n" + botHelpText())})
		return
	}

	c := cmdCtx{ctx: ctx, cfg: cfg, st: st}
	run := func() {
		reply, err := entry.fn(c, fields)
		logCommand(msg, err)
		if reply.text == "" && err != nil {
			reply.text = errorText(err.Error())
		}
		enqueueReply(out, bot, msg, reply)
	}
	if entry.async {
		go run()
		return
	}
	run()
}

func logCommand(msg tg.Message, err error) {
	if err != nil {
		log.Printf("command error user=%d command=%q err=%v", msg.From.ID, msg.Text, err)
		return
	}
	log.Printf("command ok user=%d command=%q", msg.From.ID, msg.Text)
}

func enqueueReply(out *outbox.Store, bot *tg.Client, msg tg.Message, r cmdReply) {
	if r.text == "" {
		return
	}
	var err error
	if r.html {
		err = out.EnqueueHTMLReply(msg.Chat.ID, msg.MessageID, r.text)
	} else {
		err = out.EnqueueReply(msg.Chat.ID, msg.MessageID, r.text)
	}
	if err != nil {
		log.Printf("enqueue reply: %v", err)
		go criticalNotifyUser(msg.Chat.ID, bot, "outbox", err)
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

func cmdHost(c cmdCtx, fields []string) (cmdReply, error) {
	if len(fields) < 2 {
		return cmdReply{}, errors.New("usage: host <name> | host list | host note <name> [note] | host add [<name> [<user>@<host>:<port> <keytype> <key>]]")
	}
	switch fields[1] {
	case "list":
		return cmdHostList(c, fields)
	case "note":
		text, err := setHostNote(fields[2:], c.cfg)
		if err != nil {
			return cmdReply{html: true}, err
		}
		return cmdReply{text: text, html: true}, nil
	case "add":
		return hostAdd(c.cfg, fields[2:])
	default:
		name := fields[1]
		target, ok := c.cfg.Target(name)
		if !ok {
			return cmdReply{text: "❌ unknown host " + hostNameText(name), html: true}, fmt.Errorf("unknown host %q", name)
		}
		return cmdReply{text: infoText(hostText(name, target)), html: true}, nil
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

func cmdUnlock(c cmdCtx, fields []string) (cmdReply, error) {
	text, err := handleUnlock(fields, c.st)
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
	stdout, stderr, command, exitCode, err := handleRun(c.ctx, c.cfg, c.st, fields)
	if command == "" {
		return cmdReply{html: true}, err
	}
	if err != nil {
		return cmdReply{
			text: runErrorText(fields[1], command, stdout, stderr, err.Error()),
			html: true,
		}, err
	}
	if exitCode != 0 {
		reason := fmt.Sprintf("exit status %d", exitCode)
		return cmdReply{
			text: runErrorText(fields[1], command, stdout, stderr, reason),
			html: true,
		}, errors.New(reason)
	}
	return cmdReply{text: runText(fields[1], command, stdout, stderr), html: true}, nil
}

func cmdGet(c cmdCtx, fields []string) (cmdReply, error) {
	text, err := handleGet(c.ctx, c.cfg, c.st, fields)
	if err == nil {
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
		text: transferErrorText("⬅️ get", fields[1], fields[2], localName, err.Error()),
		html: true,
	}, err
}

func cmdPut(c cmdCtx, fields []string) (cmdReply, error) {
	text, err := handlePut(c.ctx, c.cfg, c.st, fields)
	if err == nil {
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
		text: transferErrorText("➡️ put", fields[1], fields[2], remoteName, err.Error()),
		html: true,
	}, err
}

func commandName(s string) string {
	return strings.TrimLeft(strings.ToLower(s), "/")
}

func botHelpText() string {
	return strings.TrimSpace(`
commands:
/unseal
/unlock 5m
/unlock
/seal
/lock
/status
/update
/host list
/host <name>
/host note <name> [note]
/host add
/run <target> <command>
/get <target> <remote-file> [local-file]
/put <target> <local-file> [remote-file]
`)
}

func handleUnlock(fields []string, st *agent.State) (string, error) {
	ttl := 5 * time.Minute
	if len(fields) > 2 {
		return "", errors.New("usage: /unlock [5m]")
	}
	if len(fields) == 2 {
		var err error
		ttl, err = time.ParseDuration(fields[1])
		if err != nil {
			return "", fmt.Errorf("bad ttl: %w", err)
		}
	}

	if err := st.Unlock(ttl); err != nil {
		return "", err
	}
	return "🟡 unlocked (" + leftText(st.Until()) + ")", nil
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
		return "0m left"
	}
	minutes := int((left + time.Minute - time.Nanosecond) / time.Minute)
	return fmt.Sprintf("%dm left", minutes)
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
	return fmt.Sprintf("%s\n%s@%s:%d%s",
		hostNameText(name),
		html.EscapeString(target.User),
		html.EscapeString(target.Host),
		port,
		state+note,
	)
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
		User:      user,
		Host:      host,
		Port:      port,
		PublicKey: publicKey,
		Note:      existing.Note,
	}
	if err := cfg.UpsertTarget(name, target); err != nil {
		return "", err
	}
	if err := writeKnownHosts(cfg); err != nil {
		return "", fmt.Errorf("write known_hosts: %w", err)
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

func hostNameText(name string) string {
	return "<b>" + html.EscapeString(name) + "</b>"
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

func remoteShellQuote(s string) string {
	if s == "~" {
		return "~"
	}
	if strings.HasPrefix(s, "~/") {
		return "~/" + shellQuote(strings.TrimPrefix(s, "~/"))
	}
	return shellQuote(s)
}

// handleRun returns stdout, stderr, command, and the ssh exit code.
// err is non-nil only for transport-level failures (target unknown, key
// locked, ssh couldn't run). Remote non-zero exit shows up as exitCode != 0
// with err == nil — callers decide how to surface that.
func handleRun(ctx context.Context, cfg *config.Config, st *agent.State, fields []string) (stdout, stderr, command string, exitCode int, err error) {
	if len(fields) < 3 {
		return "", "", "", 0, errors.New("usage: /run <target> <command>")
	}
	command = strings.Join(fields[2:], " ")
	stdout, stderr, exitCode, err = runTarget(ctx, cfg, st, fields[1], command)
	return stdout, stderr, command, exitCode, err
}

func handleGet(ctx context.Context, cfg *config.Config, st *agent.State, fields []string) (string, error) {
	if len(fields) < 3 || len(fields) > 4 {
		return "", errors.New("usage: get <target> <remote-file> [local-file]")
	}
	localName := defaultTransferName(fields[2])
	if len(fields) == 4 {
		localName = fields[3]
	}
	if err := copyFromTarget(ctx, cfg, st, fields[1], fields[2], localName); err != nil {
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
	if err := copyToTarget(ctx, cfg, st, fields[1], fields[2], remoteName); err != nil {
		return "", err
	}
	return transferText("➡️ put", fields[1], fields[2], remoteName), nil
}

func defaultTransferName(name string) string {
	clean := path.Clean(strings.ReplaceAll(name, "\\", "/"))
	return path.Base(clean)
}

func runTarget(ctx context.Context, cfg *config.Config, st *agent.State, name, command string) (stdout, stderr string, exitCode int, err error) {
	t, err := targetForSSH(cfg, st, name)
	if err != nil {
		return "", "", 0, err
	}
	return runSSH(ctx, st.Socket(), t, command)
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

// writeKnownHosts rewrites the pinned-host-keys file atomically from the
// current target set. Called at startup and after host-DB mutations, not on
// every transfer. Removes the file when no targets carry a pinned key so
// sshCommonOpts cleanly falls through to accept-new.
func writeKnownHosts(cfg *config.Config) error {
	var lines []string
	for _, target := range cfg.AllTargets() {
		key := strings.TrimSpace(target.PublicKey)
		if key == "" || target.Disabled {
			continue
		}
		host := target.Host
		if target.Port != 0 && target.Port != 22 {
			host = fmt.Sprintf("[%s]:%d", target.Host, target.Port)
		}
		lines = append(lines, host+" "+key)
	}
	path := config.KnownHostsPath()
	if len(lines) == 0 {
		_ = os.Remove(path)
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".known_hosts-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
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
	return runSCP(ctx, st.Socket(), t, remoteSpec(t, remotePath), localPath)
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
	remoteDir := path.Dir(remotePath)
	_, mkStderr, code, err := runSSH(ctx, st.Socket(), t, "mkdir -p "+remoteShellQuote(remoteDir))
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("mkdir -p: exit status %d: %s", code, strings.TrimSpace(mkStderr))
	}
	return runSCP(ctx, st.Socket(), t, localPath, remoteSpec(t, remotePath))
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
	if target.WorkDir != "" {
		base = target.WorkDir
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

func remoteSpec(t config.Target, remotePath string) string {
	return t.User + "@" + t.Host + ":" + remotePath
}

func runSCP(ctx context.Context, agentSocket string, t config.Target, from string, to string) error {
	args := scpArgs(t)
	args = append(args, "--", from, to)
	cmd := exec.CommandContext(ctx, "scp", args...)
	cmd.Env = append(os.Environ(), "SSH_AUTH_SOCK="+agentSocket)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("scp: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// runSSH returns stdout, stderr, the exit code, and an error. The error is
// non-nil only when ssh itself failed to run (e.g. couldn't be started). When
// ssh ran to completion the exit code is whatever ssh reported — which per
// `man ssh` is the remote command's exit code, or 255 if ssh hit a
// network/auth/etc. problem of its own. Trailing newlines are trimmed.
func runSSH(ctx context.Context, agentSocket string, t config.Target, remoteCommand string) (stdout, stderr string, exitCode int, err error) {
	args := sshArgs(t)
	args = append(args,
		"--",
		t.User+"@"+t.Host,
		remoteCommand,
	)

	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Env = append(os.Environ(), "SSH_AUTH_SOCK="+agentSocket)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	runErr := cmd.Run()
	stdout = strings.TrimRight(outBuf.String(), "\n")
	stderr = strings.TrimRight(errBuf.String(), "\n")
	if runErr == nil {
		return stdout, stderr, 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return stdout, stderr, exitErr.ExitCode(), nil
	}
	return stdout, stderr, 0, fmt.Errorf("ssh: %w", runErr)
}

// sshCommonOpts builds the options shared by ssh and scp. The pinned-host-key
// file is maintained by writeKnownHosts at startup and on host-DB changes —
// here we just point ssh at it (when the target carries a public key).
// Port flag differs (-p for ssh, -P for scp), so callers add it.
func sshCommonOpts(t config.Target) []string {
	args := []string{
		"-F", "/dev/null",
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=no",
	}
	if strings.TrimSpace(t.PublicKey) != "" {
		args = append(args,
			"-o", "UserKnownHostsFile="+config.KnownHostsPath(),
			"-o", "StrictHostKeyChecking=yes",
		)
	} else {
		args = append(args, "-o", "StrictHostKeyChecking=accept-new")
	}
	return args
}

func sshArgs(t config.Target) []string {
	return append(sshCommonOpts(t), "-p", fmt.Sprint(targetPort(t)))
}

func scpArgs(t config.Target) []string {
	return append(sshCommonOpts(t), "-P", fmt.Sprint(targetPort(t)))
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
// Telegram's 4096 limit even when both stdout and stderr are shown alongside
// the command (max ≈ 600 + 1500 + 1500 + wrapper tags ≈ 3700).
const (
	maxCommandBytes  = 600
	maxStreamBytes   = 3000 // when only one of stdout/stderr is present
	maxSplitBytes    = 1500 // each, when both are present
	maxPathBytes     = 400
)

func runText(target, command, stdout, stderr string) string {
	text := actionText("▶️ run", target) +
		"\n<pre><code>" + escapedCodeBlock(command, maxCommandBytes) + "</code></pre>"
	return text + outputBlocks(stdout, stderr)
}

func runErrorText(target, command, stdout, stderr, reason string) string {
	text := actionErrorText("▶️ run", target, reason) +
		"\n<pre><code>" + escapedCodeBlock(command, maxCommandBytes) + "</code></pre>"
	return text + outputBlocks(stdout, stderr)
}

// outputBlocks renders stdout and stderr as separate code blocks, with a
// "stderr:" header on the stderr block. Budgets shrink when both are present.
func outputBlocks(stdout, stderr string) string {
	stdoutBudget, stderrBudget := maxStreamBytes, maxStreamBytes
	if stdout != "" && stderr != "" {
		stdoutBudget, stderrBudget = maxSplitBytes, maxSplitBytes
	}
	var text string
	if stdout != "" {
		text += "\n<pre><code>" + escapedCodeBlock(stdout, stdoutBudget) + "</code></pre>"
	}
	if stderr != "" {
		text += "\nstderr:\n<pre><code>" + escapedCodeBlock(stderr, stderrBudget) + "</code></pre>"
	}
	if stdout == "" && stderr == "" {
		text += "\n(no output)"
	}
	return text
}

func actionText(action, target string) string {
	return html.EscapeString(action) + " <b>" + html.EscapeString(target) + "</b>"
}

func actionErrorText(action, target, reason string) string {
	return "❌ " + actionText(action, target) + " (" + html.EscapeString(reason) + ")"
}

func transferText(op, target, source, destination string) string {
	text := actionText(op, target) +
		"\n<pre><code>" + escapedCodeBlock(source, maxPathBytes) + "</code></pre>"
	if destination != source {
		text += "\n<pre><code>" + escapedCodeBlock(destination, maxPathBytes) + "</code></pre>"
	}
	return text
}

func transferErrorText(op, target, source, destination, reason string) string {
	text := actionErrorText(op, target, reason) +
		"\n<pre><code>" + escapedCodeBlock(source, maxPathBytes) + "</code></pre>"
	if destination != source {
		text += "\n<pre><code>" + escapedCodeBlock(destination, maxPathBytes) + "</code></pre>"
	}
	return text
}
