package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"log"
	"os"
	"os/exec"
	"os/signal"
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

	writePID()
	defer removePID()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := agent.New(cfg.AgentSocket, cfg.KeyPath, config.MaxTTL(cfg))
	cleanup := st.CleanStart()
	if cfg.KeyPassphrase != "" {
		st.Unseal(cfg.KeyPassphrase)
	}
	bot := tg.New(cfg.TelegramToken)
	out, err := outbox.Open(config.DBPath(), bot)
	if err != nil {
		criticalNotifyUsers(cfg, bot, "outbox", err)
		log.Fatalf("open outbox: %v", err)
	}
	defer out.Close()

	outboxCtx, stopOutbox := context.WithCancel(context.Background())
	defer stopOutbox()
	go out.Run(outboxCtx)
	audit := newAuditState()
	go runControl(ctx, cfg, st, out, bot, audit)

	notifyUsers(out, cfg, bot, infoText(lifecycleText("started", cleanup)))

	offset := int64(0)
	for {
		updates, err := bot.GetUpdates(ctx, offset)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("getUpdates: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		for _, upd := range updates {
			offset = upd.UpdateID + 1
			if upd.Message.Text == "" {
				continue
			}
			handleMessage(ctx, out, cfg, st, bot, upd.Message)
		}
	}

	cleanup = st.CleanStart()
	notifyUsers(out, cfg, bot, infoText(lifecycleText("stopped", cleanup)))
	flushOutbox(out)
	stopOutbox()
}

func lifecycleText(event string, cleanup agent.CleanResult) string {
	text := "picoman " + version + " " + event
	if !cleanup.OK() {
		text += "\n\n" + cleanup.String()
	}
	return text
}

func notifyUsers(out *outbox.Store, cfg *config.Config, bot *tg.Client, text string) {
	for _, userID := range cfg.AllowedUsers {
		if err := out.Enqueue(userID, text); err != nil {
			log.Printf("enqueue notify user=%d: %v", userID, err)
			go criticalNotifyUser(userID, bot, "outbox", err)
		}
	}
}

func notifyUsersHTML(out *outbox.Store, cfg *config.Config, bot *tg.Client, text string) {
	for _, userID := range cfg.AllowedUsers {
		if err := out.EnqueueHTML(userID, text); err != nil {
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

func handleMessage(ctx context.Context, out *outbox.Store, cfg *config.Config, st *agent.State, bot *tg.Client, msg tg.Message) {
	if !config.AllowedSet(cfg)[msg.From.ID] {
		log.Printf("deny user=%d username=%s text=%q", msg.From.ID, msg.From.Username, msg.Text)
		if err := out.EnqueueReply(msg.Chat.ID, msg.MessageID, errorText("denied")); err != nil {
			go criticalNotifyUser(msg.Chat.ID, bot, "outbox", err)
		}
		return
	}

	text := strings.TrimSpace(msg.Text)
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return
	}

	var reply string
	var err error

	switch commandName(fields[0]) {
	case "start", "help":
		reply = infoText(botHelpText())
	case "status":
		reply = infoText(statusText(st))
	case "unlock":
		reply, err = handleUnlock(fields, st)
	case "lock":
		err = st.Lock()
		if err == nil {
			reply = "🔒 locked"
		}
	case "run":
		reply, err = handleRun(ctx, cfg, st, fields)
		if err == nil {
			if enqueueErr := out.EnqueueHTMLReply(msg.Chat.ID, msg.MessageID, reply); enqueueErr != nil {
				log.Printf("enqueue reply: %v", enqueueErr)
				go criticalNotifyUser(msg.Chat.ID, bot, "outbox", enqueueErr)
			}
			return
		}
	default:
		reply = warningText("unknown command\n\n" + botHelpText())
	}

	if err != nil {
		reply = errorText(err.Error())
		log.Printf("command error user=%d command=%q err=%v", msg.From.ID, text, err)
	} else {
		log.Printf("command ok user=%d command=%q", msg.From.ID, text)
	}

	if err := out.EnqueueReply(msg.Chat.ID, msg.MessageID, reply); err != nil {
		log.Printf("enqueue reply: %v", err)
		go criticalNotifyUser(msg.Chat.ID, bot, "outbox", err)
	}
}

func commandName(s string) string {
	return strings.TrimPrefix(strings.ToLower(s), "/")
}

func botHelpText() string {
	return strings.TrimSpace(`
commands:
/unlock 5m
/unlock
/lock
/status
/run <target> <command>
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
		lines = append(lines, "❌ sealed")
	} else {
		lines = append(lines, "✅ unsealed")
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

func handleRun(ctx context.Context, cfg *config.Config, st *agent.State, fields []string) (string, error) {
	if len(fields) < 3 {
		return "", errors.New("usage: /run <target> <command>")
	}

	remoteCommand := strings.Join(fields[2:], " ")
	out, err := runTarget(ctx, cfg, st, fields[1], remoteCommand)
	if err != nil {
		return "", err
	}
	return runText(fields[1], remoteCommand, out), nil
}

func runTarget(ctx context.Context, cfg *config.Config, st *agent.State, name, command string) (string, error) {
	t, ok := cfg.Targets[name]
	if !ok {
		return "", fmt.Errorf("unknown target %q", name)
	}
	if !st.IsUnlocked() {
		return "", errors.New("key is locked")
	}
	return runSSH(ctx, st.Socket(), t, command)
}

func runSSH(ctx context.Context, agentSocket string, t config.Target, remoteCommand string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	args := []string{
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=no",
		"-o", "StrictHostKeyChecking=accept-new",
		t.User + "@" + t.Host,
		remoteCommand,
	}

	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Env = append(os.Environ(), "SSH_AUTH_SOCK="+agentSocket)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	out := strings.TrimSpace(stdout.String())
	errOut := strings.TrimSpace(stderr.String())

	if err != nil {
		if errOut != "" {
			return out, fmt.Errorf("ssh: %w: %s", err, errOut)
		}
		return out, fmt.Errorf("ssh: %w", err)
	}
	if out == "" && errOut != "" {
		return errOut, nil
	}
	if errOut != "" {
		return out + "\n\nstderr:\n" + errOut, nil
	}
	if out == "" {
		return "ok", nil
	}
	return out, nil
}

func successText(s string) string { return "✅ " + s }
func errorText(s string) string   { return "❌ " + s }
func warningText(s string) string { return "⚠️ " + s }
func infoText(s string) string    { return "ℹ️ " + s }

func runText(target, command, output string) string {
	return "✅ " + html.EscapeString(target) +
		"\n<pre><code>" + html.EscapeString(command) + "</code></pre>" +
		"\n<pre><code>" + html.EscapeString(output) + "</code></pre>"
}
