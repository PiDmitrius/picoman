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
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		criticalNotifyUsers(cfg, bot, "workdir", err)
		log.Fatalf("create work dir: %v", err)
	}

	writePID()
	defer removePID()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := agent.New(cfg.AgentSocket, cfg.KeyPath, config.MaxTTL(cfg))
	cleanup := st.CleanStart()
	autoUnsealed := false
	if cfg.KeyPassphrase != "" {
		st.Unseal(cfg.KeyPassphrase)
		autoUnsealed = true
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

	notifyUsers(out, cfg, bot, infoText(lifecycleText("started", cleanup)))
	if autoUnsealed {
		notifyUsers(out, cfg, bot, unsealText())
	}

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
			notifyUsers(out, cfg, bot, errorText("lock failed: "+err.Error()))
		} else {
			notifyUsers(out, cfg, bot, "🔒 locked")
		}
		activeUntil = time.Time{}
	}
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
	replyHTML := false

	switch commandName(fields[0]) {
	case "start", "help":
		reply = infoText(botHelpText())
	case "status":
		reply = infoText(statusText(st))
	case "hosts":
		reply = infoText(hostsText(cfg))
		replyHTML = true
	case "host":
		reply, err = handleHost(fields, cfg)
		if err == nil {
			replyHTML = true
		} else if len(fields) == 2 && fields[1] != "add" {
			reply = "❌ unknown host " + hostNameText(fields[1])
			replyHTML = true
		}
	case "unlock":
		reply, err = handleUnlock(fields, st)
	case "lock":
		err = st.Lock()
		if err == nil {
			reply = "🔒 locked"
		}
	case "run":
		go handleAsyncAction(ctx, out, cfg, st, bot, msg, fields)
		return
	case "get":
		go handleAsyncAction(ctx, out, cfg, st, bot, msg, fields)
		return
	case "put":
		go handleAsyncAction(ctx, out, cfg, st, bot, msg, fields)
		return
	default:
		reply = warningText("unknown command\n\n" + botHelpText())
	}

	if err != nil {
		if reply == "" {
			reply = errorText(err.Error())
		}
		log.Printf("command error user=%d command=%q err=%v", msg.From.ID, text, err)
	} else {
		log.Printf("command ok user=%d command=%q", msg.From.ID, text)
	}

	var enqueueErr error
	if replyHTML {
		enqueueErr = out.EnqueueHTMLReply(msg.Chat.ID, msg.MessageID, reply)
	} else {
		enqueueErr = out.EnqueueReply(msg.Chat.ID, msg.MessageID, reply)
	}
	if enqueueErr != nil {
		log.Printf("enqueue reply: %v", enqueueErr)
		go criticalNotifyUser(msg.Chat.ID, bot, "outbox", enqueueErr)
	}
}

func handleAsyncAction(ctx context.Context, out *outbox.Store, cfg *config.Config, st *agent.State, bot *tg.Client, msg tg.Message, fields []string) {
	reply, err := handleAsyncActionReply(ctx, cfg, st, fields)
	if err != nil {
		log.Printf("command error user=%d command=%q err=%v", msg.From.ID, msg.Text, err)
	} else {
		log.Printf("command ok user=%d command=%q", msg.From.ID, msg.Text)
	}
	if enqueueErr := out.EnqueueHTMLReply(msg.Chat.ID, msg.MessageID, reply); enqueueErr != nil {
		log.Printf("enqueue reply: %v", enqueueErr)
		go criticalNotifyUser(msg.Chat.ID, bot, "outbox", enqueueErr)
	}
}

func handleAsyncActionReply(ctx context.Context, cfg *config.Config, st *agent.State, fields []string) (string, error) {
	switch commandName(fields[0]) {
	case "run":
		reply, err := handleRun(ctx, cfg, st, fields)
		if err == nil || len(fields) < 3 {
			return replyOrError(reply, err)
		}
		return runErrorText(fields[1], strings.Join(fields[2:], " "), err.Error()), err
	case "get":
		reply, err := handleGet(ctx, cfg, st, fields)
		if err == nil || len(fields) < 3 {
			return replyOrError(reply, err)
		}
		localName := defaultTransferName(fields[2])
		if len(fields) >= 4 {
			localName = fields[3]
		}
		return transferErrorText("⬅️ get", fields[1], fields[2], localName, err.Error()), err
	case "put":
		reply, err := handlePut(ctx, cfg, st, fields)
		if err == nil || len(fields) < 3 {
			return replyOrError(reply, err)
		}
		remoteName := defaultTransferName(fields[2])
		if len(fields) >= 4 {
			remoteName = fields[3]
		}
		return transferErrorText("➡️ put", fields[1], fields[2], remoteName, err.Error()), err
	}
	return errorText("unknown command"), errors.New("unknown command")
}

func replyOrError(reply string, err error) (string, error) {
	if err != nil {
		return errorText(err.Error()), err
	}
	return reply, nil
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
/hosts
/host <name>
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
	if len(cfg.Targets) == 0 {
		return "hosts empty"
	}
	names := make([]string, 0, len(cfg.Targets))
	for name := range cfg.Targets {
		names = append(names, hostNameText(name))
	}
	sortStrings(names)
	return "hosts\n" + strings.Join(names, "\n")
}

func handleHost(fields []string, cfg *config.Config) (string, error) {
	if len(fields) == 2 && fields[1] == "add" {
		line, err := hostBootstrapLine(cfg, "")
		if err != nil {
			return "", err
		}
		return "<pre><code>" + html.EscapeString(line) + "</code></pre>", nil
	}
	if len(fields) == 2 {
		target, ok := cfg.Targets[fields[1]]
		if !ok {
			return "", fmt.Errorf("unknown host %q", fields[1])
		}
		return infoText(hostText(fields[1], target)), nil
	}
	if len(fields) >= 3 && fields[1] == "add" {
		if len(fields) == 3 {
			if !validTargetName(fields[2]) {
				return "", fmt.Errorf("bad host name %q", fields[2])
			}
			line, err := hostBootstrapLine(cfg, fields[2])
			if err != nil {
				return "", err
			}
			return "<pre><code>" + html.EscapeString(line) + "</code></pre>", nil
		}
		return addHostFromFields(fields[2:], cfg)
	}
	return "", errors.New("usage: host <name> | host add | host add <name> <user>@<host>:<port> <keytype> <key>")
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
	return fmt.Sprintf("%s\n%s@%s:%d%s",
		hostNameText(name),
		html.EscapeString(target.User),
		html.EscapeString(target.Host),
		port,
		state,
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
	if !validTargetName(name) {
		return "", fmt.Errorf("bad host name %q", name)
	}
	user, host, port, err := parseTargetAddress(fields[1])
	if err != nil {
		return "", err
	}
	publicKey := fields[2] + " " + fields[3]
	if _, err := publicKeyFingerprint(publicKey); err != nil {
		return "", err
	}
	if cfg.Targets == nil {
		cfg.Targets = map[string]config.Target{}
	}
	cfg.Targets[name] = config.Target{
		User:      user,
		Host:      host,
		Port:      port,
		PublicKey: publicKey,
	}
	if err := config.SaveHostDB(cfg); err != nil {
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

func validTargetName(name string) bool {
	if name == "" || len(name) > 32 {
		return false
	}
	for i, r := range name {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			continue
		}
		if i > 0 && (r == '_' || r == '-') {
			continue
		}
		return false
	}
	return true
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

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
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

func runTarget(ctx context.Context, cfg *config.Config, st *agent.State, name, command string) (string, error) {
	t, knownHosts, err := targetForSSH(cfg, st, name)
	if err != nil {
		return "", err
	}
	return runSSH(ctx, st.Socket(), t, command, knownHosts)
}

func targetForSSH(cfg *config.Config, st *agent.State, name string) (config.Target, string, error) {
	t, ok := cfg.Targets[name]
	if !ok {
		return config.Target{}, "", fmt.Errorf("unknown target %q", name)
	}
	if t.Disabled {
		return config.Target{}, "", fmt.Errorf("target %q is disabled", name)
	}
	if !st.IsUnlocked() {
		return config.Target{}, "", errors.New("key is locked")
	}
	knownHosts, err := writeKnownHosts(cfg)
	if err != nil {
		return config.Target{}, "", err
	}
	return t, knownHosts, nil
}

func writeKnownHosts(cfg *config.Config) (string, error) {
	var lines []string
	for _, target := range cfg.Targets {
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
	if len(lines) == 0 {
		return "", nil
	}
	if err := os.MkdirAll(config.DataDir(), 0o700); err != nil {
		return "", err
	}
	path := config.KnownHostsPath()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func copyFromTarget(ctx context.Context, cfg *config.Config, st *agent.State, targetName, remoteName, localName string) error {
	t, knownHosts, err := targetForSSH(cfg, st, targetName)
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
	return runSCP(ctx, st.Socket(), t, knownHosts, remoteSpec(t, remotePath), localPath)
}

func copyToTarget(ctx context.Context, cfg *config.Config, st *agent.State, targetName, localName, remoteName string) error {
	t, knownHosts, err := targetForSSH(cfg, st, targetName)
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
	if _, err := runSSH(ctx, st.Socket(), t, "mkdir -p "+remoteShellQuote(remoteDir), knownHosts); err != nil {
		return err
	}
	return runSCP(ctx, st.Socket(), t, knownHosts, localPath, remoteSpec(t, remotePath))
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

func runSCP(ctx context.Context, agentSocket string, t config.Target, knownHosts string, from string, to string) error {
	args := scpArgs(t, knownHosts)
	args = append(args, from, to)
	cmd := exec.CommandContext(ctx, "scp", args...)
	cmd.Env = append(os.Environ(), "SSH_AUTH_SOCK="+agentSocket)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("scp: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func runSSH(ctx context.Context, agentSocket string, t config.Target, remoteCommand string, knownHosts string) (string, error) {
	args := sshArgs(t, knownHosts)
	args = append(args,
		t.User+"@"+t.Host,
		remoteCommand,
	)

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

func sshArgs(t config.Target, knownHosts string) []string {
	args := []string{
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=no",
		"-p", fmt.Sprint(targetPort(t)),
	}
	if knownHosts != "" && strings.TrimSpace(t.PublicKey) != "" {
		args = append(args,
			"-o", "UserKnownHostsFile="+knownHosts,
			"-o", "StrictHostKeyChecking=yes",
		)
	} else {
		args = append(args, "-o", "StrictHostKeyChecking=accept-new")
	}
	return args
}

func scpArgs(t config.Target, knownHosts string) []string {
	args := []string{
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=no",
		"-P", fmt.Sprint(targetPort(t)),
	}
	if knownHosts != "" && strings.TrimSpace(t.PublicKey) != "" {
		args = append(args,
			"-o", "UserKnownHostsFile="+knownHosts,
			"-o", "StrictHostKeyChecking=yes",
		)
	} else {
		args = append(args, "-o", "StrictHostKeyChecking=accept-new")
	}
	return args
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

func runText(target, command, output string) string {
	return actionText("▶️ run", target) +
		"\n<pre><code>" + html.EscapeString(command) + "</code></pre>" +
		"\n<pre><code>" + html.EscapeString(output) + "</code></pre>"
}

func runErrorText(target, command, reason string) string {
	return actionErrorText("▶️ run", target, reason) +
		"\n<pre><code>" + html.EscapeString(command) + "</code></pre>"
}

func actionText(action, target string) string {
	return html.EscapeString(action) + " <b>" + html.EscapeString(target) + "</b>"
}

func actionErrorText(action, target, reason string) string {
	return "❌ " + actionText(action, target) + " (" + html.EscapeString(reason) + ")"
}

func transferText(op, target, source, destination string) string {
	text := actionText(op, target) +
		"\n<pre><code>" + html.EscapeString(source) + "</code></pre>"
	if destination != source {
		text += "\n<pre><code>" + html.EscapeString(destination) + "</code></pre>"
	}
	return text
}

func transferErrorText(op, target, source, destination, reason string) string {
	text := actionErrorText(op, target, reason) +
		"\n<pre><code>" + html.EscapeString(source) + "</code></pre>"
	if destination != source {
		text += "\n<pre><code>" + html.EscapeString(destination) + "</code></pre>"
	}
	return text
}
