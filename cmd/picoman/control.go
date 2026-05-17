package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"picoman/internal/agent"
	"picoman/internal/config"
	"picoman/internal/outbox"
	"picoman/internal/tg"
)

// Control protocol (Unix socket, line-based):
//
//	Request : VERB [<b64-arg>]*\n
//	Response: OK [<b64-data>]\n  |  ERR <reason>\n
//
// All textual arguments are base64-encoded so the parser can tokenize on
// spaces without worrying about embedded whitespace or newlines. ASKPASS is
// the single exception: it speaks the ssh-add askpass format (raw passphrase
// line or empty line) and is dispatched before the protocol parser.

type controlServer struct {
	cfg   *config.Config
	st    *agent.State
	out   *outbox.Store
	bot   *tg.Client
	audit *auditState
}

type controlHandler func(ctx context.Context, args [][]byte) ([][]byte, error)

func (s *controlServer) handlers() map[string]controlHandler {
	bind := func(fn func([][]byte) ([][]byte, error)) controlHandler {
		return func(_ context.Context, a [][]byte) ([][]byte, error) { return fn(a) }
	}
	return map[string]controlHandler{
		"UNSEAL":    bind(s.unseal),
		"SEAL":      bind(s.seal),
		"UNLOCK":    bind(s.unlock),
		"LOCK":      bind(s.lock),
		"LOGLEVEL":  bind(s.loglevel),
		"RUN":       s.run,
		"GET":       s.get,
		"PUT":       s.put,
		"HOST_ADD":  bind(s.hostAdd),
		"HOST_NOTE": bind(s.hostNote),
	}
}

func runControl(ctx context.Context, cfg *config.Config, st *agent.State, out *outbox.Store, bot *tg.Client, audit *auditState) {
	_ = os.Remove(cfg.ControlSocket)
	ln, err := net.Listen("unix", cfg.ControlSocket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "control listen: %v\n", err)
		return
	}
	defer ln.Close()
	defer os.Remove(cfg.ControlSocket)
	_ = os.Chmod(cfg.ControlSocket, 0o600)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	srv := &controlServer{cfg: cfg, st: st, out: out, bot: bot, audit: audit}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		go srv.handle(ctx, conn)
	}
}

func (s *controlServer) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return
	}
	line = strings.TrimRight(line, "\r\n")

	verb, args, parseErr := parseControlRequest(line)
	if parseErr != nil {
		writeErr(conn, parseErr)
		return
	}
	if verb == "ASKPASS" {
		handleAskpass(conn, s.st)
		return
	}
	h, ok := s.handlers()[verb]
	if !ok {
		writeErr(conn, errors.New("unknown command"))
		return
	}
	data, err := h(ctx, args)
	if err != nil {
		writeErr(conn, err)
		return
	}
	writeOK(conn, data)
}

func parseControlRequest(line string) (string, [][]byte, error) {
	if line == "" {
		return "", nil, errors.New("empty request")
	}
	parts := strings.Split(line, " ")
	verb := parts[0]
	if verb == "ASKPASS" {
		return verb, nil, nil
	}
	args := make([][]byte, 0, len(parts)-1)
	for _, p := range parts[1:] {
		data, err := base64.StdEncoding.DecodeString(p)
		if err != nil {
			return "", nil, fmt.Errorf("bad argument encoding")
		}
		args = append(args, data)
	}
	return verb, args, nil
}

func writeOK(conn net.Conn, data [][]byte) {
	if len(data) == 0 {
		_, _ = io.WriteString(conn, "OK\n")
		return
	}
	parts := make([]string, len(data))
	for i, d := range data {
		parts[i] = base64.StdEncoding.EncodeToString(d)
	}
	_, _ = io.WriteString(conn, "OK "+strings.Join(parts, " ")+"\n")
}

func writeErr(conn net.Conn, err error) {
	_, _ = io.WriteString(conn, "ERR "+err.Error()+"\n")
}

func handleAskpass(conn net.Conn, st *agent.State) {
	passphrase := st.Passphrase()
	if passphrase == "" {
		_, _ = io.WriteString(conn, "\n")
		return
	}
	_, _ = io.WriteString(conn, passphrase+"\n")
}

// --- handlers ---

func (s *controlServer) unseal(args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("usage: UNSEAL <passphrase>")
	}
	if err := s.st.Unseal(string(args[0])); err != nil {
		notify(s.out, s.cfg, s.bot, false, errorText("unseal failed: "+err.Error()))
		return nil, err
	}
	notify(s.out, s.cfg, s.bot, false, unsealText())
	return nil, nil
}

func (s *controlServer) seal(_ [][]byte) ([][]byte, error) {
	if err := s.st.Lock(); err != nil {
		notify(s.out, s.cfg, s.bot, false, errorText("seal failed: "+err.Error()))
		return nil, err
	}
	s.st.Seal()
	notify(s.out, s.cfg, s.bot, false, "⚪ sealed")
	return nil, nil
}

func (s *controlServer) unlock(args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("usage: UNLOCK <ttl>")
	}
	ttl, err := time.ParseDuration(string(args[0]))
	if err != nil {
		return nil, fmt.Errorf("bad ttl: %w", err)
	}
	if err := s.st.Unlock(ttl); err != nil {
		notify(s.out, s.cfg, s.bot, false, errorText("unlock failed: "+err.Error()))
		return nil, err
	}
	notify(s.out, s.cfg, s.bot, false, "🟡 unlocked ("+leftText(s.st.Until())+")")
	return nil, nil
}

func (s *controlServer) lock(_ [][]byte) ([][]byte, error) {
	if err := s.st.Lock(); err != nil {
		notify(s.out, s.cfg, s.bot, false, errorText("lock failed: "+err.Error()))
		return nil, err
	}
	notify(s.out, s.cfg, s.bot, false, "🔒 locked")
	return nil, nil
}

func (s *controlServer) loglevel(args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("usage: LOGLEVEL <chat|all>")
	}
	level := string(args[0])
	if !s.audit.SetLogLevel(level) {
		return nil, errors.New("bad loglevel")
	}
	notify(s.out, s.cfg, s.bot, false, "⚙️ loglevel "+level)
	return nil, nil
}

func (s *controlServer) run(ctx context.Context, args [][]byte) ([][]byte, error) {
	if len(args) != 2 {
		return nil, errors.New("usage: RUN <host> <command>")
	}
	host, command := string(args[0]), string(args[1])
	stdout, stderr, err := runTarget(ctx, s.cfg, s.st, host, command)
	if err != nil {
		notify(s.out, s.cfg, s.bot, true, runErrorText(host, command, stdout, stderr, err.Error()))
		// Surface partial stdout/stderr to the CLI by embedding them in the
		// error reason — protocol stays simple ERR-string while CLI users
		// see more than just "ssh: exit status N".
		reason := err.Error()
		if stdout != "" {
			reason += "\nstdout:\n" + stdout
		}
		if stderr != "" {
			reason += "\nstderr:\n" + stderr
		}
		return nil, errors.New(reason)
	}
	if s.audit.LogLevel() == "all" {
		notify(s.out, s.cfg, s.bot, true, runText(host, command, stdout, stderr))
	} else {
		notify(s.out, s.cfg, s.bot, true, actionText("▶️ run", host))
	}
	return [][]byte{[]byte(stdout), []byte(stderr)}, nil
}

func (s *controlServer) get(ctx context.Context, args [][]byte) ([][]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("usage: GET <host> <remote> <local>")
	}
	host, remote, local := string(args[0]), string(args[1]), string(args[2])
	if err := copyFromTarget(ctx, s.cfg, s.st, host, remote, local); err != nil {
		notify(s.out, s.cfg, s.bot, true, transferErrorText("⬅️ get", host, remote, local, err.Error()))
		return nil, err
	}
	notify(s.out, s.cfg, s.bot, true, transferText("⬅️ get", host, remote, local))
	return nil, nil
}

func (s *controlServer) hostAdd(args [][]byte) ([][]byte, error) {
	if len(args) != 4 {
		return nil, errors.New("usage: HOST_ADD <name> <user>@<host>:<port> <keytype> <key>")
	}
	fields := []string{string(args[0]), string(args[1]), string(args[2]), string(args[3])}
	if _, err := addHostFromFields(fields, s.cfg); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *controlServer) hostNote(args [][]byte) ([][]byte, error) {
	if len(args) < 1 {
		return nil, errors.New("usage: HOST_NOTE <name> [note]")
	}
	name := string(args[0])
	note := ""
	if len(args) >= 2 {
		note = string(args[1])
	}
	if _, err := s.cfg.SetHostNote(name, note); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *controlServer) put(ctx context.Context, args [][]byte) ([][]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("usage: PUT <host> <local> <remote>")
	}
	host, local, remote := string(args[0]), string(args[1]), string(args[2])
	if err := copyToTarget(ctx, s.cfg, s.st, host, local, remote); err != nil {
		notify(s.out, s.cfg, s.bot, true, transferErrorText("➡️ put", host, local, remote, err.Error()))
		return nil, err
	}
	notify(s.out, s.cfg, s.bot, true, transferText("➡️ put", host, local, remote))
	return nil, nil
}

// --- CLI ---

func runUnseal(args []string) {
	passphrase, err := readPassphrase(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if passphrase == "" {
		fmt.Fprintln(os.Stderr, "empty passphrase")
		os.Exit(1)
	}
	simpleControl("UNSEAL", passphrase)
}

func runSeal() { simpleControl("SEAL") }
func runLock() { simpleControl("LOCK") }

func runUnlock(args []string) {
	if len(args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman unlock [5m]")
		os.Exit(1)
	}
	ttl := "5m"
	if len(args) == 1 {
		ttl = args[0]
	}
	simpleControl("UNLOCK", ttl)
}

func runLocalRun(args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: picoman run <target> <command>")
		os.Exit(1)
	}
	parts, err := requestControl("RUN", args[0], strings.Join(args[1:], " "))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(parts) > 0 && len(parts[0]) > 0 {
		os.Stdout.Write(parts[0])
		if !strings.HasSuffix(string(parts[0]), "\n") {
			fmt.Println()
		}
	}
	if len(parts) > 1 && len(parts[1]) > 0 {
		os.Stderr.Write(parts[1])
		if !strings.HasSuffix(string(parts[1]), "\n") {
			fmt.Fprintln(os.Stderr)
		}
	}
}

func runLocalGet(args []string) {
	if len(args) < 2 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: picoman get <target> <remote-file> [local-file]")
		os.Exit(1)
	}
	localName := defaultTransferName(args[1])
	if len(args) == 3 {
		localName = args[2]
	}
	simpleControl("GET", args[0], args[1], localName)
}

func runLocalPut(args []string) {
	if len(args) < 2 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: picoman put <target> <local-file> [remote-file]")
		os.Exit(1)
	}
	remoteName := defaultTransferName(args[1])
	if len(args) == 3 {
		remoteName = args[2]
	}
	simpleControl("PUT", args[0], args[1], remoteName)
}

func runLogLevel(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman loglevel <chat|all>")
		os.Exit(1)
	}
	simpleControl("LOGLEVEL", args[0])
}

func runHost(args []string) {
	if len(args) == 0 {
		runHostList()
		return
	}
	switch args[0] {
	case "list":
		runHostList()
	case "add":
		runHostAdd(args[1:])
	case "note":
		runHostNote(args[1:])
	default:
		runHostShow(args[0])
	}
}

// loadHosts returns config with hosts.json loaded. Reads disk directly — the
// daemon writes via atomic rename, so readers always see a consistent snapshot.
func loadHosts() *config.Config {
	cfg := loadConfigOrExit()
	if err := config.LoadHostDB(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "load host db: %v\n", err)
		os.Exit(1)
	}
	return cfg
}

func runHostList() {
	cfg := loadHosts()
	names := cfg.HostNames()
	if len(names) == 0 {
		fmt.Println("host list empty")
		return
	}
	fmt.Println("host list")
	for _, n := range names {
		t, _ := cfg.Target(n)
		line := "- " + n
		if t.Note != "" {
			line += " (" + t.Note + ")"
		}
		if t.Disabled {
			line += " (disabled)"
		}
		fmt.Println(line)
	}
}

func runHostShow(name string) {
	cfg := loadHosts()
	t, ok := cfg.Target(name)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown host %q\n", name)
		os.Exit(1)
	}
	port := t.Port
	if port == 0 {
		port = 22
	}
	fmt.Println(name)
	fmt.Printf("%s@%s:%d\n", t.User, t.Host, port)
	if t.Disabled {
		fmt.Println("disabled")
	}
	if t.Note != "" {
		fmt.Println(t.Note)
	}
}

func runHostAdd(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: picoman host add <name> <user>@<host>:<port> <keytype> <key>")
		os.Exit(1)
	}
	simpleControl("HOST_ADD", args[0], args[1], args[2], args[3])
}

func runHostNote(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman host note <name> [note]")
		os.Exit(1)
	}
	simpleControl("HOST_NOTE", args[0], strings.Join(args[1:], " "))
}

func runAskpass() {
	// ASKPASS is special: it speaks raw ssh-askpass format, not OK/ERR.
	cfg := loadConfigOrExit()
	resp, err := rawControl(cfg.ControlSocket, "ASKPASS")
	if err != nil {
		os.Exit(1)
	}
	fmt.Print(resp)
}

// simpleControl runs a verb, prints the first response payload (if any) to
// stdout, exits non-zero on ERR. Multi-payload responses (RUN's stdout/stderr)
// should be consumed via requestControl directly.
func simpleControl(verb string, args ...string) {
	parts, err := requestControl(verb, args...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(parts) > 0 && len(parts[0]) > 0 {
		os.Stdout.Write(parts[0])
		if !strings.HasSuffix(string(parts[0]), "\n") {
			fmt.Println()
		}
	}
}

// requestControl encodes args, sends the request, and decodes the OK payload
// into separate parts (one per b64 token after "OK"). Returns nil parts when
// the response is bare "OK". On ERR or transport failure returns the reason.
func requestControl(verb string, args ...string) ([][]byte, error) {
	cfg := loadConfigOrExit()
	line := verb
	for _, a := range args {
		line += " " + base64.StdEncoding.EncodeToString([]byte(a))
	}
	resp, err := rawControl(cfg.ControlSocket, line)
	if err != nil {
		return nil, err
	}
	resp = strings.TrimRight(resp, "\r\n")
	switch {
	case resp == "OK":
		return nil, nil
	case strings.HasPrefix(resp, "OK "):
		tokens := strings.Split(strings.TrimPrefix(resp, "OK "), " ")
		parts := make([][]byte, 0, len(tokens))
		for _, t := range tokens {
			b, decErr := base64.StdEncoding.DecodeString(t)
			if decErr != nil {
				return nil, decErr
			}
			parts = append(parts, b)
		}
		return parts, nil
	case strings.HasPrefix(resp, "ERR "):
		return nil, errors.New(strings.TrimPrefix(resp, "ERR "))
	default:
		return nil, fmt.Errorf("unexpected response: %q", resp)
	}
}

func rawControl(socket, line string) (string, error) {
	conn, err := net.DialTimeout("unix", socket, 3*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, line+"\n"); err != nil {
		return "", err
	}
	resp, err := io.ReadAll(conn)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func readPassphrase(args []string) (string, error) {
	if len(args) > 1 {
		return "", fmt.Errorf("usage: picoman unseal [passphrase]")
	}
	if len(args) == 1 {
		return args[0], nil
	}

	info, err := os.Stdin.Stat()
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		data, err := io.ReadAll(os.Stdin)
		return strings.TrimRight(string(data), "\r\n"), err
	}

	fmt.Fprint(os.Stderr, "Passphrase: ")
	_ = exec.Command("stty", "-echo").Run()
	defer exec.Command("stty", "echo").Run()
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Fprintln(os.Stderr)
	return strings.TrimRight(line, "\r\n"), err
}

func loadConfigOrExit() *config.Config {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot load config: %v\n", err)
		os.Exit(1)
	}
	return cfg
}
