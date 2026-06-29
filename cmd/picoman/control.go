package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
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
		"LOCK":      bind(s.lock),
		"LOGLEVEL":  bind(s.loglevel),
		"RUN":       s.run,
		"GET":       s.get,
		"PUT":       s.put,
		"HOST_ADD":  bind(s.hostAdd),
		"HOST_NOTE": bind(s.hostNote),
		"HOST_SET":  bind(s.hostSet),
		"HOST_RM":   bind(s.hostRemove),
		"GROUP_ADD": bind(s.groupAdd),
		"GROUP_RM":  bind(s.groupRemove),
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
		s.handleAskpass(conn)
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

// handleAskpass speaks the raw ssh-askpass format. To avoid handing the
// passphrase to arbitrary same-user processes, only descendants of the
// running daemon get a real answer; everyone else sees the sealed-style
// empty line. Legit caller chain is: askpass-symlink → ssh-add → daemon.
func (s *controlServer) handleAskpass(conn net.Conn) {
	if !s.askpassCallerTrusted(conn) {
		_, _ = io.WriteString(conn, "\n")
		return
	}
	passphrase := s.st.Passphrase()
	if passphrase == "" {
		_, _ = io.WriteString(conn, "\n")
		return
	}
	_, _ = io.WriteString(conn, passphrase+"\n")
}

func (s *controlServer) askpassCallerTrusted(conn net.Conn) bool {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return false
	}
	peer, err := peerPID(uc)
	if err != nil {
		log.Printf("askpass peer pid: %v", err)
		return false
	}
	daemon := os.Getpid()
	if peer == daemon {
		return false // we never call ourselves
	}
	if isDescendantOf(peer, daemon) {
		return true
	}
	cmd := procCommand(peer)
	log.Printf("askpass refused: peer pid=%d cmd=%q is not a descendant of daemon pid=%d", peer, cmd, daemon)
	notify(s.out, s.cfg, s.bot, false,
		fmt.Sprintf("❌ askpass refused\npid=%d cmd=%q", peer, cmd))
	return false
}

// procCommand returns /proc/$pid/comm, trimmed. Best-effort; "" on failure.
func procCommand(pid int) string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func peerPID(uc *net.UnixConn) (int, error) {
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, err
	}
	var ucred *syscall.Ucred
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		ucred, sockErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if sockErr != nil {
		return 0, sockErr
	}
	return int(ucred.Pid), nil
}

// isDescendantOf walks /proc/$pid/status's PPid: chain. Returns true if
// ancestor appears anywhere above pid before hitting pid 1 / 0.
func isDescendantOf(pid, ancestor int) bool {
	for hops := 0; hops < 64; hops++ {
		ppid, err := readPPID(pid)
		if err != nil || ppid <= 1 {
			return false
		}
		if ppid == ancestor {
			return true
		}
		pid = ppid
	}
	return false
}

func readPPID(pid int) (int, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "PPid:") {
			continue
		}
		return strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "PPid:")))
	}
	return 0, errors.New("PPid line missing")
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
	if err := setLogLevel(s.cfg, s.audit, level); err != nil {
		return nil, err
	}
	notify(s.out, s.cfg, s.bot, false, "⚙️ loglevel "+level)
	return nil, nil
}

func (s *controlServer) run(ctx context.Context, args [][]byte) ([][]byte, error) {
	if len(args) != 2 {
		return nil, errors.New("usage: RUN <host> <command>")
	}
	host, command := string(args[0]), string(args[1])
	startedCh := s.startAction(runStartAuditText(s.audit, host, command))
	output, exitCode, err := runTargetSelector(ctx, s.cfg, s.st, host, command)
	log.Printf("control run host=%s exit=%d err=%v", host, exitCode, err)
	if err != nil {
		s.finishAction(startedCh, true, runAuditText(s.audit, host, command, output, err.Error()))
		return nil, err
	}
	switch {
	case exitCode == 255:
		// ssh-level failure (connect/auth/protocol). Distinct from any
		// remote exit 1..254 which we propagate transparently.
		s.finishAction(startedCh, true, runAuditText(s.audit, host, command, output, "ssh: exit status 255 (connect/auth/protocol)"))
	case s.audit.LogLevel() == "all":
		s.finishAction(startedCh, true, runText(host, command, output))
	default:
		s.finishAction(startedCh, true, actionText("▶️ run", host))
	}
	return [][]byte{
		[]byte(output),
		[]byte(strconv.Itoa(exitCode)),
	}, nil
}

func (s *controlServer) get(ctx context.Context, args [][]byte) ([][]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("usage: GET <host> <remote> <local>")
	}
	host, remote, local := string(args[0]), string(args[1]), string(args[2])
	startedCh := s.startAction(transferStartAuditText(s.audit, "⬅️ get", host, remote, local))
	if err := copyFromTargetSelector(ctx, s.cfg, s.st, host, remote, local); err != nil {
		s.finishAction(startedCh, true, transferAuditText(s.audit, "⬅️ get", host, remote, local, err.Error()))
		return nil, err
	}
	s.finishAction(startedCh, true, transferAuditText(s.audit, "⬅️ get", host, remote, local, ""))
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

func (s *controlServer) hostSet(args [][]byte) ([][]byte, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, errors.New("usage: HOST_SET <name> remote_work_dir [path]")
	}
	fields := []string{string(args[0]), string(args[1])}
	if len(args) == 3 {
		fields = append(fields, string(args[2]))
	}
	if _, err := setHostField(fields, s.cfg); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *controlServer) hostRemove(args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("usage: HOST_RM <name>")
	}
	if err := s.cfg.RemoveTarget(string(args[0])); err != nil {
		return nil, err
	}
	if err := writeKnownHosts(s.cfg); err != nil {
		return nil, fmt.Errorf("write known_hosts: %w", err)
	}
	return nil, nil
}

func (s *controlServer) groupAdd(args [][]byte) ([][]byte, error) {
	if len(args) != 2 {
		return nil, errors.New("usage: GROUP_ADD <group> <host>")
	}
	if _, err := s.cfg.AddHostGroup(string(args[1]), string(args[0])); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *controlServer) groupRemove(args [][]byte) ([][]byte, error) {
	if len(args) != 2 {
		return nil, errors.New("usage: GROUP_RM <group> <host>")
	}
	if _, err := s.cfg.RemoveHostGroup(string(args[1]), string(args[0])); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *controlServer) put(ctx context.Context, args [][]byte) ([][]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("usage: PUT <host> <local> <remote>")
	}
	host, local, remote := string(args[0]), string(args[1]), string(args[2])
	startedCh := s.startAction(transferStartAuditText(s.audit, "➡️ put", host, local, remote))
	if err := copyToTargetSelector(ctx, s.cfg, s.st, host, local, remote); err != nil {
		s.finishAction(startedCh, true, transferAuditText(s.audit, "➡️ put", host, local, remote, err.Error()))
		return nil, err
	}
	s.finishAction(startedCh, true, transferAuditText(s.audit, "➡️ put", host, local, remote, ""))
	return nil, nil
}

func (s *controlServer) startAction(text string) <-chan []actionMessage {
	ch := make(chan []actionMessage, 1)
	go func() {
		ch <- sendActionStart(s.cfg, s.bot, true, text)
	}()
	return ch
}

func (s *controlServer) finishAction(startedCh <-chan []actionMessage, html bool, text string) {
	go func() {
		started := <-startedCh
		if len(started) > 0 {
			editActionMessages(s.out, s.bot, started, html, text)
			return
		}
		notify(s.out, s.cfg, s.bot, html, text)
	}()
}

// --- CLI ---

func runUnseal(args []string) {
	if len(args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman unseal [passphrase]")
		os.Exit(1)
	}
	passphrase, err := resolveCLIUnseal(args)
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

// resolveCLIUnseal collects the passphrase from the most explicit source
// available: an explicit arg, piped stdin, or the configured/default
// unseal command (with stdin inherited so interactive helpers can prompt).
func resolveCLIUnseal(args []string) (string, error) {
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
	return interactiveUnseal(context.Background(), loadConfigOrExit())
}

func runSeal() { simpleControl("SEAL") }
func runLock() { simpleControl("LOCK") }

func runLocalRun(args []string) {
	target, command, err := resolveRunCommand(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	parts, err := requestControl("RUN", target, command)
	if err != nil {
		// Transport-level failure (target unknown, key locked, ssh couldn't
		// start). Use 255 to match ssh's own "something went wrong" exit code.
		printOpError(err)
		os.Exit(255)
	}
	if len(parts) > 0 && len(parts[0]) > 0 {
		os.Stdout.Write(parts[0])
		if !strings.HasSuffix(string(parts[0]), "\n") {
			fmt.Println()
		}
	}
	exitCode := 0
	if len(parts) > 1 {
		if code, perr := strconv.Atoi(strings.TrimSpace(string(parts[1]))); perr == nil {
			exitCode = code
		}
	}
	os.Exit(exitCode)
}

func resolveRunCommand(args []string) (string, string, error) {
	if len(args) < 1 {
		return "", "", errors.New("usage: picoman run <target> [<command>...]")
	}
	if len(args) > 1 {
		return args[0], strings.Join(args[1:], " "), nil
	}
	info, err := os.Stdin.Stat()
	if err != nil {
		return "", "", err
	}
	if info.Mode()&os.ModeCharDevice != 0 {
		return "", "", errors.New("usage: picoman run <target> [<command>...]")
	}
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", "", err
	}
	return args[0], strings.TrimRight(string(data), "\r\n"), nil
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
	opControl("GET", args[0], args[1], localName)
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
	opControl("PUT", args[0], args[1], remoteName)
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
		fmt.Fprintln(os.Stderr, hostCommandsHelp())
		os.Exit(1)
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "usage: picoman host list")
			os.Exit(1)
		}
		runHostList()
	case "info":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: picoman host info <name>")
			os.Exit(1)
		}
		runHostInfo(args[1])
	case "add":
		runHostAdd(args[1:])
	case "note":
		runHostNote(args[1:])
	case "set":
		runHostSet(args[1:])
	case "rm", "remove":
		runHostRemove(args[1:])
	default:
		fmt.Fprintln(os.Stderr, hostCommandsHelp())
		os.Exit(1)
	}
}

func hostCommandsHelp() string {
	return strings.Join([]string{
		"host commands:",
		"  host list",
		"  host info <name>",
		"  host add [<name> [<user>@<host>:<port> <keytype> <key>]]",
		"  host note <name> [note]",
		"  host set <name> remote_work_dir [path]",
		"  host remove <name>",
	}, "\n")
}

func runHosts(args []string) {
	if len(args) == 0 || args[0] == "list" {
		runHostList()
		return
	}
	fmt.Fprintln(os.Stderr, "usage: picoman hosts")
	os.Exit(1)
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

func runHostInfo(name string) {
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
	if len(t.Groups) > 0 {
		fmt.Println("groups: " + strings.Join(t.Groups, ", "))
	}
	if t.RemoteWorkDir != "" {
		fmt.Println("remote_work_dir: " + t.RemoteWorkDir)
	}
}

func runHostAdd(args []string) {
	switch len(args) {
	case 0:
		printBootstrap("")
	case 1:
		if !config.ValidName(args[0]) {
			fmt.Fprintf(os.Stderr, "bad host name %q\n", args[0])
			os.Exit(1)
		}
		printBootstrap(args[0])
	case 4:
		simpleControl("HOST_ADD", args[0], args[1], args[2], args[3])
	default:
		fmt.Fprintln(os.Stderr, "usage: picoman host add [<name> [<user>@<host>:<port> <keytype> <key>]]")
		os.Exit(1)
	}
}

func printBootstrap(name string) {
	cfg := loadConfigOrExit()
	line, err := hostBootstrapLine(cfg, name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(line)
}

func runHostNote(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman host note <name> [note]")
		os.Exit(1)
	}
	simpleControl("HOST_NOTE", args[0], strings.Join(args[1:], " "))
}

func runHostSet(args []string) {
	if len(args) < 2 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: picoman host set <name> remote_work_dir [path]")
		os.Exit(1)
	}
	switch args[1] {
	case "remote_work_dir":
		value := ""
		if len(args) == 3 {
			value = args[2]
		}
		simpleControl("HOST_SET", args[0], args[1], value)
	default:
		fmt.Fprintln(os.Stderr, "usage: picoman host set <name> remote_work_dir [path]")
		os.Exit(1)
	}
}

func runHostRemove(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman host remove <name>")
		os.Exit(1)
	}
	simpleControl("HOST_RM", args[0])
}

func runGroup(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, groupCommandsHelp())
		os.Exit(1)
	}
	if args[0] == "list" {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "usage: picoman group list")
			os.Exit(1)
		}
		runGroupList()
		return
	}
	switch args[0] {
	case "info":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: picoman group info <target-expression>")
			os.Exit(1)
		}
		runGroupShow(args[1])
		return
	case "add":
		runGroupModify("add", args[1:])
		return
	case "rm", "remove":
		runGroupModify("remove", args[1:])
		return
	}
	fmt.Fprintln(os.Stderr, groupCommandsHelp())
	os.Exit(1)
}

func groupCommandsHelp() string {
	return strings.Join([]string{
		"group commands:",
		"  group list",
		"  group info <target-expression>",
		"  group add @<group> <host>",
		"  group remove @<group> <host>",
	}, "\n")
}

func runGroupModify(action string, args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: picoman group "+action+" @<group> <host>")
		os.Exit(1)
	}
	group, err := parseGroupSelector(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if group == builtinAllGroup {
		fmt.Fprintln(os.Stderr, "cannot modify built-in group @all")
		os.Exit(1)
	}
	host := args[1]
	switch action {
	case "add":
		simpleControl("GROUP_ADD", group, host)
	case "remove":
		simpleControl("GROUP_RM", group, host)
	default:
		fmt.Fprintln(os.Stderr, "usage: picoman group add @<group> <host> | group remove @<group> <host>")
		os.Exit(1)
	}
}

func runGroups(args []string) {
	if len(args) == 0 || args[0] == "list" {
		runGroupList()
		return
	}
	fmt.Fprintln(os.Stderr, "usage: picoman groups")
	os.Exit(1)
}

func runGroupList() {
	cfg := loadHosts()
	names := cfg.GroupNames()
	fmt.Println("groups list")
	fmt.Println("- @all")
	for _, name := range names {
		if name == builtinAllGroup {
			continue
		}
		fmt.Println("- @" + name)
	}
}

func runGroupShow(selector string) {
	cfg := loadHosts()
	text, err := groupInfoText(cfg, selector, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(text)
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

// printOpError reports a control error for an operator-run run/get/put. Unlock
// is reachable only by the user from Telegram, so when the daemon reports the
// key is locked the operator cannot fix it locally — point them at the user.
func printOpError(err error) {
	if strings.Contains(err.Error(), "key is locked") {
		fmt.Fprintln(os.Stderr, "key is locked, ask user to unlock")
		return
	}
	fmt.Fprintln(os.Stderr, err)
}

// opControl runs a run/get/put-style request and routes failures through
// printOpError so the locked-key hint is shown.
func opControl(verb string, args ...string) {
	parts, err := requestControl(verb, args...)
	if err != nil {
		printOpError(err)
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

func loadConfigOrExit() *config.Config {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot load config: %v\n", err)
		os.Exit(1)
	}
	return cfg
}
