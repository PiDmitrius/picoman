package main

import (
	"bufio"
	"context"
	"encoding/base64"
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

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		go handleControl(conn, cfg, st, out, bot, audit)
	}
}

func handleControl(conn net.Conn, cfg *config.Config, st *agent.State, out *outbox.Store, bot *tg.Client, audit *auditState) {
	defer conn.Close()

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return
	}
	line = strings.TrimSpace(line)

	switch {
	case strings.HasPrefix(line, "UNSEAL "):
		raw := strings.TrimPrefix(line, "UNSEAL ")
		data, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			_, _ = io.WriteString(conn, "ERR bad secret\n")
			return
		}
		st.Unseal(string(data))
		notifyUsers(out, cfg, bot, unsealText())
		_, _ = io.WriteString(conn, "OK\n")
	case line == "SEAL":
		if err := st.Lock(); err != nil {
			notifyUsers(out, cfg, bot, errorText("seal failed: "+err.Error()))
			_, _ = io.WriteString(conn, "ERR "+err.Error()+"\n")
			return
		}
		st.Seal()
		notifyUsers(out, cfg, bot, "⚪ sealed")
		_, _ = io.WriteString(conn, "OK\n")
	case line == "ASKPASS":
		passphrase := st.Passphrase()
		if passphrase == "" {
			_, _ = io.WriteString(conn, "\n")
			return
		}
		_, _ = io.WriteString(conn, passphrase+"\n")
	case strings.HasPrefix(line, "UNLOCK "):
		rawTTL := strings.TrimPrefix(line, "UNLOCK ")
		ttl, err := time.ParseDuration(rawTTL)
		if err != nil {
			_, _ = io.WriteString(conn, "ERR bad ttl\n")
			return
		}
		if err := st.Unlock(ttl); err != nil {
			notifyUsers(out, cfg, bot, errorText("unlock failed: "+err.Error()))
			_, _ = io.WriteString(conn, "ERR "+err.Error()+"\n")
			return
		}
		notifyUsers(out, cfg, bot, "🟡 unlocked ("+leftText(st.Until())+")")
		_, _ = io.WriteString(conn, "OK\n")
	case line == "LOCK":
		if err := st.Lock(); err != nil {
			notifyUsers(out, cfg, bot, errorText("lock failed: "+err.Error()))
			_, _ = io.WriteString(conn, "ERR "+err.Error()+"\n")
			return
		}
		notifyUsers(out, cfg, bot, "🔒 locked")
		_, _ = io.WriteString(conn, "OK\n")
	case strings.HasPrefix(line, "LOGLEVEL "):
		level := strings.TrimPrefix(line, "LOGLEVEL ")
		if !audit.SetLogLevel(level) {
			_, _ = io.WriteString(conn, "ERR bad loglevel\n")
			return
		}
		notifyUsers(out, cfg, bot, "⚙️ loglevel "+level)
		_, _ = io.WriteString(conn, "OK\n")
	case strings.HasPrefix(line, "RUN "):
		parts := strings.SplitN(strings.TrimPrefix(line, "RUN "), " ", 2)
		if len(parts) != 2 {
			_, _ = io.WriteString(conn, "ERR bad run request\n")
			return
		}
		command, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			_, _ = io.WriteString(conn, "ERR bad command\n")
			return
		}
		output, err := runTarget(context.Background(), cfg, st, parts[0], string(command))
		if err != nil {
			notifyUsersHTML(out, cfg, bot, runErrorText(parts[0], string(command), err.Error()))
			_, _ = io.WriteString(conn, "ERR "+err.Error()+"\n")
			return
		}
		if audit.LogLevel() == "all" {
			notifyUsersHTML(out, cfg, bot, runText(parts[0], string(command), output))
		} else {
			notifyUsersHTML(out, cfg, bot, actionText("▶️ run", parts[0]))
		}
		_, _ = io.WriteString(conn, "OK "+base64.StdEncoding.EncodeToString([]byte(output))+"\n")
	case strings.HasPrefix(line, "GET "):
		parts := strings.Split(strings.TrimPrefix(line, "GET "), " ")
		if len(parts) != 3 {
			_, _ = io.WriteString(conn, "ERR bad get request\n")
			return
		}
		remoteName, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			_, _ = io.WriteString(conn, "ERR bad remote file\n")
			return
		}
		localName, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			_, _ = io.WriteString(conn, "ERR bad local file\n")
			return
		}
		err = copyFromTarget(context.Background(), cfg, st, parts[0], string(remoteName), string(localName))
		if err != nil {
			notifyUsersHTML(out, cfg, bot, transferErrorText("⬅️ get", parts[0], string(remoteName), string(localName), err.Error()))
			_, _ = io.WriteString(conn, "ERR "+err.Error()+"\n")
			return
		}
		notifyUsersHTML(out, cfg, bot, transferText("⬅️ get", parts[0], string(remoteName), string(localName)))
		_, _ = io.WriteString(conn, "OK\n")
	case strings.HasPrefix(line, "PUT "):
		parts := strings.Split(strings.TrimPrefix(line, "PUT "), " ")
		if len(parts) != 3 {
			_, _ = io.WriteString(conn, "ERR bad put request\n")
			return
		}
		localName, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			_, _ = io.WriteString(conn, "ERR bad local file\n")
			return
		}
		remoteName, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			_, _ = io.WriteString(conn, "ERR bad remote file\n")
			return
		}
		err = copyToTarget(context.Background(), cfg, st, parts[0], string(localName), string(remoteName))
		if err != nil {
			notifyUsersHTML(out, cfg, bot, transferErrorText("➡️ put", parts[0], string(localName), string(remoteName), err.Error()))
			_, _ = io.WriteString(conn, "ERR "+err.Error()+"\n")
			return
		}
		notifyUsersHTML(out, cfg, bot, transferText("➡️ put", parts[0], string(localName), string(remoteName)))
		_, _ = io.WriteString(conn, "OK\n")
	default:
		_, _ = io.WriteString(conn, "ERR unknown command\n")
	}
}

func runUnseal(args []string) {
	cfg := loadConfigOrExit()
	passphrase, err := readPassphrase(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if passphrase == "" {
		fmt.Fprintln(os.Stderr, "empty passphrase")
		os.Exit(1)
	}
	resp, err := controlRequest(cfg.ControlSocket, "UNSEAL "+base64.StdEncoding.EncodeToString([]byte(passphrase)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printControlResponse(resp)
}

func runSeal() {
	cfg := loadConfigOrExit()
	resp, err := controlRequest(cfg.ControlSocket, "SEAL")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printControlResponse(resp)
}

func runUnlock(args []string) {
	if len(args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman unlock [5m]")
		os.Exit(1)
	}
	ttl := "5m"
	if len(args) == 1 {
		ttl = args[0]
	}
	cfg := loadConfigOrExit()
	resp, err := controlRequest(cfg.ControlSocket, "UNLOCK "+ttl)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printControlResponse(resp)
}

func runLock() {
	cfg := loadConfigOrExit()
	resp, err := controlRequest(cfg.ControlSocket, "LOCK")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printControlResponse(resp)
}

func runLocalRun(args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: picoman run <target> <command>")
		os.Exit(1)
	}
	cfg := loadConfigOrExit()
	command := strings.Join(args[1:], " ")
	resp, err := controlRequest(cfg.ControlSocket, "RUN "+args[0]+" "+base64.StdEncoding.EncodeToString([]byte(command)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if strings.HasPrefix(resp, "ERR ") {
		fmt.Print(resp)
		os.Exit(1)
	}
	raw := strings.TrimSpace(strings.TrimPrefix(resp, "OK "))
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(data))
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
	cfg := loadConfigOrExit()
	resp, err := controlRequest(cfg.ControlSocket, "GET "+args[0]+" "+base64.StdEncoding.EncodeToString([]byte(args[1]))+" "+base64.StdEncoding.EncodeToString([]byte(localName)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printControlResponse(resp)
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
	cfg := loadConfigOrExit()
	resp, err := controlRequest(cfg.ControlSocket, "PUT "+args[0]+" "+base64.StdEncoding.EncodeToString([]byte(args[1]))+" "+base64.StdEncoding.EncodeToString([]byte(remoteName)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printControlResponse(resp)
}

func runLogLevel(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman loglevel <chat|all>")
		os.Exit(1)
	}
	cfg := loadConfigOrExit()
	resp, err := controlRequest(cfg.ControlSocket, "LOGLEVEL "+args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printControlResponse(resp)
}

func runAskpass() {
	cfg := loadConfigOrExit()
	resp, err := controlRequest(cfg.ControlSocket, "ASKPASS")
	if err != nil {
		os.Exit(1)
	}
	fmt.Print(resp)
}

func controlRequest(socket, line string) (string, error) {
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

func printControlResponse(resp string) {
	fmt.Print(resp)
	if strings.HasPrefix(resp, "ERR ") {
		os.Exit(1)
	}
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
