package agent

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type State struct {
	// Immutable after New().
	socket  string
	pidPath string
	keyPath string
	maxTTL  time.Duration

	// ioMu serializes ssh-agent/ssh-add invocations so state-getters
	// (Sealed, IsUnlocked, Until) do not block on external I/O.
	ioMu sync.Mutex

	// stateMu protects the mutable fields below.
	stateMu    sync.RWMutex
	pid        int
	until      time.Time
	passphrase string
	startedAt  time.Time
}

type CleanResult struct {
	Agent   string
	Socket  string
	PIDFile string
}

func New(socket, keyPath string, maxTTL time.Duration) *State {
	return &State{socket: socket, pidPath: socket + ".pid", keyPath: keyPath, maxTTL: maxTTL}
}

func (s *State) CleanStart() CleanResult {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	agentStatus := s.killOldAgent()

	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.pid = 0
	s.until = time.Time{}
	s.passphrase = ""
	return CleanResult{
		Agent:   agentStatus,
		Socket:  removeStatus(s.socket),
		PIDFile: removeStatus(s.pidPath),
	}
}

func (r CleanResult) OK() bool {
	return r.Agent == "none" &&
		(r.Socket == "absent" || r.Socket == "removed") &&
		(r.PIDFile == "absent" || r.PIDFile == "removed")
}

func (r CleanResult) String() string {
	return fmt.Sprintf("agent: %s\nsocket: %s\npid_file: %s", r.Agent, r.Socket, r.PIDFile)
}

func (s *State) Unseal(passphrase string) error {
	if err := s.verifyPassphrase(passphrase); err != nil {
		return err
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.passphrase = passphrase
	return nil
}

func (s *State) verifyPassphrase(passphrase string) error {
	if s.keyPath == "" {
		return fmt.Errorf("key_path is empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ssh-keygen", "-y", "-P", passphrase, "-f", s.keyPath)
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		return err
	}
	defer devnull.Close()
	cmd.Stdin = devnull

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bad passphrase")
	}
	return nil
}

func (s *State) Seal() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.passphrase = ""
}

func (s *State) Passphrase() string {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.passphrase
}

func (s *State) Sealed() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.passphrase == ""
}

func (s *State) Unlock(ttl time.Duration) error {
	if s.keyPath == "" {
		return fmt.Errorf("key_path is empty")
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	if ttl > s.maxTTL {
		return fmt.Errorf("ttl exceeds max %s", s.maxTTL)
	}
	if s.Sealed() {
		return fmt.Errorf("sealed")
	}

	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	if err := s.ensureAgent(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ssh-add", "-t", strconv.Itoa(int(ttl.Seconds())), s.keyPath)
	cmd.Env = append(os.Environ(), "SSH_AUTH_SOCK="+s.socket)
	askpass, cleanup, err := makeAskpass()
	if err != nil {
		return err
	}
	defer cleanup()
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		return err
	}
	defer devnull.Close()
	cmd.Stdin = devnull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(cmd.Env,
		"SSH_ASKPASS="+askpass,
		"SSH_ASKPASS_REQUIRE=force",
		"DISPLAY=picoman",
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ssh-add: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	s.stateMu.Lock()
	s.until = time.Now().Add(ttl)
	s.stateMu.Unlock()
	return nil
}

func makeAskpass() (string, func(), error) {
	exe, err := os.Executable()
	if err != nil {
		return "", nil, err
	}
	f, err := os.CreateTemp("", "picoman-askpass-*")
	if err != nil {
		return "", nil, err
	}
	path := f.Name()
	script := "#!/bin/sh\nexec " + strconv.Quote(exe) + " askpass\n"
	if _, err := f.WriteString(script); err != nil {
		f.Close()
		os.Remove(path)
		return "", nil, err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", nil, err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		os.Remove(path)
		return "", nil, err
	}
	return path, func() { _ = os.Remove(path) }, nil
}

func (s *State) Lock() error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	s.stateMu.RLock()
	pid := s.pid
	s.stateMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ssh-add", "-D")
	cmd.Env = append(os.Environ(), "SSH_AUTH_SOCK="+s.socket)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil && pid != 0 {
		return fmt.Errorf("ssh-add -D: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	s.killOldAgent()
	_ = os.Remove(s.socket)
	_ = os.Remove(s.pidPath)
	s.stateMu.Lock()
	s.pid = 0
	s.until = time.Time{}
	s.stateMu.Unlock()
	return nil
}

func (s *State) IsUnlocked() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.pid != 0 && time.Now().Before(s.until)
}

func (s *State) Until() time.Time {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.until
}

func (s *State) Socket() string {
	return s.socket
}

// ensureAgent starts ssh-agent if not already running.
// Caller must hold ioMu.
func (s *State) ensureAgent() error {
	s.stateMu.RLock()
	pid := s.pid
	s.stateMu.RUnlock()
	if pid != 0 {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.socket), 0o700); err != nil {
		return err
	}
	_ = os.Remove(s.socket)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ssh-agent", "-a", s.socket, "-s")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ssh-agent: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	newPID, err := parseAgentPID(stdout.String())
	if err != nil {
		return err
	}

	s.stateMu.Lock()
	s.pid = newPID
	s.startedAt = time.Now()
	s.stateMu.Unlock()
	_ = os.WriteFile(s.pidPath, []byte(strconv.Itoa(newPID)), 0o600)
	return nil
}

// killOldAgent stops any tracked ssh-agent. Caller must hold ioMu.
func (s *State) killOldAgent() string {
	s.stateMu.RLock()
	pid := s.pid
	s.stateMu.RUnlock()
	if pid == 0 {
		data, err := os.ReadFile(s.pidPath)
		if err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
	}
	if pid > 0 {
		if !isSSHAgent(pid) {
			return fmt.Sprintf("agent: pid %d is not ssh-agent, left untouched", pid)
		}
		if p, err := os.FindProcess(pid); err == nil {
			if err := p.Kill(); err != nil {
				return fmt.Sprintf("agent: kill pid %d failed: %v", pid, err)
			}
			_, _ = p.Wait()
			return fmt.Sprintf("agent: killed pid %d", pid)
		}
	}
	return "none"
}

func removeStatus(path string) string {
	err := os.Remove(path)
	switch {
	case err == nil:
		return "removed"
	case os.IsNotExist(err):
		return "absent"
	default:
		return "remove failed: " + err.Error()
	}
}

func isSSHAgent(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return false
	}
	return strings.Contains(string(data), "ssh-agent")
}

func parseAgentPID(output string) (int, error) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "SSH_AGENT_PID=") {
			continue
		}
		value := strings.TrimPrefix(line, "SSH_AGENT_PID=")
		value = strings.TrimSuffix(value, "; export SSH_AGENT_PID;")
		value = strings.TrimSpace(value)
		pid, err := strconv.Atoi(value)
		if err != nil {
			return 0, err
		}
		return pid, nil
	}
	return 0, fmt.Errorf("could not parse SSH_AGENT_PID from ssh-agent output")
}
