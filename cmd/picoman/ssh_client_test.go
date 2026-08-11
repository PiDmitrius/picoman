package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"picoman/internal/agent"
	"picoman/internal/config"
)

func TestDialSSHHandshakeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	state, publicKey := unlockedTestState(t)
	port := listener.Addr().(*net.TCPAddr).Port
	cfg := &config.Config{SSHConnectTimeout: "50ms"}
	target := config.Target{User: "user", Host: "127.0.0.1", Port: port, PublicKey: publicKey}
	started := time.Now()
	if _, _, err := dialSSH(context.Background(), cfg, state, target); err == nil {
		t.Fatal("dialSSH succeeded against a server that never spoke SSH")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("SSH handshake exceeded timeout: %s", elapsed)
	}
	select {
	case conn := <-accepted:
		conn.Close()
	case <-time.After(time.Second):
		t.Fatal("test server did not accept connection")
	}
}

func TestDialSSHHandshakeCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	state, publicKey := unlockedTestState(t)
	port := listener.Addr().(*net.TCPAddr).Port
	cfg := &config.Config{SSHConnectTimeout: "30s"}
	target := config.Target{User: "user", Host: "127.0.0.1", Port: port, PublicKey: publicKey}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := dialSSH(ctx, cfg, state, target)
		done <- err
	}()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("test server did not accept connection")
	}
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("dialSSH succeeded after cancellation")
		}
		if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
			t.Fatalf("handshake cancellation took %s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake did not stop after cancellation")
	}
}

func TestPinnedHostKeyIsRequired(t *testing.T) {
	if _, err := parsePinnedHostKey(""); err == nil {
		t.Fatal("empty host key accepted")
	}
	if _, err := parsePinnedHostKey("not a key"); err == nil {
		t.Fatal("invalid host key accepted")
	}
	_, publicKey := unlockedTestState(t)
	if _, err := parsePinnedHostKey(publicKey); err != nil {
		t.Fatalf("valid host key rejected: %v", err)
	}
}

func TestPinnedRSAHostKeyUsesSHA2Algorithms(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	got := pinnedHostKeyAlgorithms(signer.PublicKey())
	want := []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("RSA host-key algorithms = %v, want %v", got, want)
	}
}

func TestLockedBufferAcceptsConcurrentStreams(t *testing.T) {
	var buffer lockedBuffer
	done := make(chan struct{}, 2)
	for _, value := range []string{"stdout", "stderr"} {
		go func() {
			for range 100 {
				_, _ = buffer.Write([]byte(value))
			}
			done <- struct{}{}
		}()
	}
	<-done
	<-done
	got := buffer.String()
	if strings.Count(got, "stdout") != 100 || strings.Count(got, "stderr") != 100 {
		t.Fatalf("concurrent output lost data: %d bytes", len(got))
	}
}

func TestSFTPPutAndGet(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server := sftp.NewRequestServer(serverConn, sftp.InMemHandler())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve() }()
	client, err := sftp.NewClientPipe(clientConn, clientConn)
	if err != nil {
		t.Fatal(err)
	}

	localDir := t.TempDir()
	source := filepath.Join(localDir, "source")
	destination := filepath.Join(localDir, "destination")
	want := []byte("picoman sftp round trip\n")
	if err := os.WriteFile(source, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := uploadSFTP(client, source, "/remote"); err != nil {
		t.Fatalf("uploadSFTP: %v", err)
	}
	want = []byte("atomic replacement\n")
	if err := os.WriteFile(source, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := uploadSFTP(client, source, "/remote"); err != nil {
		t.Fatalf("replacement uploadSFTP: %v", err)
	}
	if err := downloadSFTP(client, "/remote", destination); err != nil {
		t.Fatalf("downloadSFTP: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("download = %q, want %q", got, want)
	}

	client.Close()
	server.Close()
	clientConn.Close()
	serverConn.Close()
	if err := <-serverDone; err != nil && err != io.EOF {
		t.Fatalf("SFTP server: %v", err)
	}
}

func TestProductionCodeHasNoExternalSSHCapability(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	forbidden := []string{"SSH_AUTH_SOCK", `CommandContext(ctx, "ssh"`, `CommandContext(ctx, "scp"`, `Command("ssh"`, `Command("scp"`}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, value := range forbidden {
			if strings.Contains(string(data), value) {
				t.Errorf("%s contains forbidden SSH capability %q", path, value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func unlockedTestState(t *testing.T) (*agent.State, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKeyWithPassphrase(privateKey, "test", []byte("passphrase"))
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "id_ed25519")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	state := agent.New(keyPath, time.Minute)
	if err := state.Unseal("passphrase"); err != nil {
		t.Fatal(err)
	}
	if err := state.Unlock(time.Minute); err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return state, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
}
