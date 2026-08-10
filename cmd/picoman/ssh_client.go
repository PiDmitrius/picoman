package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"picoman/internal/agent"
	"picoman/internal/config"
)

func runSSH(ctx context.Context, cfg *config.Config, st *agent.State, t config.Target, command string) (string, int, error) {
	opCtx, cancel := context.WithTimeout(ctx, config.SSHOperationTimeout(cfg))
	defer cancel()

	client, closeClient, err := dialSSH(opCtx, cfg, st, t)
	if err != nil {
		return err.Error(), 255, nil
	}
	defer closeClient()

	session, err := client.NewSession()
	if err != nil {
		return err.Error(), 255, nil
	}
	defer session.Close()

	var output lockedBuffer
	session.Stdout = &output
	session.Stderr = &output
	err = session.Run(command)
	text := strings.TrimRight(output.String(), "\n")
	if err == nil {
		return text, 0, nil
	}
	var exitErr *ssh.ExitError
	if errors.As(err, &exitErr) {
		return text, exitErr.ExitStatus(), nil
	}
	if text != "" {
		text += "\n"
	}
	return text + err.Error(), 255, nil
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func getSFTP(ctx context.Context, cfg *config.Config, st *agent.State, t config.Target, remotePath, localPath string) error {
	opCtx, cancel := context.WithTimeout(ctx, config.SSHOperationTimeout(cfg))
	defer cancel()

	client, closeClient, err := dialSSH(opCtx, cfg, st, t)
	if err != nil {
		return err
	}
	defer closeClient()
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		return fmt.Errorf("start sftp: %w", err)
	}
	defer sftpClient.Close()
	return downloadSFTP(sftpClient, remotePath, localPath)
}

func downloadSFTP(sftpClient *sftp.Client, remotePath, localPath string) error {
	var err error
	remotePath, err = expandRemoteHome(sftpClient, remotePath)
	if err != nil {
		return err
	}
	remote, err := sftpClient.Open(remotePath)
	if err != nil {
		return fmt.Errorf("open remote file: %w", err)
	}
	defer remote.Close()

	tmp, err := os.CreateTemp(pathDir(localPath), ".picoman-get-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		tmp.Close()
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := io.Copy(tmp, remote); err != nil {
		return fmt.Errorf("download: %w", err)
	}
	if err := remote.Close(); err != nil {
		return fmt.Errorf("close remote file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, localPath); err != nil {
		return err
	}
	keep = true
	return nil
}

func putSFTP(ctx context.Context, cfg *config.Config, st *agent.State, t config.Target, localPath, remotePath string) error {
	opCtx, cancel := context.WithTimeout(ctx, config.SSHOperationTimeout(cfg))
	defer cancel()

	client, closeClient, err := dialSSH(opCtx, cfg, st, t)
	if err != nil {
		return err
	}
	defer closeClient()
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		return fmt.Errorf("start sftp: %w", err)
	}
	defer sftpClient.Close()
	return uploadSFTP(sftpClient, localPath, remotePath)
}

func uploadSFTP(sftpClient *sftp.Client, localPath, remotePath string) error {
	local, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer local.Close()

	remotePath, err = expandRemoteHome(sftpClient, remotePath)
	if err != nil {
		return err
	}
	tempPath, err := remoteTempPath(remotePath)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = sftpClient.Remove(tempPath)
		}
	}()
	remote, err := sftpClient.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return fmt.Errorf("open remote file: %w", err)
	}
	if _, err := io.Copy(remote, local); err != nil {
		remote.Close()
		return fmt.Errorf("upload: %w", err)
	}
	if err := remote.Close(); err != nil {
		return fmt.Errorf("close remote file: %w", err)
	}
	if _, ok := sftpClient.HasExtension("posix-rename@openssh.com"); ok {
		err = sftpClient.PosixRename(tempPath, remotePath)
	} else {
		err = sftpClient.Rename(tempPath, remotePath)
	}
	if err != nil {
		return fmt.Errorf("commit remote file: %w", err)
	}
	committed = true
	return nil
}

func remoteTempPath(destination string) (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("random remote temp name: %w", err)
	}
	name := "." + path.Base(destination) + ".picoman-" + hex.EncodeToString(random)
	return path.Join(path.Dir(destination), name), nil
}

func dialSSH(ctx context.Context, cfg *config.Config, st *agent.State, t config.Target) (*ssh.Client, func(), error) {
	signer, err := st.Signer()
	if err != nil {
		return nil, nil, err
	}
	hostKey, err := parsePinnedHostKey(t.PublicKey)
	if err != nil {
		return nil, nil, err
	}

	address := net.JoinHostPort(t.Host, strconv.Itoa(targetPort(t)))
	dialer := net.Dialer{Timeout: config.SSHConnectTimeout(cfg)}
	netConn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("connect %s: %w", address, err)
	}
	connected := false
	defer func() {
		if !connected {
			netConn.Close()
		}
	}()

	handshakeDeadline := time.Now().Add(config.SSHConnectTimeout(cfg))
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(handshakeDeadline) {
		handshakeDeadline = deadline
	}
	if err := netConn.SetDeadline(handshakeDeadline); err != nil {
		return nil, nil, err
	}
	clientConfig := &ssh.ClientConfig{
		User:              t.User,
		Auth:              []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback:   ssh.FixedHostKey(hostKey),
		HostKeyAlgorithms: pinnedHostKeyAlgorithms(hostKey),
	}
	conn, channels, requests, err := ssh.NewClientConn(netConn, address, clientConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("ssh handshake: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := netConn.SetDeadline(deadline); err != nil {
			conn.Close()
			return nil, nil, err
		}
	} else if err := netConn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, nil, err
	}

	client := ssh.NewClient(conn, channels, requests)
	stopCancel := context.AfterFunc(ctx, func() { _ = client.Close() })
	connected = true
	closeClient := func() {
		stopCancel()
		_ = client.Close()
	}
	return client, closeClient, nil
}

func pinnedHostKeyAlgorithms(key ssh.PublicKey) []string {
	if key.Type() == ssh.KeyAlgoRSA {
		return []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
	}
	return []string{key.Type()}
}

func parsePinnedHostKey(value string) (ssh.PublicKey, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("target has no pinned host key")
	}
	key, _, _, rest, err := ssh.ParseAuthorizedKey([]byte(value))
	if err != nil || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, errors.New("target has invalid pinned host key")
	}
	return key, nil
}

func expandRemoteHome(client *sftp.Client, remotePath string) (string, error) {
	if remotePath != "~" && !strings.HasPrefix(remotePath, "~/") {
		return remotePath, nil
	}
	home, err := client.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve remote home: %w", err)
	}
	if remotePath == "~" {
		return home, nil
	}
	return path.Join(home, strings.TrimPrefix(remotePath, "~/")), nil
}

func pathDir(name string) string {
	i := strings.LastIndexByte(name, os.PathSeparator)
	if i < 0 {
		return "."
	}
	if i == 0 {
		return string(os.PathSeparator)
	}
	return name[:i]
}
