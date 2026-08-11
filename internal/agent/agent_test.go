package agent

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestCredentialLifecycle(t *testing.T) {
	keyPath := writeEncryptedKey(t, "correct")
	state := New(keyPath, time.Minute)

	if err := state.Unseal("wrong"); err == nil {
		t.Fatal("Unseal accepted a bad passphrase")
	}
	if err := state.Unseal("correct"); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if state.IsUnlocked() {
		t.Fatal("Unseal also unlocked the key")
	}
	if err := state.Unlock(time.Second); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	signer, err := state.Signer()
	if err != nil {
		t.Fatalf("Signer while unlocked: %v", err)
	}
	state.Lock()
	if _, err := state.Signer(); err == nil {
		t.Fatal("Signer remained available after lock")
	}
	if _, err := signer.Sign(rand.Reader, []byte("after lock")); err == nil {
		t.Fatal("captured signer signed after lock")
	}
	if state.Sealed() {
		t.Fatal("Lock unexpectedly sealed the state")
	}
	state.Seal()
	if !state.Sealed() {
		t.Fatal("Seal retained the passphrase")
	}
}

func TestUnlockTTLAndLimit(t *testing.T) {
	state := New(writeEncryptedKey(t, "passphrase"), 100*time.Millisecond)
	if err := state.Unseal("passphrase"); err != nil {
		t.Fatal(err)
	}
	if err := state.Unlock(101 * time.Millisecond); err == nil {
		t.Fatal("Unlock accepted a TTL over max")
	}
	if err := state.Unlock(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	signer, err := state.Signer()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := state.Signer(); err == nil {
		t.Fatal("Signer remained available after TTL")
	}
	if _, err := signer.Sign(rand.Reader, []byte("after ttl")); err == nil {
		t.Fatal("captured signer signed after TTL")
	}
}

func writeEncryptedKey(t *testing.T, passphrase string) string {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKeyWithPassphrase(privateKey, "test", []byte(passphrase))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "id_ed25519")
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
