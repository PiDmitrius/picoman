package agent

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// State is the sole owner of decrypted SSH credential material. It never
// exports a socket or starts an external signer process.
type State struct {
	keyPath string
	maxTTL  time.Duration

	mu         sync.RWMutex
	until      time.Time
	passphrase string
	signer     ssh.Signer
	generation uint64
}

func New(keyPath string, maxTTL time.Duration) *State {
	return &State{keyPath: keyPath, maxTTL: maxTTL}
}

func (s *State) Unseal(passphrase string) error {
	if _, err := s.parseSigner(passphrase); err != nil {
		return err
	}
	s.mu.Lock()
	s.passphrase = passphrase
	s.mu.Unlock()
	return nil
}

func (s *State) parseSigner(passphrase string) (ssh.Signer, error) {
	if s.keyPath == "" {
		return nil, fmt.Errorf("key_path is empty")
	}
	key, err := os.ReadFile(s.keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	signer, err := ssh.ParsePrivateKeyWithPassphrase(key, []byte(passphrase))
	if err != nil {
		return nil, fmt.Errorf("bad passphrase")
	}
	return signer, nil
}

func (s *State) Seal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signer = nil
	s.until = time.Time{}
	s.passphrase = ""
	s.generation++
}

func (s *State) Sealed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.passphrase == ""
}

func (s *State) Unlock(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	if ttl > s.maxTTL {
		return fmt.Errorf("ttl exceeds max %s", s.maxTTL)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	passphrase := s.passphrase
	if passphrase == "" {
		return fmt.Errorf("sealed")
	}
	signer, err := s.parseSigner(passphrase)
	if err != nil {
		return err
	}

	s.signer = signer
	s.until = time.Now().Add(ttl)
	s.generation++
	return nil
}

func (s *State) Lock() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lock()
}

func (s *State) LockExpired(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.signer == nil || s.until.IsZero() || now.Before(s.until) {
		return false
	}
	s.lock()
	return true
}

func (s *State) lock() {
	s.signer = nil
	s.until = time.Time{}
	s.generation++
}

func (s *State) IsUnlocked() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.signer != nil && time.Now().Before(s.until)
}

func (s *State) Until() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.until
}

func (s *State) Signer() (ssh.Signer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.signer == nil || !time.Now().Before(s.until) {
		return nil, fmt.Errorf("key is locked")
	}
	return &guardedSigner{state: s, generation: s.generation, publicKey: s.signer.PublicKey()}, nil
}

type guardedSigner struct {
	state      *State
	generation uint64
	publicKey  ssh.PublicKey
}

func (s *guardedSigner) PublicKey() ssh.PublicKey { return s.publicKey }

func (s *guardedSigner) Sign(random io.Reader, data []byte) (*ssh.Signature, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	signer, err := s.currentSigner()
	if err != nil {
		return nil, err
	}
	return signer.Sign(random, data)
}

func (s *guardedSigner) SignWithAlgorithm(random io.Reader, data []byte, algorithm string) (*ssh.Signature, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	signer, err := s.currentSigner()
	if err != nil {
		return nil, err
	}
	algorithmSigner, ok := signer.(ssh.AlgorithmSigner)
	if !ok {
		return nil, fmt.Errorf("signer does not support algorithm selection")
	}
	return algorithmSigner.SignWithAlgorithm(random, data, algorithm)
}

func (s *guardedSigner) currentSigner() (ssh.Signer, error) {
	if s.state.signer == nil || s.state.generation != s.generation || !time.Now().Before(s.state.until) {
		return nil, fmt.Errorf("key is locked")
	}
	return s.state.signer, nil
}

var _ ssh.AlgorithmSigner = (*guardedSigner)(nil)
