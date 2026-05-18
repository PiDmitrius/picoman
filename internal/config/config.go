package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

type Target struct {
	User      string `json:"user"`
	Host      string `json:"host"`
	Port      int    `json:"port,omitempty"`
	PublicKey string `json:"public_key,omitempty"`
	WorkDir   string `json:"work_dir,omitempty"`
	Disabled  bool   `json:"disabled,omitempty"`
	Note      string `json:"note,omitempty"`
}

type HostDB struct {
	Hosts map[string]Target `json:"hosts"`
}

type Config struct {
	TelegramToken        string            `json:"tg_token"`
	AllowedUsers         []int64           `json:"tg_allowed_users"`
	KeyPath              string            `json:"key_path"`
	KeyPassphrase        string            `json:"key_passphrase"`
	KeyPassphraseCommand string            `json:"key_passphrase_command,omitempty"`
	AgentSocket          string            `json:"agent_socket"`
	ControlSocket        string            `json:"control_socket"`
	MaxUnlockTTL         string            `json:"max_unlock_ttl"`
	DeveloperDir         string            `json:"developer_dir"`
	HostDB               string            `json:"host_db"`
	WorkDir              string            `json:"work_dir"`
	RemoteWorkDir        string            `json:"remote_work_dir"`
	LogLevel             string            `json:"loglevel"`
	Targets              map[string]Target `json:"targets"`

	mu sync.RWMutex
}

func (c *Config) Target(name string) (Target, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.Targets[name]
	return t, ok
}

func (c *Config) HostNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.Targets))
	for name := range c.Targets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *Config) AllTargets() map[string]Target {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]Target, len(c.Targets))
	for k, v := range c.Targets {
		out[k] = v
	}
	return out
}

func (c *Config) UpsertTarget(name string, t Target) error {
	if err := ValidateTarget(name, t); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Targets == nil {
		c.Targets = map[string]Target{}
	}
	c.Targets[name] = t
	return saveHostDBLocked(c)
}

func (c *Config) SetHostNote(name, note string) (Target, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.Targets[name]
	if !ok {
		return Target{}, fmt.Errorf("unknown host %q", name)
	}
	t.Note = note
	c.Targets[name] = t
	if err := saveHostDBLocked(c); err != nil {
		return Target{}, err
	}
	return t, nil
}

func Dir() string {
	if d := os.Getenv("PICOMAN_CONFIG_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "picoman")
}

func DataDir() string {
	if d := os.Getenv("PICOMAN_DATA_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "picoman")
}

func DBPath() string {
	return filepath.Join(DataDir(), "picoman.sqlite")
}

func KnownHostsPath() string {
	return filepath.Join(DataDir(), "known_hosts")
}

func Default() *Config {
	home, _ := os.UserHomeDir()
	return &Config{
		AgentSocket:   filepath.Join(DataDir(), "agent.sock"),
		ControlSocket: filepath.Join(DataDir(), "control.sock"),
		MaxUnlockTTL:  "15m",
		HostDB:        filepath.Join(Dir(), "hosts.json"),
		WorkDir:       filepath.Join(home, "picoman"),
		RemoteWorkDir: "~/picoman",
		LogLevel:      "chat",
		Targets:       map[string]Target{},
	}
}

func Load() (*Config, error) {
	path := filepath.Join(Dir(), "config.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return normalize(&c)
}

func Save(c *Config) error {
	dir := Dir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	norm, err := normalize(c)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(norm, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "config.json"), data, 0o600)
}

func LoadHostDB(c *Config) error {
	if c.HostDB == "" {
		return nil
	}
	data, err := os.ReadFile(c.HostDB)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var db HostDB
	if err := json.Unmarshal(data, &db); err != nil {
		return err
	}
	if db.Hosts == nil {
		db.Hosts = map[string]Target{}
	}
	for name, target := range db.Hosts {
		if err := ValidateTarget(name, target); err != nil {
			return err
		}
	}
	c.Targets = db.Hosts
	return nil
}

// saveHostDBLocked writes the host DB. Caller must hold c.mu.
func saveHostDBLocked(c *Config) error {
	if c.HostDB == "" {
		return fmt.Errorf("host_db is empty")
	}
	dir := filepath.Dir(c.HostDB)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	db := HostDB{Hosts: c.Targets}
	data, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".hosts-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if _, err := tmp.WriteString("\n"); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, c.HostDB)
}

func MaxTTL(c *Config) time.Duration {
	d, err := time.ParseDuration(c.MaxUnlockTTL)
	if err != nil || d <= 0 {
		return 15 * time.Minute
	}
	return d
}

func AllowedSet(c *Config) map[int64]bool {
	out := make(map[int64]bool, len(c.AllowedUsers))
	for _, id := range c.AllowedUsers {
		out[id] = true
	}
	return out
}

func normalize(c *Config) (*Config, error) {
	if c == nil {
		c = Default()
	}
	def := Default()
	if c.AgentSocket == "" {
		c.AgentSocket = def.AgentSocket
	}
	if c.ControlSocket == "" {
		c.ControlSocket = def.ControlSocket
	}
	if c.MaxUnlockTTL == "" {
		c.MaxUnlockTTL = def.MaxUnlockTTL
	}
	if c.HostDB == "" {
		c.HostDB = def.HostDB
	}
	if c.WorkDir == "" {
		c.WorkDir = def.WorkDir
	}
	if c.RemoteWorkDir == "" {
		c.RemoteWorkDir = def.RemoteWorkDir
	}
	switch c.LogLevel {
	case "":
		c.LogLevel = def.LogLevel
	case "chat", "all":
	default:
		return nil, fmt.Errorf("invalid log level %q", c.LogLevel)
	}
	if c.KeyPassphrase != "" && c.KeyPassphraseCommand != "" {
		return nil, fmt.Errorf("set exactly one of key_passphrase or key_passphrase_command, not both")
	}
	if c.Targets == nil {
		c.Targets = map[string]Target{}
	}
	if c.AllowedUsers == nil {
		c.AllowedUsers = []int64{}
	}
	return c, nil
}

var (
	targetNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)
	userRe       = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,31}$`)
	hostRe       = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9.:-]{0,253}$`)
)

func ValidName(name string) bool { return targetNameRe.MatchString(name) }

func ValidateTarget(name string, target Target) error {
	if !targetNameRe.MatchString(name) {
		return fmt.Errorf("bad target name %q", name)
	}
	if !userRe.MatchString(target.User) {
		return fmt.Errorf("target %q has bad user", name)
	}
	if !hostRe.MatchString(target.Host) {
		return fmt.Errorf("target %q has bad host", name)
	}
	if target.Port < 0 || target.Port > 65535 {
		return fmt.Errorf("target %q port is invalid", name)
	}
	return nil
}
