package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	TelegramToken string            `json:"tg_token"`
	AllowedUsers  []int64           `json:"tg_allowed_users"`
	KeyPath       string            `json:"key_path"`
	KeyPassphrase string            `json:"key_passphrase"`
	AgentSocket   string            `json:"agent_socket"`
	ControlSocket string            `json:"control_socket"`
	MaxUnlockTTL  string            `json:"max_unlock_ttl"`
	SourceDir     string            `json:"source_dir"`
	HostDB        string            `json:"host_db"`
	WorkDir       string            `json:"work_dir"`
	RemoteWorkDir string            `json:"remote_work_dir"`
	LogLevel      string            `json:"loglevel"`
	Targets       map[string]Target `json:"targets"`
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
	return normalize(&c), nil
}

func Save(c *Config) error {
	dir := Dir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(normalize(c), "", "  ")
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

func SaveHostDB(c *Config) error {
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

func normalize(c *Config) *Config {
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
	if c.LogLevel != "all" {
		c.LogLevel = def.LogLevel
	}
	if c.Targets == nil {
		c.Targets = map[string]Target{}
	}
	if c.AllowedUsers == nil {
		c.AllowedUsers = []int64{}
	}
	return c
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
