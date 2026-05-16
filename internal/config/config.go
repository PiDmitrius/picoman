package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type Target struct {
	User string `json:"user"`
	Host string `json:"host"`
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

func Default() *Config {
	return &Config{
		AgentSocket:   filepath.Join(DataDir(), "agent.sock"),
		ControlSocket: filepath.Join(DataDir(), "control.sock"),
		MaxUnlockTTL:  "15m",
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
	if c.Targets == nil {
		c.Targets = map[string]Target{}
	}
	if c.AllowedUsers == nil {
		c.AllowedUsers = []int64{}
	}
	return c
}
