package config

import (
	"testing"
	"time"
)

func TestSSHTimeoutDefaults(t *testing.T) {
	cfg, err := normalize(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	if got := SSHConnectTimeout(cfg); got != 10*time.Second {
		t.Fatalf("connect timeout = %s", got)
	}
}

func TestSSHTimeoutValidation(t *testing.T) {
	for _, cfg := range []*Config{
		{SSHConnectTimeout: "bad"},
		{SSHConnectTimeout: "-1s"},
	} {
		if _, err := normalize(cfg); err == nil {
			t.Fatalf("normalize accepted invalid timeouts: %#v", cfg)
		}
	}
}
