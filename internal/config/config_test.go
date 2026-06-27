package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTargetSnapshotDoesNotShareGroupsBackingArray(t *testing.T) {
	cfg := &Config{
		HostDB:  t.TempDir() + "/hosts.json",
		Targets: map[string]Target{},
	}
	if err := cfg.UpsertTarget("host", Target{User: "u", Host: "host.example", Groups: []string{"a", "b", "c"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := cfg.Target("host")
	if !ok {
		t.Fatal("missing target")
	}
	if _, err := cfg.RemoveHostGroup("host", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.AddHostGroup("host", "d"); err != nil {
		t.Fatal(err)
	}
	if got, want := snapshot.Groups, []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot groups = %#v, want %#v", got, want)
	}
}

func TestAllTargetsSnapshotDoesNotShareGroupsBackingArray(t *testing.T) {
	cfg := &Config{
		HostDB:  t.TempDir() + "/hosts.json",
		Targets: map[string]Target{},
	}
	if err := cfg.UpsertTarget("host", Target{User: "u", Host: "host.example", Groups: []string{"a", "b", "c"}}); err != nil {
		t.Fatal(err)
	}
	snapshot := cfg.AllTargets()["host"]
	if _, err := cfg.RemoveHostGroup("host", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.AddHostGroup("host", "d"); err != nil {
		t.Fatal(err)
	}
	if got, want := snapshot.Groups, []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot groups = %#v, want %#v", got, want)
	}
}

func TestRemoveTargetDeletesHost(t *testing.T) {
	cfg := &Config{
		HostDB:  t.TempDir() + "/hosts.json",
		Targets: map[string]Target{},
	}
	if err := cfg.UpsertTarget("host", Target{User: "u", Host: "host.example"}); err != nil {
		t.Fatal(err)
	}
	if err := cfg.RemoveTarget("host"); err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Target("host"); ok {
		t.Fatal("target still exists after RemoveTarget")
	}
	if err := cfg.RemoveTarget("host"); err == nil {
		t.Fatal("RemoveTarget accepted unknown host")
	}
}

func TestSetHostRemoteWorkDirPersistsAndClears(t *testing.T) {
	hostDB := t.TempDir() + "/hosts.json"
	cfg := &Config{
		HostDB:  hostDB,
		Targets: map[string]Target{},
	}
	if err := cfg.UpsertTarget("host", Target{User: "u", Host: "host.example"}); err != nil {
		t.Fatal(err)
	}
	target, err := cfg.SetHostRemoteWorkDir("host", "~/deploy")
	if err != nil {
		t.Fatal(err)
	}
	if target.RemoteWorkDir != "~/deploy" {
		t.Fatalf("RemoteWorkDir = %q, want ~/deploy", target.RemoteWorkDir)
	}
	data, err := os.ReadFile(hostDB)
	if err != nil {
		t.Fatal(err)
	}
	var db HostDB
	if err := json.Unmarshal(data, &db); err != nil {
		t.Fatal(err)
	}
	if got := db.Hosts["host"].RemoteWorkDir; got != "~/deploy" {
		t.Fatalf("saved RemoteWorkDir = %q, want ~/deploy", got)
	}

	target, err = cfg.SetHostRemoteWorkDir("host", "")
	if err != nil {
		t.Fatal(err)
	}
	if target.RemoteWorkDir != "" {
		t.Fatalf("RemoteWorkDir = %q, want empty", target.RemoteWorkDir)
	}
	data, err = os.ReadFile(hostDB)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]map[string]map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["hosts"]["host"]["remote_work_dir"]; ok {
		t.Fatal("remote_work_dir remained in hosts.json after clearing")
	}
}

func TestSetLogLevelPersistsConfigField(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PICOMAN_CONFIG_DIR", dir)
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"tg_token":"token","loglevel":"all"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{LogLevel: "all"}
	if err := cfg.SetLogLevel("chat"); err != nil {
		t.Fatal(err)
	}
	if got := cfg.LogLevel; got != "chat" {
		t.Fatalf("LogLevel = %q, want chat", got)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if got := raw["loglevel"]; got != "chat" {
		t.Fatalf("persisted loglevel = %q, want chat", got)
	}
	if got := raw["tg_token"]; got != "token" {
		t.Fatalf("unrelated config field changed: %q", got)
	}
}
