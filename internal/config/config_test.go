package config

import (
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
