package main

import (
	"reflect"
	"testing"

	"picoman/internal/config"
)

func TestNormalizeCommandFieldsSplitsUnderscoreArguments(t *testing.T) {
	name, fields := normalizeCommandFields([]string{"/unlock_1h"})
	if name != "unlock" {
		t.Fatalf("name = %q, want unlock", name)
	}
	want := []string{"/unlock", "1h"}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields = %#v, want %#v", fields, want)
	}
}

func TestNormalizeCommandFieldsKeepsVersionCommands(t *testing.T) {
	in := []string{"/v1_2_3"}
	name, fields := normalizeCommandFields(in)
	if name != "v1_2_3" {
		t.Fatalf("name = %q, want v1_2_3", name)
	}
	if !reflect.DeepEqual(fields, in) {
		t.Fatalf("fields = %#v, want %#v", fields, in)
	}
}

func TestNormalizeCommandFieldsKeepsRegularUnlock(t *testing.T) {
	in := []string{"/unlock", "5m"}
	name, fields := normalizeCommandFields(in)
	if name != "unlock" {
		t.Fatalf("name = %q, want unlock", name)
	}
	if !reflect.DeepEqual(fields, in) {
		t.Fatalf("fields = %#v, want %#v", fields, in)
	}
}

func TestParseGroupSelectorRequiresAtPrefix(t *testing.T) {
	group, err := parseGroupSelector("@caddy")
	if err != nil {
		t.Fatalf("parseGroupSelector returned error: %v", err)
	}
	if group != "caddy" {
		t.Fatalf("group = %q, want caddy", group)
	}
	if _, err := parseGroupSelector("caddy"); err == nil {
		t.Fatal("parseGroupSelector accepted group without @")
	}
}

func TestHostGroupsAreListedSorted(t *testing.T) {
	cfg := &config.Config{
		HostDB:  t.TempDir() + "/hosts.json",
		Targets: map[string]config.Target{},
	}
	if err := cfg.UpsertTarget("one", config.Target{User: "u", Host: "one.example", Groups: []string{"kl"}}); err != nil {
		t.Fatal(err)
	}
	if err := cfg.UpsertTarget("two", config.Target{User: "u", Host: "two.example", Groups: []string{"caddy", "kl"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.AddHostGroup("one", "caddy"); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.RemoveHostGroup("two", "kl"); err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.GroupNames(), []string{"caddy", "kl"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("GroupNames = %#v, want %#v", got, want)
	}
	if got, want := cfg.HostsInGroup("caddy"), []string{"one", "two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("HostsInGroup = %#v, want %#v", got, want)
	}
	if got, want := cfg.HostsInGroup("kl"), []string{"one"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("HostsInGroup = %#v, want %#v", got, want)
	}
}

func TestBuiltinAllGroupListsEnabledHosts(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one":   {User: "u", Host: "one.example"},
			"two":   {User: "u", Host: "two.example"},
			"three": {User: "u", Host: "three.example", Disabled: true},
		},
	}
	if got, want := groupHosts(cfg, "all"), []string{"one", "two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("groupHosts(all) = %#v, want %#v", got, want)
	}
}
