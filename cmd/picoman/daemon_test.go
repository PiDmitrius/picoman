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

func TestCmdLogLevelUpdatesAuditState(t *testing.T) {
	audit := newAuditState("chat")
	reply, err := cmdLogLevel(cmdCtx{audit: audit}, []string{"/loglevel", "all"})
	if err != nil {
		t.Fatalf("cmdLogLevel returned error: %v", err)
	}
	if reply.text != "⚙️ loglevel all" {
		t.Fatalf("reply = %q, want loglevel confirmation", reply.text)
	}
	if got := audit.LogLevel(); got != "all" {
		t.Fatalf("LogLevel = %q, want all", got)
	}
}

func TestPluralListCommandsRejectObjectActions(t *testing.T) {
	cfg := &config.Config{Targets: map[string]config.Target{}}
	if _, err := cmdHosts(cmdCtx{cfg: cfg}, []string{"hosts", "rm", "host"}); err == nil {
		t.Fatal("cmdHosts accepted object action")
	}
	if _, err := cmdGroupList(cmdCtx{cfg: cfg}, []string{"groups", "@all"}); err == nil {
		t.Fatal("cmdGroupList accepted object selector")
	}
}

func TestCmdHostRemoveCommand(t *testing.T) {
	t.Setenv("PICOMAN_DATA_DIR", t.TempDir())
	cfg := &config.Config{
		HostDB:  t.TempDir() + "/hosts.json",
		Targets: map[string]config.Target{},
	}
	if err := cfg.UpsertTarget("host", config.Target{User: "user", Host: "host.example"}); err != nil {
		t.Fatal(err)
	}
	reply, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "remove", "host"})
	if err != nil {
		t.Fatalf("cmdHost returned error: %v", err)
	}
	if reply.text == "" {
		t.Fatal("cmdHost returned empty reply")
	}
	if _, ok := cfg.Target("host"); ok {
		t.Fatal("host was not removed")
	}
}

func TestCmdHostShowCommand(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	reply, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "show", "host"})
	if err != nil {
		t.Fatalf("cmdHost returned error: %v", err)
	}
	if reply.text == "" {
		t.Fatal("cmdHost returned empty reply")
	}
}

func TestCmdHostRejectsMissingOrObjectCommand(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	if _, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host"}); err == nil {
		t.Fatal("cmdHost accepted missing command")
	}
	if _, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "host"}); err == nil {
		t.Fatal("cmdHost accepted object without show command")
	}
	if _, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "host", "remove"}); err == nil {
		t.Fatal("cmdHost accepted object action")
	}
}

func TestCmdGroupCommands(t *testing.T) {
	cfg := &config.Config{
		HostDB: t.TempDir() + "/hosts.json",
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	if _, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "add", "@web", "host"}); err != nil {
		t.Fatalf("cmdGroup add returned error: %v", err)
	}
	if got, want := cfg.HostsInGroup("web"), []string{"host"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("HostsInGroup(web) = %#v, want %#v", got, want)
	}
	if _, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "remove", "@web", "host"}); err != nil {
		t.Fatalf("cmdGroup remove returned error: %v", err)
	}
	if got := cfg.HostsInGroup("web"); len(got) != 0 {
		t.Fatalf("HostsInGroup(web) = %#v, want empty", got)
	}
}

func TestCmdGroupShowCommand(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example", Groups: []string{"web"}},
		},
	}
	reply, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "show", "@web"})
	if err != nil {
		t.Fatalf("cmdGroup returned error: %v", err)
	}
	if reply.text == "" {
		t.Fatal("cmdGroup returned empty reply")
	}
}

func TestCmdGroupRejectsObjectAction(t *testing.T) {
	cfg := &config.Config{
		HostDB: t.TempDir() + "/hosts.json",
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	if _, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group"}); err == nil {
		t.Fatal("cmdGroup accepted missing command")
	}
	if _, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "@web"}); err == nil {
		t.Fatal("cmdGroup accepted object without show command")
	}
	if _, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "@web", "add", "host"}); err == nil {
		t.Fatal("cmdGroup accepted object action")
	}
}
