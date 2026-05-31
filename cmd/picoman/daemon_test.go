package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"picoman/internal/agent"
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

func TestNormalizeCommandFieldsSplitsUnlockMax(t *testing.T) {
	name, fields := normalizeCommandFields([]string{"/unlock_max"})
	if name != "unlock" {
		t.Fatalf("name = %q, want unlock", name)
	}
	want := []string{"/unlock", "max"}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields = %#v, want %#v", fields, want)
	}
}

func TestHandleUnlockMaxUsesMaxTTL(t *testing.T) {
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Hour)
	if _, err := handleUnlock([]string{"/unlock", "max"}, st, 2*time.Hour); err == nil {
		t.Fatal("handleUnlock succeeded with locked key")
	} else if strings.Contains(err.Error(), "bad ttl") {
		t.Fatalf("max argument was parsed as duration: %v", err)
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

func TestLogCommandNameDoesNotIncludeArguments(t *testing.T) {
	if got, want := logCommandName("/run host secret command"), "run"; got != want {
		t.Fatalf("logCommandName = %q, want %q", got, want)
	}
	if got, want := logCommandName("/unlock_1h"), "unlock"; got != want {
		t.Fatalf("logCommandName = %q, want %q", got, want)
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

func TestChatLogLevelKeepsRunErrorMinimal(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Minute)
	reply, err := cmdRun(cmdCtx{cfg: cfg, st: st, audit: newAuditState("chat")}, []string{"run", "host", "secret", "command"})
	if err == nil {
		t.Fatal("cmdRun succeeded with locked key")
	}
	if got, want := reply.text, actionText("▶️ run", "host"); got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
	if containsAny(reply.text, "secret", "command", "key is locked") {
		t.Fatalf("chat reply leaked details: %q", reply.text)
	}
}

func TestStartTextFollowsLogLevel(t *testing.T) {
	chat := commandStartText(newAuditState("chat"), "run", []string{"run", "host", "secret", "command"})
	if got, want := chat, actionStartText("▶️ run", "host"); got != want {
		t.Fatalf("chat start = %q, want %q", got, want)
	}
	if containsAny(chat, "secret", "command") {
		t.Fatalf("chat start leaked command: %q", chat)
	}

	all := commandStartText(newAuditState("all"), "run", []string{"run", "host", "secret", "command"})
	if !strings.Contains(all, "secret command") {
		t.Fatalf("all start did not include command: %q", all)
	}
	if strings.Contains(all, "(waiting...)") {
		t.Fatalf("all start has redundant waiting marker: %q", all)
	}
}

func TestAllLogLevelKeepsRunErrorDetails(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Minute)
	reply, err := cmdRun(cmdCtx{cfg: cfg, st: st, audit: newAuditState("all")}, []string{"run", "host", "secret", "command"})
	if err == nil {
		t.Fatal("cmdRun succeeded with locked key")
	}
	if !containsAny(reply.text, "secret command", "key is locked") {
		t.Fatalf("all reply did not include details: %q", reply.text)
	}
}

func TestChatLogLevelKeepsTransferErrorsMinimal(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Minute)
	for _, tt := range []struct {
		name   string
		fn     func(cmdCtx, []string) (cmdReply, error)
		fields []string
		want   string
	}{
		{name: "get", fn: cmdGet, fields: []string{"get", "host", "secret-remote", "secret-local"}, want: actionText("⬅️ get", "host")},
		{name: "put", fn: cmdPut, fields: []string{"put", "host", "secret-local", "secret-remote"}, want: actionText("➡️ put", "host")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reply, err := tt.fn(cmdCtx{cfg: cfg, st: st, audit: newAuditState("chat")}, tt.fields)
			if err == nil {
				t.Fatal("command succeeded with locked key")
			}
			if reply.text != tt.want {
				t.Fatalf("reply = %q, want %q", reply.text, tt.want)
			}
			if containsAny(reply.text, "secret-local", "secret-remote", "key is locked") {
				t.Fatalf("chat reply leaked details: %q", reply.text)
			}
		})
	}
}

func TestPutCommandAcceptsGroupSelector(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example", Groups: []string{"web"}},
		},
	}
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Minute)
	reply, err := cmdPut(cmdCtx{cfg: cfg, st: st, audit: newAuditState("chat")}, []string{"put", "@web", "local", "remote"})
	if err == nil {
		t.Fatal("command succeeded with locked key")
	}
	if strings.Contains(err.Error(), "unknown target") {
		t.Fatalf("group selector was treated as host: %v", err)
	}
	if got, want := reply.text, actionText("➡️ put", "@web"); got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

func TestGetCommandRejectsGroupSelector(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example", Groups: []string{"web"}},
		},
	}
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Minute)
	if _, err := cmdGet(cmdCtx{cfg: cfg, st: st, audit: newAuditState("all")}, []string{"get", "@web", "remote", "local"}); err == nil {
		t.Fatal("cmdGet accepted group selector")
	} else if !strings.Contains(err.Error(), "get on group is not supported") {
		t.Fatalf("cmdGet error = %v, want unsupported group get", err)
	}
}

func TestPutCommandRejectsEmptyGroupSelector(t *testing.T) {
	cfg := &config.Config{Targets: map[string]config.Target{}}
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Minute)
	if _, err := cmdPut(cmdCtx{cfg: cfg, st: st, audit: newAuditState("all")}, []string{"put", "@empty", "local", "remote"}); err == nil {
		t.Fatal("cmdPut accepted empty group")
	} else if !strings.Contains(err.Error(), `group "@empty" is empty`) {
		t.Fatalf("cmdPut error = %v, want empty group", err)
	}
}

func TestDeveloperAutoUnlockTTLRequiresDeveloperDir(t *testing.T) {
	cfg := &config.Config{MaxUnlockTTL: "2h"}
	if _, ok := developerAutoUnlockTTL(cfg); ok {
		t.Fatal("developerAutoUnlockTTL enabled without developer_dir")
	}
	cfg.DeveloperDir = "/tmp/picoman"
	ttl, ok := developerAutoUnlockTTL(cfg)
	if !ok {
		t.Fatal("developerAutoUnlockTTL disabled with developer_dir")
	}
	if ttl != 2*time.Hour {
		t.Fatalf("ttl = %s, want 2h", ttl)
	}
}

func TestDeveloperAutoUnlockTTLUsesNormalizedMaxTTL(t *testing.T) {
	cfg := &config.Config{DeveloperDir: "/tmp/picoman", MaxUnlockTTL: "bad"}
	ttl, ok := developerAutoUnlockTTL(cfg)
	if !ok {
		t.Fatal("developerAutoUnlockTTL disabled with developer_dir")
	}
	if ttl != 15*time.Minute {
		t.Fatalf("ttl = %s, want default 15m", ttl)
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

func TestCmdHostInfoCommand(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example"},
		},
	}
	reply, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "info", "host"})
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
		t.Fatal("cmdHost accepted object without info command")
	}
	if _, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "host", "remove"}); err == nil {
		t.Fatal("cmdHost accepted object action")
	}
	if _, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "show", "host"}); err == nil {
		t.Fatal("cmdHost accepted old show command")
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

func TestCmdGroupInfoCommand(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "user", Host: "host.example", Groups: []string{"web"}},
		},
	}
	reply, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "info", "@web"})
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
		t.Fatal("cmdGroup accepted object without info command")
	}
	if _, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "@web", "add", "host"}); err == nil {
		t.Fatal("cmdGroup accepted object action")
	}
	if _, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "show", "@web"}); err == nil {
		t.Fatal("cmdGroup accepted old show command")
	}
}

func containsAny(s string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}
