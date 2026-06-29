package main

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
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

func TestTargetExpressionsResolveSets(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"alpha":   {User: "u", Host: "alpha.example", Groups: []string{"a"}},
			"beta":    {User: "u", Host: "beta.example", Groups: []string{"a", "b"}},
			"delta":   {User: "u", Host: "delta.example", Disabled: true},
			"gamma":   {User: "u", Host: "gamma.example", Groups: []string{"b"}},
			"standby": {User: "u", Host: "standby.example"},
		},
	}
	for _, tt := range []struct {
		expr string
		want []string
	}{
		{expr: "@a,@b", want: []string{"alpha", "beta", "gamma"}},
		{expr: "@a^@b", want: []string{"alpha"}},
		{expr: "@a+@b", want: []string{"beta"}},
		{expr: "standby,@b^gamma", want: []string{"beta", "standby"}},
		{expr: "@all^@a", want: []string{"gamma", "standby"}},
	} {
		t.Run(tt.expr, func(t *testing.T) {
			got, err := hostsForTargetExpr(cfg, tt.expr)
			if err != nil {
				t.Fatalf("hostsForTargetExpr returned error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("hostsForTargetExpr = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestTargetExpressionsRejectBadInput(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"host": {User: "u", Host: "host.example", Groups: []string{"web"}},
		},
	}
	for _, expr := range []string{",@web", "@web+", "host^"} {
		t.Run(expr, func(t *testing.T) {
			if _, err := hostsForTargetExpr(cfg, expr); err == nil {
				t.Fatal("hostsForTargetExpr accepted bad expression")
			}
		})
	}
	if _, err := hostsForTargetExpr(cfg, "missing,@web"); err == nil {
		t.Fatal("hostsForTargetExpr accepted unknown host")
	} else if !strings.Contains(err.Error(), `unknown target "missing"`) {
		t.Fatalf("error = %v, want unknown target", err)
	}
	if _, err := hostsForTargetExpr(cfg, "@all^@missing"); err == nil {
		t.Fatal("hostsForTargetExpr accepted missing group in expression")
	} else if !strings.Contains(err.Error(), `group "@missing" is empty`) {
		t.Fatalf("error = %v, want empty group", err)
	}
}

func TestRunGroupStartsHostsInParallelAndReportsInOrder(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one": {User: "user", Host: "one.example", Groups: []string{"web"}},
			"two": {User: "user", Host: "two.example", Groups: []string{"web"}},
		},
	}
	started := make(chan string, 2)
	release := make(chan struct{})
	done := make(chan struct {
		output string
		code   int
		err    error
	}, 1)

	go func() {
		output, code, err := runTargetSelectorWithRunner(context.Background(), cfg, "@web", "uptime", func() bool { return true }, func(_ context.Context, host, _ string) (string, int, error) {
			started <- host
			<-release
			return host + " output", 0, nil
		})
		done <- struct {
			output string
			code   int
			err    error
		}{output: output, code: code, err: err}
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			close(release)
			t.Fatal("group run did not start all hosts before waiting for completion")
		}
	}
	close(release)

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("runTargetSelectorWithRunner returned error: %v", got.err)
		}
		if got.code != 0 {
			t.Fatalf("exit code = %d, want 0", got.code)
		}
		want := "== one ==\none output\n\n\n== two ==\ntwo output"
		if got.output != want {
			t.Fatalf("output = %q, want %q", got.output, want)
		}
	case <-time.After(time.Second):
		t.Fatal("group run did not finish after releasing hosts")
	}
}

func TestRunTargetExpressionCanStartWithHost(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one": {User: "user", Host: "one.example"},
			"two": {User: "user", Host: "two.example", Groups: []string{"web"}},
		},
	}
	var ran []string
	var ranMu sync.Mutex
	output, code, err := runTargetSelectorWithRunner(context.Background(), cfg, "one,@web", "uptime", func() bool { return true }, func(_ context.Context, host, _ string) (string, int, error) {
		ranMu.Lock()
		ran = append(ran, host)
		ranMu.Unlock()
		return host + " output", 0, nil
	})
	if err != nil {
		t.Fatalf("runTargetSelectorWithRunner returned error: %v", err)
	}
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	sort.Strings(ran)
	if got, want := ran, []string{"one", "two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ran hosts = %#v, want %#v", got, want)
	}
	if want := "== one ==\none output\n\n\n== two ==\ntwo output"; output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestRunGroupLimitsParallelHosts(t *testing.T) {
	targets := map[string]config.Target{}
	for i := 0; i < maxParallelGroupRuns+1; i++ {
		name := "host" + string(rune('a'+i))
		targets[name] = config.Target{User: "user", Host: name + ".example", Groups: []string{"web"}}
	}
	cfg := &config.Config{Targets: targets}
	started := make(chan struct{}, maxParallelGroupRuns+1)
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		_, _, err := runTargetSelectorWithRunner(context.Background(), cfg, "@web", "uptime", func() bool { return true }, func(context.Context, string, string) (string, int, error) {
			started <- struct{}{}
			<-release
			return "", 0, nil
		})
		done <- err
	}()

	for i := 0; i < maxParallelGroupRuns; i++ {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			close(release)
			t.Fatalf("only %d hosts started, want concurrency cap %d", i, maxParallelGroupRuns)
		}
	}
	select {
	case <-started:
		close(release)
		t.Fatalf("more than %d hosts started concurrently", maxParallelGroupRuns)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runTargetSelectorWithRunner returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("group run did not finish after releasing hosts")
	}
}

func TestRunGroupPreservesExitCodePrecedence(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one": {User: "user", Host: "one.example", Groups: []string{"web"}},
			"two": {User: "user", Host: "two.example", Groups: []string{"web"}},
		},
	}
	output, code, err := runTargetSelectorWithRunner(context.Background(), cfg, "@web", "uptime", func() bool { return true }, func(_ context.Context, host, _ string) (string, int, error) {
		if host == "one" {
			return "one failed", 3, nil
		}
		return "two failed", 5, nil
	})
	if err != nil {
		t.Fatalf("runTargetSelectorWithRunner returned error: %v", err)
	}
	if code != 3 {
		t.Fatalf("exit code = %d, want first host code 3", code)
	}
	if !strings.Contains(output, "exit status 3") || !strings.Contains(output, "exit status 5") {
		t.Fatalf("output did not include both exit statuses: %q", output)
	}
}

func TestRunGroupPreservesMixedErrorAndExitPrecedence(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one": {User: "user", Host: "one.example", Groups: []string{"web"}},
			"two": {User: "user", Host: "two.example", Groups: []string{"web"}},
		},
	}
	for _, tt := range []struct {
		name     string
		oneErr   error
		oneCode  int
		twoErr   error
		twoCode  int
		wantCode int
	}{
		{name: "exit first", oneCode: 2, twoErr: errors.New("transport failed"), wantCode: 2},
		{name: "error first", oneErr: errors.New("transport failed"), twoCode: 2, wantCode: 255},
	} {
		t.Run(tt.name, func(t *testing.T) {
			output, code, err := runTargetSelectorWithRunner(context.Background(), cfg, "@web", "uptime", func() bool { return true }, func(_ context.Context, host, _ string) (string, int, error) {
				if host == "one" {
					return "", tt.oneCode, tt.oneErr
				}
				return "", tt.twoCode, tt.twoErr
			})
			if err != nil {
				t.Fatalf("runTargetSelectorWithRunner returned error: %v", err)
			}
			if code != tt.wantCode {
				t.Fatalf("exit code = %d, want %d", code, tt.wantCode)
			}
			if !strings.Contains(output, "exit status 2") || !strings.Contains(output, "transport failed") {
				t.Fatalf("output did not include mixed failure details: %q", output)
			}
		})
	}
}

func TestRunGroupRejectsEmptyGroupAndLockedKey(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one": {User: "user", Host: "one.example", Groups: []string{"web"}},
		},
	}
	if _, _, err := runTargetSelectorWithRunner(context.Background(), cfg, "@empty", "uptime", func() bool { return true }, nil); err == nil {
		t.Fatal("empty group succeeded")
	} else if !strings.Contains(err.Error(), `group "@empty" is empty`) {
		t.Fatalf("empty group error = %v", err)
	}
	if _, _, err := runTargetSelectorWithRunner(context.Background(), cfg, "@web", "uptime", func() bool { return false }, nil); err == nil {
		t.Fatal("locked group run succeeded")
	} else if err.Error() != "key is locked" {
		t.Fatalf("locked group error = %v", err)
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

func TestPutCommandAcceptsTargetExpression(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one": {User: "user", Host: "one.example"},
			"two": {User: "user", Host: "two.example", Groups: []string{"web"}},
		},
	}
	st := agent.New(t.TempDir()+"/agent.sock", t.TempDir()+"/key", time.Minute)
	reply, err := cmdPut(cmdCtx{cfg: cfg, st: st, audit: newAuditState("chat")}, []string{"put", "one,@web", "local", "remote"})
	if err == nil {
		t.Fatal("command succeeded with locked key")
	}
	if strings.Contains(err.Error(), "unknown target") {
		t.Fatalf("target expression was treated as host: %v", err)
	}
	if got, want := reply.text, actionText("➡️ put", "one,@web"); got != want {
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

func TestRemoteWorkPathUsesHostOverride(t *testing.T) {
	cfg := &config.Config{RemoteWorkDir: "~/global"}
	target := config.Target{RemoteWorkDir: "~/host"}
	got, err := remoteWorkPath(cfg, target, "dir/file")
	if err != nil {
		t.Fatalf("remoteWorkPath returned error: %v", err)
	}
	if got != "~/host/dir/file" {
		t.Fatalf("remoteWorkPath = %q, want host override", got)
	}

	target = config.Target{}
	got, err = remoteWorkPath(cfg, target, "file")
	if err != nil {
		t.Fatalf("remoteWorkPath returned error: %v", err)
	}
	if got != "~/global/file" {
		t.Fatalf("remoteWorkPath = %q, want global remote work dir", got)
	}

	cfg = &config.Config{}
	got, err = remoteWorkPath(cfg, target, "file")
	if err != nil {
		t.Fatalf("remoteWorkPath returned error: %v", err)
	}
	if got != "~/picoman/file" {
		t.Fatalf("remoteWorkPath = %q, want default remote work dir", got)
	}
}

func TestRemoteWorkPathRejectsEscape(t *testing.T) {
	cfg := &config.Config{RemoteWorkDir: "~/global"}
	for _, name := range []string{"../file", "dir/../../file", "/abs/file"} {
		t.Run(name, func(t *testing.T) {
			if _, err := remoteWorkPath(cfg, config.Target{}, name); err == nil {
				t.Fatal("remoteWorkPath accepted escaping path")
			}
		})
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
			"host": {User: "user", Host: "host.example", RemoteWorkDir: "~/deploy"},
		},
	}
	reply, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "info", "host"})
	if err != nil {
		t.Fatalf("cmdHost returned error: %v", err)
	}
	if reply.text == "" {
		t.Fatal("cmdHost returned empty reply")
	}
	if !strings.Contains(reply.text, "remote_work_dir: ~/deploy") {
		t.Fatalf("host info did not include remote_work_dir: %q", reply.text)
	}
}

func TestCmdHostSetRemoteWorkDir(t *testing.T) {
	cfg := &config.Config{
		HostDB:  t.TempDir() + "/hosts.json",
		Targets: map[string]config.Target{},
	}
	if err := cfg.UpsertTarget("host", config.Target{User: "user", Host: "host.example"}); err != nil {
		t.Fatal(err)
	}
	reply, err := cmdHost(cmdCtx{cfg: cfg}, []string{"host", "set", "host", "remote_work_dir", "~/deploy"})
	if err != nil {
		t.Fatalf("cmdHost returned error: %v", err)
	}
	if !strings.Contains(reply.text, "~/deploy") {
		t.Fatalf("reply = %q, want remote work dir", reply.text)
	}
	target, _ := cfg.Target("host")
	if target.RemoteWorkDir != "~/deploy" {
		t.Fatalf("RemoteWorkDir = %q, want ~/deploy", target.RemoteWorkDir)
	}

	reply, err = cmdHost(cmdCtx{cfg: cfg}, []string{"host", "set", "host", "remote_work_dir"})
	if err != nil {
		t.Fatalf("cmdHost clear returned error: %v", err)
	}
	if !strings.Contains(reply.text, "cleared") {
		t.Fatalf("reply = %q, want cleared", reply.text)
	}
	target, _ = cfg.Target("host")
	if target.RemoteWorkDir != "" {
		t.Fatalf("RemoteWorkDir = %q, want empty", target.RemoteWorkDir)
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

func TestCmdGroupInfoAcceptsTargetExpression(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one":   {User: "user", Host: "one.example", Groups: []string{"web"}},
			"two":   {User: "user", Host: "two.example", Groups: []string{"web", "prod"}},
			"three": {User: "user", Host: "three.example", Groups: []string{"prod"}},
		},
	}
	reply, err := cmdGroup(cmdCtx{cfg: cfg}, []string{"group", "info", "one,@prod^three"})
	if err != nil {
		t.Fatalf("cmdGroup returned error: %v", err)
	}
	if !strings.Contains(reply.text, "group expression one,@prod^three") {
		t.Fatalf("reply = %q, want expression header", reply.text)
	}
	if !containsAny(reply.text, "<b>one</b>") || !containsAny(reply.text, "<b>two</b>") {
		t.Fatalf("reply = %q, want resolved hosts", reply.text)
	}
	if strings.Contains(reply.text, "<b>three</b>") {
		t.Fatalf("reply = %q, excluded host present", reply.text)
	}

	plain, err := groupInfoText(cfg, "@web+@prod", false)
	if err != nil {
		t.Fatalf("groupInfoText returned error: %v", err)
	}
	if plain != "group expression @web+@prod\n- two" {
		t.Fatalf("plain group info = %q, want resolved expression", plain)
	}
}

func TestGroupInfoExpressionShowsEmptyIntersection(t *testing.T) {
	cfg := &config.Config{
		Targets: map[string]config.Target{
			"one": {User: "user", Host: "one.example", Groups: []string{"web"}},
			"two": {User: "user", Host: "two.example", Groups: []string{"prod"}},
		},
	}
	text, err := groupInfoText(cfg, "@web+@prod", false)
	if err != nil {
		t.Fatalf("groupInfoText returned error: %v", err)
	}
	if text != "group expression @web+@prod empty" {
		t.Fatalf("group info = %q, want empty expression", text)
	}
	if _, err := groupInfoText(cfg, "@web^@missing", false); err == nil {
		t.Fatal("groupInfoText accepted missing group in expression")
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
