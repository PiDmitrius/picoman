package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"slices"
	"sort"
	"sync"
	"time"

	"picoman/internal/config"
	"picoman/internal/outbox"
	"picoman/internal/transport"
)

// In digest level, successful control-socket run/get/put are counted per
// action and target and reported at the top of each hour, on loglevel change,
// before a lock or seal notice, on shutdown and on demand; failures are
// reported immediately. A digest covers the time since the previous one and a
// call belongs to the digest in which it completes; an hour without calls
// sends nothing and keeps the interval open, an explicit request answers "no
// calls". Counts are mirrored to the store on every change and cleared only
// after the digest is queued for at least one recipient, so a crash or an
// unavailable transport repeats or delays a digest instead of losing it;
// counts left by a previous process are reported at startup. A call is
// counted if it starts in digest level; if its count cannot be stored it is
// reported immediately instead.
type auditState struct {
	mu       sync.Mutex
	logLevel string
	counts   map[digestKey]int
	since    time.Time
	save     func(string) error
}

type savedDigest struct {
	Since  int64        `json:"since"`
	Counts []savedCount `json:"counts"`
}

type savedCount struct {
	Action string `json:"action"`
	Target string `json:"target"`
	N      int    `json:"n"`
}

var digestActions = []string{"▶️ run", "⬅️ get", "➡️ put"}

type digestKey struct {
	action, target string
}

func newAuditState(level string) *auditState {
	if !config.ValidLogLevel(level) {
		level = "chat"
	}
	return &auditState{logLevel: level, counts: map[digestKey]int{}, since: time.Now()}
}

func (a *auditState) LogLevel() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.logLevel
}

func (a *auditState) SetLogLevel(level string) bool {
	if !config.ValidLogLevel(level) {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.logLevel = level
	return true
}

func (a *auditState) digest() bool {
	return a != nil && a.LogLevel() == "digest"
}

func (a *auditState) attachStore(saved string, save func(string) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var d savedDigest
	if json.Unmarshal([]byte(saved), &d) == nil && d.Since > 0 {
		a.since = time.Unix(d.Since, 0)
		for _, c := range d.Counts {
			a.counts[digestKey{c.Action, c.Target}] += c.N
		}
	}
	a.save = save
	_ = a.persistLocked()
}

func (a *auditState) persistLocked() error {
	if a.save == nil {
		return nil
	}
	d := savedDigest{Since: a.since.Unix(), Counts: []savedCount{}}
	for k, n := range a.counts {
		d.Counts = append(d.Counts, savedCount{k.action, k.target, n})
	}
	data, _ := json.Marshal(d)
	err := a.save(string(data))
	if err != nil {
		log.Printf("save digest: %v", err)
	}
	return err
}

func (a *auditState) count(action, target string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	k := digestKey{action, target}
	a.counts[k]++
	if a.persistLocked() == nil {
		return true
	}
	if a.counts[k]--; a.counts[k] == 0 {
		delete(a.counts, k)
	}
	return false
}

func (a *auditState) sendDigest(hub *transportHub, origin *transport.Message, end time.Time, force bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.counts) == 0 && !force {
		return
	}
	for _, text := range a.digestTextLocked(end) {
		if !hub.notify(true, text, origin) {
			return
		}
	}
	a.counts, a.since = map[digestKey]int{}, end
	_ = a.persistLocked()
}

func (a *auditState) digestTextLocked(end time.Time) []string {
	keys := make([]digestKey, 0, len(a.counts))
	for k := range a.counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].target != keys[j].target {
			return keys[i].target < keys[j].target
		}
		return slices.Index(digestActions, keys[i].action) < slices.Index(digestActions, keys[j].action)
	})
	header := fmt.Sprintf("📊 %s–%s", a.since.Format("15:04"), end.Format("15:04"))
	if len(keys) == 0 {
		return []string{header + "\nno calls"}
	}
	var texts []string
	text := header
	for _, k := range keys {
		line := fmt.Sprintf("\n%s <b>%s</b> ×%d", html.EscapeString(k.action), escapedCodeBlock(k.target, maxPathBytes), a.counts[k])
		if text != header && len(text)+len(line) > outbox.MaxMessageBytes {
			texts = append(texts, text)
			text = header
		}
		text += line
	}
	return append(texts, text)
}

func notifyLockState(hub *transportHub, audit *auditState, text string, origin *transport.Message) {
	audit.sendDigest(hub, origin, time.Now(), false)
	hub.notify(false, text, origin)
}

func runDigest(ctx context.Context, hub *transportHub, audit *auditState) {
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		audit.sendDigest(hub, nil, next, false)
	}
}
