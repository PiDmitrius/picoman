package main

import "sync"

type auditState struct {
	mu       sync.Mutex
	logLevel string
}

func newAuditState(level string) *auditState {
	if level != "all" {
		level = "chat"
	}
	return &auditState{logLevel: level}
}

func (a *auditState) LogLevel() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.logLevel
}

func (a *auditState) SetLogLevel(level string) bool {
	if level != "chat" && level != "all" {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.logLevel = level
	return true
}
