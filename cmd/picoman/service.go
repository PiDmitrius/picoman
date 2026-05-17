package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"picoman/internal/config"
)

func runServiceCtl(action string) {
	cmd := exec.Command("systemctl", "--user", action, "picoman")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

func runStatus() {
	cmd := exec.Command("systemctl", "--user", "status", "picoman", "--no-pager", "-l")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

func pidPath() string {
	return filepath.Join(config.DataDir(), "picoman.pid")
}

func writePID() {
	_ = os.MkdirAll(config.DataDir(), 0o700)
	_ = os.WriteFile(pidPath(), []byte(fmt.Sprintf("%d", os.Getpid())), 0o600)
}

func removePID() {
	_ = os.Remove(pidPath())
}

type restartMarker struct {
	Reason string `json:"reason"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
}

func markerPath() string {
	return filepath.Join(config.DataDir(), "restart.json")
}

func writeRestartMarker(reason, from, to string) error {
	if err := os.MkdirAll(config.DataDir(), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(restartMarker{Reason: reason, From: from, To: to})
	if err != nil {
		return err
	}
	return os.WriteFile(markerPath(), data, 0o600)
}

func readRestartMarker() restartMarker {
	data, err := os.ReadFile(markerPath())
	if err != nil {
		return restartMarker{}
	}
	_ = os.Remove(markerPath())
	var marker restartMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return restartMarker{}
	}
	return marker
}
