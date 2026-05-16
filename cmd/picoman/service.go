package main

import (
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
