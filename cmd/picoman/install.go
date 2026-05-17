package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func runInstall() {
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot find executable: %v\n", err)
		os.Exit(1)
	}

	home, _ := os.UserHomeDir()
	binDir := filepath.Join(home, ".local", "bin")
	_ = os.MkdirAll(binDir, 0o755)
	dst := filepath.Join(binDir, "picoman")
	if err := copyFile(exe, dst, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "cannot install: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("installed: %s\n", tildePath(dst))

	unitDir := filepath.Join(home, ".config", "systemd", "user")
	_ = os.MkdirAll(unitDir, 0o755)
	unitPath := filepath.Join(unitDir, "picoman.service")
	unit := renderServiceUnit(dst)
	if err := os.WriteFile(unitPath, []byte(unit), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "cannot install service unit: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("installed: %s\n", tildePath(unitPath))

	_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
	_ = exec.Command("systemctl", "--user", "enable", "picoman").Run()

	out, _ := exec.Command("systemctl", "--user", "is-active", "picoman").Output()
	if strings.TrimSpace(string(out)) == "active" {
		fmt.Println("\nInstalled. Service is running.")
	} else {
		fmt.Println("\nInstalled. Run: picoman start")
	}
}

func runUninstall() {
	_ = exec.Command("systemctl", "--user", "stop", "picoman").Run()
	_ = exec.Command("systemctl", "--user", "disable", "picoman").Run()
	home, _ := os.UserHomeDir()
	_ = os.Remove(filepath.Join(home, ".config", "systemd", "user", "picoman.service"))
	_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
	fmt.Println("uninstalled")
}

func copyFile(src, dst string, mode os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	_ = os.Remove(dst)
	return os.WriteFile(dst, data, mode)
}

func renderServiceUnit(binPath string) string {
	return fmt.Sprintf(`[Unit]
Description=picoman — Telegram-controlled SSH key opener
After=network.target
StartLimitBurst=3
StartLimitIntervalSec=60

[Service]
Type=simple
ExecStart=%s start
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
RestrictSUIDSGID=true
LockPersonality=true

[Install]
WantedBy=default.target
`, binPath)
}
