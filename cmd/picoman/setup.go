package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"picoman/internal/config"
)

func runSetup() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("picoman setup")
	fmt.Println("-------------")

	cfg, err := config.Load()
	if err != nil {
		if os.IsNotExist(err) {
			cfg = config.Default()
		} else {
			fmt.Fprintf(os.Stderr, "error: cannot load config: %v\n", err)
			os.Exit(1)
		}
	}

	cfg.TelegramToken = promptSecretKeep(reader, "Telegram bot token", cfg.TelegramToken)
	cfg.AllowedUsers = promptInt64ListKeep(reader, "Telegram allowed users", cfg.AllowedUsers)
	cfg.KeyPath = expandPathValue(promptStringKeep(reader, "SSH key path", displayPathValue(cfg.KeyPath)), "")
	cfg.KeyPassphrase = promptSecretKeep(reader, "SSH key passphrase", cfg.KeyPassphrase)
	cfg.AgentSocket = expandPathValue(promptStringKeep(reader, "SSH agent socket", displayPathValue(cfg.AgentSocket)), "")
	cfg.ControlSocket = expandPathValue(promptStringKeep(reader, "Control socket", displayPathValue(cfg.ControlSocket)), "")
	cfg.MaxUnlockTTL = promptDurationKeep(reader, "Max unlock TTL", cfg.MaxUnlockTTL)
	cfg.SourceDir = expandPathValue(promptStringKeep(reader, "Source directory", displayPathValue(defaultSourceDir(cfg.SourceDir))), "")

	if err := config.Save(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Saved to %s\n", filepath.Join(config.Dir(), "config.json"))
}

func maskToken(s string) string {
	if len(s) <= 8 {
		return "***"
	}
	return s[:4] + "***" + s[len(s)-4:]
}

func promptSecretKeep(reader *bufio.Reader, label, current string) string {
	hint := "enter=keep empty"
	if current != "" {
		hint = "enter=keep " + maskToken(current)
	}
	fmt.Printf("%s [%s, -=clear]: ", label, hint)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	switch line {
	case "":
		return current
	case "-":
		return ""
	default:
		return line
	}
}

func promptStringKeep(reader *bufio.Reader, label, current string) string {
	hint := "enter=keep empty"
	if current != "" {
		hint = "enter=keep " + current
	}
	fmt.Printf("%s [%s, -=clear]: ", label, hint)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	switch line {
	case "":
		return current
	case "-":
		return ""
	default:
		return line
	}
}

func promptDurationKeep(reader *bufio.Reader, label, current string) string {
	for {
		line := promptStringKeep(reader, label, current)
		if line == "" {
			return line
		}
		d, err := time.ParseDuration(line)
		if err != nil || d <= 0 {
			fmt.Println("Invalid duration, use values like 5m or 15m")
			continue
		}
		return line
	}
}

func promptInt64ListKeep(reader *bufio.Reader, label string, current []int64) []int64 {
	hint := "enter=keep empty"
	if len(current) > 0 {
		hint = "enter=keep " + formatInt64List(current)
	}
	for {
		fmt.Printf("%s [%s, -=clear, comma-separated]: ", label, hint)
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			return current
		}
		if line == "-" {
			return []int64{}
		}
		values, err := parseInt64List(line)
		if err != nil {
			fmt.Printf("Invalid list: %v\n", err)
			continue
		}
		return values
	}
}

func displayPathValue(path string) string {
	if path == "" {
		return ""
	}
	return tildePath(path)
}

func expandPathValue(path, fallback string) string {
	home, _ := os.UserHomeDir()
	switch {
	case path == "":
		return fallback
	case path == "~":
		return home
	case strings.HasPrefix(path, "~/"):
		return filepath.Join(home, path[2:])
	default:
		return path
	}
}

func defaultSourceDir(current string) string {
	if current != "" {
		return current
	}
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return cwd
}

func formatInt64List(values []int64) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, strconv.FormatInt(v, 10))
	}
	return strings.Join(parts, ",")
}

func parseInt64List(line string) ([]int64, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return []int64{}, nil
	}
	parts := strings.Split(line, ",")
	values := make([]int64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}
