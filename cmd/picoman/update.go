package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"

	"picoman/internal/config"
)

var versionRe = regexp.MustCompile(`(const version = ")(v?)(\d+)\.(\d+)\.(\d+)(")`)

func runUpdate() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot load config: %v\n", err)
		os.Exit(1)
	}

	srcDir := cfg.SourceDir
	if srcDir == "" {
		srcDir, err = os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot find source dir: %v\n", err)
			os.Exit(1)
		}
	}

	if err := bumpPatch(srcDir); err != nil {
		fmt.Fprintf(os.Stderr, "version bump failed: %v\n", err)
		os.Exit(1)
	}

	bin := filepath.Join(srcDir, "picoman")
	build := exec.Command("go", "build", "-o", bin, "./cmd/picoman")
	build.Dir = srcDir
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n", err)
		os.Exit(1)
	}

	install := exec.Command(bin, "install")
	install.Stdout = os.Stdout
	install.Stderr = os.Stderr
	if err := install.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "install failed: %v\n", err)
		os.Exit(1)
	}

	restart := exec.Command("systemctl", "--user", "restart", "picoman")
	restart.Stdout = os.Stdout
	restart.Stderr = os.Stderr
	if err := restart.Run(); err != nil {
		reset := exec.Command("systemctl", "--user", "reset-failed", "picoman")
		reset.Stdout = os.Stdout
		reset.Stderr = os.Stderr
		_ = reset.Run()

		start := exec.Command("systemctl", "--user", "start", "picoman")
		start.Stdout = os.Stdout
		start.Stderr = os.Stderr
		if startErr := start.Run(); startErr != nil {
			fmt.Fprintf(os.Stderr, "restart failed: %v\n", err)
			fmt.Fprintf(os.Stderr, "start after reset-failed failed: %v\n", startErr)
			os.Exit(1)
		}
	}
}

func bumpPatch(srcDir string) error {
	path := filepath.Join(srcDir, "cmd", "picoman", "main.go")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	m := versionRe.FindSubmatchIndex(data)
	if m == nil {
		return fmt.Errorf("version string not found in %s", path)
	}
	patch, _ := strconv.Atoi(string(data[m[10]:m[11]]))
	repl := fmt.Sprintf("%s%s%s.%s.%d%s",
		string(data[m[2]:m[3]]),
		string(data[m[4]:m[5]]),
		string(data[m[6]:m[7]]),
		string(data[m[8]:m[9]]),
		patch+1,
		string(data[m[12]:m[13]]),
	)

	out := make([]byte, 0, len(data)+4)
	out = append(out, data[:m[0]]...)
	out = append(out, repl...)
	out = append(out, data[m[1]:]...)

	fmt.Printf("version: %s.%s.%d -> %s.%s.%d\n",
		string(data[m[6]:m[7]]), string(data[m[8]:m[9]]), patch,
		string(data[m[6]:m[7]]), string(data[m[8]:m[9]]), patch+1)
	return os.WriteFile(path, out, 0o644)
}
