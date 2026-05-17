package main

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"picoman/internal/config"
	"picoman/internal/outbox"
	"picoman/internal/tg"
)

const repo = "PiDmitrius/picoman"

var versionRe = regexp.MustCompile(`(const version = ")(v?)(\d+)\.(\d+)\.(\d+)(")`)

func runUpdate() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot load config: %v\n", err)
		os.Exit(1)
	}

	if cfg.SourceDir == "" {
		tag, err := latestTag()
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot get latest version: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("latest: %s (current: %s)\n", tag, version)
		if err := installRelease(tag); err != nil {
			fmt.Fprintf(os.Stderr, "update failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	next, err := bumpPatch(cfg.SourceDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "version bump failed: %v\n", err)
		os.Exit(1)
	}

	bin := filepath.Join(cfg.SourceDir, "picoman")
	build := exec.Command("go", "build", "-o", bin, "./cmd/picoman")
	build.Dir = cfg.SourceDir
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n", err)
		os.Exit(1)
	}

	if err := installBinary(bin, version, next); err != nil {
		fmt.Fprintf(os.Stderr, "install failed: %v\n", err)
		os.Exit(1)
	}
}

func runFallback(args []string) {
	if len(args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: picoman fallback [tag]")
		os.Exit(1)
	}
	if len(args) == 0 {
		text, err := updateText()
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot list releases: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(stripHTML(text))
		return
	}
	tag := args[0]
	if !strings.HasPrefix(tag, "v") {
		tag = "v" + tag
	}
	fmt.Printf("installing %s (current: %s)\n", tag, version)
	if err := installRelease(tag); err != nil {
		fmt.Fprintf(os.Stderr, "fallback failed: %v\n", err)
		os.Exit(1)
	}
}

func bumpPatch(srcDir string) (string, error) {
	path := filepath.Join(srcDir, "cmd", "picoman", "main.go")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	m := versionRe.FindSubmatchIndex(data)
	if m == nil {
		return "", fmt.Errorf("version string not found in %s", path)
	}
	patch, _ := strconv.Atoi(string(data[m[10]:m[11]]))
	next := fmt.Sprintf("v%s.%s.%d",
		string(data[m[6]:m[7]]),
		string(data[m[8]:m[9]]),
		patch+1,
	)
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

	fmt.Printf("version: v%s.%s.%d -> %s\n",
		string(data[m[6]:m[7]]), string(data[m[8]:m[9]]), patch, next)
	return next, os.WriteFile(path, out, 0o644)
}

func latestTag() (string, error) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Head("https://github.com/" + repo + "/releases/latest")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", fmt.Errorf("no releases found")
	}
	parts := strings.Split(loc, "/")
	tag := parts[len(parts)-1]
	if !strings.HasPrefix(tag, "v") {
		return "", fmt.Errorf("unexpected tag format: %s", tag)
	}
	return tag, nil
}

func installRelease(tag string) error {
	bin, err := downloadRelease(tag)
	if err != nil {
		return err
	}
	defer os.Remove(bin)
	return installBinary(bin, version, tag)
}

func downloadRelease(tag string) (string, error) {
	if runtime.GOOS != "linux" {
		return "", fmt.Errorf("unsupported os %s", runtime.GOOS)
	}
	name := fmt.Sprintf("picoman-%s-linux-%s", tag, runtime.GOARCH)
	url := fmt.Sprintf("https://github.com/%s/releases/download/%s/%s", repo, tag, name)

	fmt.Printf("downloading %s...\n", name)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download failed: %s", resp.Status)
	}

	tmp, err := os.CreateTemp("", "picoman-update-*")
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	if err := os.Chmod(tmp.Name(), 0o755); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

func installBinary(bin, from, to string) error {
	install := exec.Command(bin, "install")
	install.Stdout = os.Stdout
	install.Stderr = os.Stderr
	if err := install.Run(); err != nil {
		return err
	}
	if err := writeRestartMarker("update", from, to); err != nil {
		return err
	}
	return restartService()
}

func restartService() error {
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
			return fmt.Errorf("restart failed: %v; start after reset-failed failed: %w", err, startErr)
		}
	}
	return nil
}

type releaseInfo struct {
	Tag         string `json:"tag_name"`
	PublishedAt string `json:"published_at"`
	URL         string `json:"html_url"`
}

func fetchReleases() ([]releaseInfo, error) {
	url := "https://api.github.com/repos/" + repo + "/releases?per_page=10"
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub releases: %s", resp.Status)
	}
	var releases []releaseInfo
	if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
		return nil, err
	}
	return releases, nil
}

func updateText() (string, error) {
	releases, err := fetchReleases()
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "picoman current: <b>%s</b>\n", html.EscapeString(version))
	if len(releases) == 0 {
		sb.WriteString("\nno releases")
		return sb.String(), nil
	}
	fmt.Fprintf(&sb, "latest: <a href=\"%s\">%s</a>\n\n",
		html.EscapeString(releases[0].URL),
		html.EscapeString(releases[0].Tag),
	)
	for _, r := range releases {
		mark := ""
		if r.Tag == version {
			mark = " current"
		}
		date := r.PublishedAt
		if len(date) > 10 {
			date = date[:10]
		}
		alias := strings.ReplaceAll(strings.TrimPrefix(r.Tag, "v"), ".", "_")
		fmt.Fprintf(&sb, "/v%s <a href=\"%s\">%s</a> %s%s\n",
			html.EscapeString(alias),
			html.EscapeString(r.URL),
			html.EscapeString(r.Tag),
			html.EscapeString(date),
			mark,
		)
	}
	return strings.TrimSpace(sb.String()), nil
}

func handleInstallVersionMessage(out *outbox.Store, bot *tg.Client, msg tg.Message, tag string) {
	if err := out.EnqueueReply(msg.Chat.ID, msg.MessageID, "⏳ installing "+tag+"..."); err != nil {
		logEnqueueError(bot, msg.Chat.ID, err)
	}
	flushOutbox(out)
	if err := installRelease(tag); err != nil {
		if enqueueErr := out.EnqueueReply(msg.Chat.ID, msg.MessageID, errorText(err.Error())); enqueueErr != nil {
			logEnqueueError(bot, msg.Chat.ID, enqueueErr)
		}
		flushOutbox(out)
	}
}

func isVersionCommand(cmd string) bool {
	if !strings.HasPrefix(cmd, "v") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(cmd, "v"), "_")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.Atoi(part); err != nil {
			return false
		}
	}
	return true
}

func tagFromVersionCommand(cmd string) string {
	return "v" + strings.ReplaceAll(strings.TrimPrefix(cmd, "v"), "_", ".")
}

func logEnqueueError(bot *tg.Client, chatID int64, err error) {
	fmt.Fprintf(os.Stderr, "enqueue reply: %v\n", err)
	go criticalNotifyUser(chatID, bot, "outbox", err)
}

func stripHTML(s string) string {
	r := strings.NewReplacer("<b>", "", "</b>", "", "<a href=\"", "", "\">", " ", "</a>", "")
	return r.Replace(s)
}
