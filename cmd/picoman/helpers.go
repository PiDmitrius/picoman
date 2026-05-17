package main

import (
	"fmt"
	"html"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// escapedCodeBlock escapes s for HTML and trims so the escaped output (plus
// the truncated marker) fits within budget bytes. Trimming happens on a UTF-8
// boundary in the *source* string before escaping, so HTML entities and tags
// are never split mid-sequence — Telegram parses them cleanly.
func escapedCodeBlock(s string, budget int) string {
	escaped := html.EscapeString(s)
	if len(escaped) <= budget {
		return escaped
	}
	// Reserve space for the marker so we don't overshoot when we add it.
	target := budget - len("\n... (truncated 9999999 bytes)")
	if target < 0 {
		target = 0
	}
	// Binary search the largest source prefix whose escaped form fits.
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		for mid > 0 && !utf8.RuneStart(s[mid]) {
			mid--
		}
		if mid == lo {
			break
		}
		if len(html.EscapeString(s[:mid])) <= target {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return html.EscapeString(s[:lo]) + fmt.Sprintf("\n... (truncated %d bytes)", len(s)-lo)
}

func tildePath(path string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	if path == home {
		return "~"
	}
	if strings.HasPrefix(path, home+string(filepath.Separator)) {
		return "~" + strings.TrimPrefix(path, home)
	}
	return path
}
