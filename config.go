package main

// Shared .env loader for the main binary (cook/waiter/manager).
// tui.go carries its own copy: it builds standalone via `go run tui.go`.
// Real process env always wins; missing file = real env only (CI, in-cluster).

import (
	"bufio"
	"os"
	"strings"
)

func loadDotEnv() {
	f, err := os.Open(".env")
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		ln := strings.TrimSpace(sc.Text())
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		ln = strings.TrimSpace(strings.TrimPrefix(ln, "export "))
		k, v, ok := strings.Cut(ln, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			continue
		}
		v = strings.TrimSpace(strings.Trim(strings.TrimSpace(v), `"'`))
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
