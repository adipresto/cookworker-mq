//go:build ignore

// Kitchen TUI: what the waiter & cooks see — queue depths + pods + recent events.
// Run: go run tui.go  (snapshot without TUI: go run tui.go -once)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type qstat struct {
	Name   string `json:"name"`
	Ready  int    `json:"messages_ready"`
	Unack  int    `json:"messages_unacknowledged"`
	Total  int    `json:"messages"`
}

type snapshot struct {
	orders qstat
	done   qstat
	pods   []string
	waiter []string
	cook   []string
	at     time.Time
	err    string
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getQueue(api, user, pass, q string) (qstat, error) {
	var s qstat
	req, err := http.NewRequest("GET", strings.TrimRight(api, "/")+"/api/queues/%2F/"+q, nil)
	if err != nil {
		return s, err
	}
	req.SetBasicAuth(user, pass)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return s, fmt.Errorf("%s: %s", q, strings.TrimSpace(string(body)))
	}
	return s, json.Unmarshal(body, &s)
}

func getK8s() (pods, waiter, cook []string) {
	script := `sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen get pods --no-headers 2>&1; echo ---W---; sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen logs deploy/waiter --tail=6 2>&1; echo ---C---; sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen logs deploy/cook --tail=6 2>&1`
	out, _ := exec.Command("wsl", "-d", "NixOS", "--", "sh", "-c", script).CombinedOutput()
	section := 0
	for _, ln := range strings.Split(string(out), "\n") {
		ln = strings.TrimRight(ln, "\r")
		switch strings.TrimSpace(ln) {
		case "---W---":
			section = 1
			continue
		case "---C---":
			section = 2
			continue
		}
		if strings.TrimSpace(ln) == "" {
			continue
		}
		switch section {
		case 0:
			pods = append(pods, ln)
		case 1:
			waiter = append(waiter, ln)
		default:
			cook = append(cook, ln)
		}
	}
	return pods, waiter, cook
}

func fetch(api, user, pass string) snapshot {
	s := snapshot{at: time.Now()}
	var errs []string
	if q, err := getQueue(api, user, pass, "orders"); err != nil {
		errs = append(errs, err.Error())
	} else {
		s.orders = q
	}
	if q, err := getQueue(api, user, pass, "done"); err != nil {
		errs = append(errs, err.Error())
	} else {
		s.done = q
	}
	s.pods, s.waiter, s.cook = getK8s()
	if len(s.pods) == 0 {
		errs = append(errs, "k3s: no pods (namespace missing?)")
	}
	s.err = strings.Join(errs, " · ")
	return s
}

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	boxStyle   = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("63")).Padding(0, 1)
	hintStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)
)

type tickMsg struct{}
type dataMsg struct{ s snapshot }

type model struct {
	api, user, pass string
	cur             snapshot
	loading         bool
}

func (m model) Init() tea.Cmd { return m.refresh }

func (m model) refresh() tea.Msg {
	return dataMsg{s: fetch(m.api, m.user, m.pass)}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case dataMsg:
		m.cur, m.loading = msg.s, false
		return m, tea.Tick(3*time.Second, func(time.Time) tea.Msg { return tickMsg{} })
	case tickMsg:
		m.loading = true
		return m, m.refresh
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "r":
			m.loading = true
			return m, m.refresh
		}
	}
	return m, nil
}

func qline(s qstat) string {
	if s.Name == "" {
		return "…"
	}
	return fmt.Sprintf("%-6s Ready=%-4d Unacked=%-4d Total=%d", s.Name, s.Ready, s.Unack, s.Total)
}

func (m model) View() string {
	s := titleStyle.Render("Kitchen — what waiter & cooks see") + "\n"
	s += hintStyle.Render(fmt.Sprintf("broker %s · updated %s", m.api, m.cur.at.Format("15:04:05"))) + "\n"
	s += boxStyle.Render("QUEUES\n"+qline(m.cur.orders)+"\n"+qline(m.cur.done)) + "\n"
	pods := "(none)"
	if len(m.cur.pods) > 0 {
		pods = strings.Join(m.cur.pods, "\n")
	}
	s += boxStyle.Render("PODS\n"+pods) + "\n"
	ev := append(append([]string{"-- waiter"}, m.cur.waiter...), append([]string{"-- cook"}, m.cur.cook...)...)
	s += boxStyle.Render("EVENTS\n" + strings.Join(ev, "\n")) + "\n"
	if m.cur.err != "" {
		s += errStyle.Render("! "+m.cur.err) + "\n"
	}
	if m.loading {
		s += hintStyle.Render("refreshing…") + "\n"
	}
	s += hintStyle.Render("r refresh | q quit")
	return s
}

func main() {
	once := flag.Bool("once", false, "print one snapshot, no TUI")
	flag.Parse()
	api, user, pass := getenv("RABBIT_API", "http://localhost:15672"), getenv("RABBIT_USER", "guest"), getenv("RABBIT_PASS", "guest")
	if *once {
		s := fetch(api, user, pass)
		fmt.Printf("orders: Ready=%d Unacked=%d | done: Ready=%d Unacked=%d\n", s.orders.Ready, s.orders.Unack, s.done.Ready, s.done.Unack)
		fmt.Println("pods:"); for _, p := range s.pods {
			fmt.Println("  " + p)
		}
		if s.err != "" {
			fmt.Println("err: " + s.err)
		}
		return
	}
	if _, err := tea.NewProgram(model{api: api, user: user, pass: pass, loading: true}).Run(); err != nil {
		fmt.Fprintln(os.Stderr, "tui:", err)
		os.Exit(1)
	}
}
