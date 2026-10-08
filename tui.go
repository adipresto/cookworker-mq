//go:build ignore

// Kitchen TUI: order like a customer, manage cooks like a boss.
// o order · j/k select cook · x kill · X kill all · +/- scale · R restart · r refresh · q quit · -once snapshot.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	amqp "github.com/rabbitmq/amqp091-go"
)

type qstat struct {
	Name  string `json:"name"`
	Ready int    `json:"messages_ready"`
	Unack int    `json:"messages_unacknowledged"`
	Total int    `json:"messages"`
}

type cookStat struct {
	pod   string // short id for display
	full  string // full pod name for kubectl
	line  string // raw last log line
	state string // cooking | idle
	order string
	value string
	start time.Time
	prep  time.Duration
}

type orderItem struct {
	id    string
	value string
}

type snapshot struct {
	requests qstat
	orders   qstat
	done     qstat
	cooks    []cookStat
	waiter   []string
	waiting  []orderItem // orders queue Ready: registered, no free cook
	ready    []orderItem // done queue Ready: cooked, waiter hasn't picked up
	want     int         // cook deploy desired replicas
	readyN   string      // "ready/want" for cook deploy
	at       time.Time
	err      string
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

// peekQueue lists Ready payloads without consuming (ack_requeue_true).
func peekQueue(api, user, pass, q string, count int) []orderItem {
	body, _ := json.Marshal(map[string]any{"count": count, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": 50000})
	req, err := http.NewRequest("POST", strings.TrimRight(api, "/")+"/api/queues/%2F/"+q+"/get", bytes.NewReader(body))
	if err != nil {
		return nil
	}
	req.SetBasicAuth(user, pass)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil
	}
	var msgs []struct {
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil
	}
	var out []orderItem
	for _, m := range msgs {
		var o struct {
			ID    string `json:"id"`
			Value string `json:"value"`
		}
		if err := json.Unmarshal([]byte(m.Payload), &o); err != nil || o.ID == "" {
			continue
		}
		out = append(out, orderItem{id: o.ID, value: o.Value})
	}
	return out
}

func shortPod(p string) string {
	if i := strings.LastIndex(p, "-"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// parse turns the last log line into a live state.
// cooking lines look like: 2006/01/02 15:04:05 cooking <id> "value" (45s)
func (c *cookStat) parse() {
	c.state = "idle"
	f := strings.Fields(c.line)
	if len(f) >= 4 && f[2] == "cooking" {
		ts, terr := time.Parse("2006/01/02 15:04:05", f[0]+" "+f[1])
		if i := strings.Index(c.line, "\""); i >= 0 {
			if j := strings.Index(c.line[i+1:], "\""); j >= 0 {
				c.value = c.line[i+1 : i+1+j]
			}
		}
		if i := strings.LastIndex(c.line, "("); i >= 0 {
			if d, err := time.ParseDuration(strings.TrimRight(strings.TrimSpace(c.line[i+1:]), ")")); err == nil {
				c.prep = d
			}
		}
		if terr == nil && c.prep > 0 {
			c.start, c.order, c.state = ts, f[3], "cooking"
		}
	}
}

// status renders the countdown live.
func (c cookStat) status() string {
	if c.state == "cooking" {
		left := c.prep - time.Since(c.start)
		if left < 0 {
			left = 0
		}
		v := c.value
		if v == "" {
			v = c.order
		}
		return fmt.Sprintf("cooking %q · %ds left", v, int(left.Seconds()))
	}
	if c.line == "" {
		return "(starting…)"
	}
	if strings.Contains(c.line, "cooked") {
		return "idle"
	}
	if len(c.line) > 90 {
		return c.line[:89] + "…"
	}
	return c.line
}

// One wsl call: pods, then per-cook-pod tails, then waiter tail.
// One wsl call: pods, then per-cook-pod tails, then waiter tail.
// NOTE: runs via temp script FILE — $vars passed in `sh -c "..."` arrive
// empty through wsl.exe arg forwarding, so never inline them.
func getK8s() (pods []string, cooks []cookStat, waiter []string, want int, ready string) {
	script := `K="sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen"
$K get pods --no-headers 2>&1
echo ---COOKS---
for p in $($K get pods -l app=cook --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
  echo ---POD $p---
  $K logs pod/$p --tail=4 2>&1
done
echo ---W---
$K logs deploy/waiter --tail=6 2>&1
echo ---DEPLOY---
$K get deploy cook -o jsonpath='{.spec.replicas} {.status.readyReplicas}' 2>&1
`
	tmp, err := os.CreateTemp("", "kitchen-*.sh")
	if err != nil {
		return nil, nil, nil, 0, ""
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(script); err != nil {
		return nil, nil, nil, 0, ""
	}
	tmp.Close()
	win := tmp.Name()
	wslPath := "/mnt/" + strings.ToLower(string(win[0])) + strings.ReplaceAll(win[2:], `\`, "/")
	out, _ := exec.Command("wsl", "-d", "NixOS", "--", "sh", wslPath).CombinedOutput()
	section := 0
	cur := -1
	for _, ln := range strings.Split(string(out), "\n") {
		ln = strings.TrimRight(ln, "\r")
		t := strings.TrimSpace(ln)
		if strings.HasPrefix(t, "---POD ") {
			section = 1
			name := strings.TrimSuffix(strings.TrimPrefix(t, "---POD "), "---")
			cooks = append(cooks, cookStat{pod: shortPod(name), full: name})
			cur = len(cooks) - 1
			continue
		}
		switch t {
		case "---COOKS---":
			continue
		case "---W---":
			section = 2
			continue
		case "---DEPLOY---":
			section = 3
			continue
		}
		if t == "" {
			continue
		}
		switch section {
		case 0:
			pods = append(pods, ln)
		case 1:
			if cur >= 0 {
				cooks[cur].line = ln // last line wins = what the cook is doing now
			}
		case 2:
			waiter = append(waiter, ln)
		default: // deploy: "want ready"
			if f := strings.Fields(t); len(f) >= 1 {
				if n, err := strconv.Atoi(f[0]); err == nil {
					want = n
					r := "0"
					if len(f) >= 2 {
						r = f[1]
					}
					ready = r + "/" + f[0]
				}
			}
		}
	}
	return pods, cooks, waiter, want, ready
}

func fetch(api, user, pass string) snapshot {
	s := snapshot{at: time.Now()}
	var errs []string
	for _, q := range []string{"requests", "orders", "done"} {
		st, err := getQueue(api, user, pass, q)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		switch q {
		case "requests":
			s.requests = st
		case "orders":
			s.orders = st
		default:
			s.done = st
		}
	}
	var pods []string
	pods, s.cooks, s.waiter, s.want, s.readyN = getK8s()
	_ = pods
	for i := range s.cooks {
		s.cooks[i].parse()
	}
	s.waiting = peekQueue(api, user, pass, "orders", 20)
	s.ready = peekQueue(api, user, pass, "done", 20)
	if len(s.cooks) == 0 && len(s.waiter) == 0 {
		errs = append(errs, "k3s: no data (namespace missing?)")
	}
	s.err = strings.Join(errs, " · ")
	return s
}

// sendRequest hands the dish to the waiter via the requests queue.
func sendRequest(amqpURL, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("empty order")
	}
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return "", err
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare("requests", true, false, false, false, nil); err != nil {
		return "", err
	}
	body, _ := json.Marshal(map[string]string{"value": value})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, "", "requests", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
	}); err != nil {
		return "", err
	}
	return value, nil
}

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	boxStyle   = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("63")).Padding(0, 1)
	hintStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	okStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Bold(true)
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)
)

type tickMsg struct{}
type dataMsg struct{ s snapshot }
type sentMsg struct {
	dish string
	err  error
}
type actionMsg struct {
	msg string
	err error
}

// kctl runs kubectl in WSL with no shell (avoids wsl.exe $var forwarding loss).
func kctl(note string, args ...string) tea.Cmd {
	return func() tea.Msg {
		full := append([]string{"-d", "NixOS", "--", "sudo", "kubectl", "--kubeconfig", "/etc/rancher/k3s/k3s.yaml", "-n", "kitchen"}, args...)
		if out, err := exec.Command("wsl", full...).CombinedOutput(); err != nil {
			return actionMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(out)))}
		}
		return actionMsg{msg: note}
	}
}

type model struct {
	api, user, pass, amqpURL string
	cur                      snapshot
	input                    textinput.Model
	typing                   bool
	loading                  bool
	notice                   string
	sel                      int // selected cook
}

func (m model) Init() tea.Cmd { return m.refresh }

func (m model) refresh() tea.Msg {
	return dataMsg{s: fetch(m.api, m.user, m.pass)}
}

func (m model) send() tea.Msg {
	dish, err := sendRequest(m.amqpURL, m.input.Value())
	return sentMsg{dish: dish, err: err}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case dataMsg:
		m.cur, m.loading = msg.s, false
		if m.sel >= len(m.cur.cooks) {
			m.sel = max(len(m.cur.cooks)-1, 0)
		}
		return m, tea.Tick(3*time.Second, func(time.Time) tea.Msg { return tickMsg{} })
	case tickMsg:
		m.loading = true
		return m, m.refresh
	case sentMsg:
		if msg.err != nil {
			m.notice = "send failed: " + msg.err.Error()
		} else {
			m.notice = "requested " + msg.dish
			m.input.SetValue("")
		}
		m.typing = false
		m.input.Blur()
		m.loading = true
		return m, m.refresh
	case actionMsg:
		if msg.err != nil {
			m.notice = "action failed: " + msg.err.Error()
		} else {
			m.notice = msg.msg
		}
		m.loading = true
		return m, m.refresh
	case tea.KeyMsg:
		if m.typing {
			switch msg.String() {
			case "esc":
				m.typing = false
				m.input.Blur()
				return m, nil
			case "ctrl+u":
				m.input.SetValue("")
				return m, nil
			case "enter":
				return m, m.send
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			return m, cmd
		}
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "r":
			m.loading = true
			return m, m.refresh
		case "o", "tab":
			m.typing = true
			m.notice = ""
			return m, m.input.Focus()
		case "j", "down":
			if m.sel < len(m.cur.cooks)-1 {
				m.sel++
			}
		case "k", "up":
			if m.sel > 0 {
				m.sel--
			}
		case "x":
			if len(m.cur.cooks) == 0 {
				m.notice = "no cooks to kill"
				return m, nil
			}
			c := m.cur.cooks[m.sel]
			return m, kctl("killed "+c.pod, "delete", "pod/"+c.full, "--wait=false")
		case "X":
			return m, kctl("killed all cooks", "delete", "pods", "-l", "app=cook", "--wait=false")
		case "+", "=":
			if m.cur.want >= 5 {
				m.notice = "already at max 5 cooks"
				return m, nil
			}
			return m, kctl("scaled up", "scale", "deploy/cook", fmt.Sprintf("--replicas=%d", m.cur.want+1))
		case "-", "_":
			if m.cur.want <= 0 {
				m.notice = "already at 0 cooks"
				return m, nil
			}
			return m, kctl("scaled down", "scale", "deploy/cook", fmt.Sprintf("--replicas=%d", m.cur.want-1))
		case "R":
			return m, kctl("restarted cooks", "rollout", "restart", "deploy/cook")
		}
	}
	return m, nil
}

func qline(s qstat) string {
	if s.Name == "" {
		return "…"
	}
	return fmt.Sprintf("%-8s Ready=%-4d Unacked=%-4d", s.Name, s.Ready, s.Unack)
}

func shortID(id string) string {
	if len(id) > 18 {
		return "…" + id[len(id)-15:]
	}
	return id
}

func orderLine(o orderItem) string {
	if o.value == "" {
		return shortID(o.id)
	}
	return fmt.Sprintf("%q (%s)", o.value, shortID(o.id))
}

func (m model) View() string {
	s := titleStyle.Render("Kitchen — order here, waiter runs it") + "\n"
	s += hintStyle.Render(fmt.Sprintf("updated %s", m.cur.at.Format("15:04:05"))) + "\n"
	s += boxStyle.Render("QUEUES\n"+qline(m.cur.requests)+"\n"+qline(m.cur.orders)+"\n"+qline(m.cur.done)) + "\n"
	// Orders by lifecycle stage.
	var wt, ck, rd []string
	for _, o := range m.cur.waiting {
		wt = append(wt, "· "+orderLine(o))
	}
	for _, c := range m.cur.cooks {
		if c.state == "cooking" {
			v := c.value
			if v == "" {
				v = c.order
			}
			ck = append(ck, fmt.Sprintf("· %q (%s) @%s", v, shortID(c.order), c.pod))
		}
	}
	for _, o := range m.cur.ready {
		rd = append(rd, "· "+orderLine(o))
	}
	if len(wt) == 0 {
		wt = []string{"(none)"}
	}
	if len(ck) == 0 {
		ck = []string{"(none)"}
	}
	if len(rd) == 0 {
		rd = []string{"(none)"}
	}
	if extra := m.cur.orders.Ready - len(m.cur.waiting); extra > 0 {
		wt = append(wt, fmt.Sprintf("+%d more…", extra))
	}
	if extra := m.cur.orders.Unack - len(ck); extra > 0 {
		ck = append(ck, fmt.Sprintf("+%d more…", extra))
	}
	if extra := m.cur.done.Ready - len(m.cur.ready); extra > 0 {
		rd = append(rd, fmt.Sprintf("+%d more…", extra))
	}
	s += boxStyle.Render(fmt.Sprintf("ORDERS — waiting:%d cooking:%d ready:%d\nWAITING (orders Ready)\n%s\nCOOKING (orders Unacked)\n%s\nREADY FOR WAITER (done Ready, serving Unacked=%d)\n%s",
		m.cur.orders.Ready, m.cur.orders.Unack, m.cur.done.Ready, strings.Join(wt, "\n"), strings.Join(ck, "\n"), m.cur.done.Unack, strings.Join(rd, "\n"))) + "\n"
	cooks := "(no cooks)"
	if len(m.cur.cooks) > 0 {
		var lines []string
		for i, c := range m.cur.cooks {
			cur := "  "
			if i == m.sel {
				cur = "> "
			}
			lines = append(lines, cur+c.pod+": "+c.status())
		}
		cooks = strings.Join(lines, "\n")
	}
	deploy := "cook deploy"
	if m.cur.readyN != "" {
		deploy = fmt.Sprintf("cook deploy: %s ready (want %d)", m.cur.readyN, m.cur.want)
	}
	s += boxStyle.Render("COOKS — "+deploy+"\n"+cooks) + "\n"
	ev := strings.Join(m.cur.waiter, "\n")
	if ev == "" {
		ev = "(nothing yet)"
	}
	s += boxStyle.Render("WAITER\n"+ev) + "\n"
	s += "Order [o to type, enter to send]: " + m.input.View() + "\n"
	if m.notice != "" {
		if strings.HasPrefix(m.notice, "requested") {
			s += okStyle.Render(m.notice) + "\n"
		} else {
			s += errStyle.Render(m.notice) + "\n"
		}
	}
	if m.cur.err != "" {
		s += errStyle.Render("! "+m.cur.err) + "\n"
	}
	if m.loading {
		s += hintStyle.Render("refreshing…") + "\n"
	}
	s += hintStyle.Render("o order · j/k select cook · x kill · X kill all · +/- scale · R restart · r refresh · q quit")
	return s
}

func main() {
	once := flag.Bool("once", false, "print one snapshot, no TUI")
	flag.Parse()
	api, user, pass := getenv("RABBIT_API", "http://localhost:15672"), getenv("RABBIT_USER", "guest"), getenv("RABBIT_PASS", "guest")
	amqpURL := getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	if *once {
		s := fetch(api, user, pass)
		fmt.Printf("requests=%d orders: Ready=%d Unacked=%d | done: Ready=%d Unacked=%d\n", s.requests.Ready, s.orders.Ready, s.orders.Unack, s.done.Ready, s.done.Unack)
		fmt.Printf("cook deploy: %s ready (want %d)\n", s.readyN, s.want)
		fmt.Printf("waiting (%d):\n", s.orders.Ready)
		for _, o := range s.waiting {
			fmt.Printf("  - %s\n", orderLine(o))
		}
		fmt.Printf("cooking (%d):\n", s.orders.Unack)
		for _, c := range s.cooks {
			if c.state == "cooking" {
				v := c.value
				if v == "" {
					v = c.order
				}
				fmt.Printf("  - %q (%s) @%s\n", v, shortID(c.order), c.pod)
			}
		}
		fmt.Printf("ready for waiter (%d):\n", s.done.Ready)
		for _, o := range s.ready {
			fmt.Printf("  - %s\n", orderLine(o))
		}
		for _, c := range s.cooks {
			fmt.Printf("cook %s: %s\n", c.pod, c.status())
		}
		if s.err != "" {
			fmt.Println("err: " + s.err)
		}
		return
	}
	ti := textinput.New()
	ti.Placeholder = "mie ayam…"
	ti.CharLimit = 64
	ti.Width = 40
	if _, err := tea.NewProgram(model{api: api, user: user, pass: pass, amqpURL: amqpURL, input: ti, loading: true}).Run(); err != nil {
		fmt.Fprintln(os.Stderr, "tui:", err)
		os.Exit(1)
	}
}
