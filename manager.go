package main

// Manager: the only thing allowed to touch k8s. Clients ask it over HTTP,
// never kubectl, never wsl. Same binary, ROLE=manager.
//
// API (no auth: local-dev LAN like RabbitMQ guest/guest):
//   GET  /healthz
//   GET  /pods                        live cook+waiter pods
//   POST /cooks/kill      {"pod"}     delete one cook pod
//   POST /cooks/kill-all              delete all cook pods
//   POST /cooks/scale     {"replicas"} 0-5
//   POST /cooks/restart               rollout restart
//
// ponytail: stdlib REST against the API server, no client-go for 4 calls.
// ponytail: no auth, add when Manager leaves the laptop.

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type k8sClient struct {
	base  string
	token string
	http  *http.Client
	ns    string
}

func k8sConfig() k8sClient {
	ns := env("NAMESPACE", "kitchen")
	if host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT"); host != "" {
		tok, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		ca, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		return k8sClient{
			base:  "https://" + host + ":" + port,
			token: strings.TrimSpace(string(tok)),
			http:  &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}},
			ns:    ns,
		}
	}
	// Local fallback: kubectl-style kubeconfig is just a server URL we trust.
	// ponytail: file parse skipped, K8S_SERVER override if local runs matter.
	return k8sClient{
		base: strings.TrimRight(env("K8S_SERVER", "https://127.0.0.1:6443"), "/"),
		http: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}, //nolint:gosec
		ns:   ns,
	}
}

func (k k8sClient) do(method, path string, body any) (int, []byte, error) {
	var r *strings.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = strings.NewReader(string(b))
	} else {
		r = strings.NewReader("")
	}
	req, err := http.NewRequest(method, k.base+path, r)
	if err != nil {
		return 0, nil, err
	}
	if k.token != "" {
		req.Header.Set("Authorization", "Bearer "+k.token)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/merge-patch+json")
	}
	resp, err := k.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	if resp.StatusCode >= 300 {
		return resp.StatusCode, buf, fmt.Errorf("k8s %d: %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	return resp.StatusCode, buf, nil
}

type podInfo struct {
	Name     string `json:"name"`
	App      string `json:"app"`
	Phase    string `json:"phase"`
	Ready    bool   `json:"ready"`
	Restarts int    `json:"restarts"`
	Age      string `json:"age"`
}

func (k k8sClient) pods() ([]podInfo, error) {
	_, raw, err := k.do("GET", "/api/v1/namespaces/"+k.ns+"/pods", nil)
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
				Age    string            `json:"creationTimestamp"`
			} `json:"metadata"`
			Status struct {
				Phase      string `json:"phase"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
				ContainerStatuses []struct {
					RestartCount int `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	var out []podInfo
	for _, p := range list.Items {
		app := p.Metadata.Labels["app"]
		if app != "cook" && app != "waiter" {
			continue
		}
		ready := false
		for _, c := range p.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				ready = true
			}
		}
		restarts := 0
		for _, c := range p.Status.ContainerStatuses {
			restarts += c.RestartCount
		}
		age := p.Metadata.Age
		if t, err := time.Parse(time.RFC3339, p.Metadata.Age); err == nil {
			age = time.Since(t).Truncate(time.Second).String()
		}
		out = append(out, podInfo{Name: p.Metadata.Name, App: app, Phase: p.Status.Phase, Ready: ready, Restarts: restarts, Age: age})
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func runManager() error {
	k := k8sConfig()
	addr := env("MANAGER_ADDR", ":8080")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/pods", func(w http.ResponseWriter, _ *http.Request) {
		pods, err := k.pods()
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, pods)
	})
	mux.HandleFunc("/cooks/kill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeJSON(w, 405, map[string]string{"error": "POST"})
			return
		}
		var req struct {
			Pod string `json:"pod"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Pod == "" {
			writeJSON(w, 400, map[string]string{"error": "need {\"pod\": ...}"})
			return
		}
		if _, _, err := k.do("DELETE", "/api/v1/namespaces/"+k.ns+"/pods/"+req.Pod, nil); err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		log.Printf("killed %s", req.Pod)
		writeJSON(w, 200, map[string]string{"killed": req.Pod})
	})
	mux.HandleFunc("/cooks/kill-all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeJSON(w, 405, map[string]string{"error": "POST"})
			return
		}
		if _, _, err := k.do("DELETE", "/api/v1/namespaces/"+k.ns+"/pods?labelSelector=app%3Dcook", nil); err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		log.Print("killed all cooks")
		writeJSON(w, 200, map[string]string{"killed": "all"})
	})
	mux.HandleFunc("/cooks/scale", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeJSON(w, 405, map[string]string{"error": "POST"})
			return
		}
		var req struct {
			Replicas int `json:"replicas"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Replicas < 0 || req.Replicas > 5 {
			writeJSON(w, 400, map[string]string{"error": "replicas 0-5"})
			return
		}
		_, _, err := k.do("PATCH", "/apis/apps/v1/namespaces/"+k.ns+"/deployments/cook",
			map[string]any{"spec": map[string]any{"replicas": req.Replicas}})
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		log.Printf("scaled to %d", req.Replicas)
		writeJSON(w, 200, map[string]int{"replicas": req.Replicas})
	})
	mux.HandleFunc("/cooks/restart", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeJSON(w, 405, map[string]string{"error": "POST"})
			return
		}
		patch := map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]string{
			"kubectl.kubernetes.io/restartedAt": time.Now().UTC().Format(time.RFC3339),
		}}}}}
		_, _, err := k.do("PATCH", "/apis/apps/v1/namespaces/"+k.ns+"/deployments/cook", patch)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		log.Print("restarted cooks")
		writeJSON(w, 200, map[string]string{"restarted": "cook"})
	})
	log.Printf("manager on %s (ns %s)", addr, k.ns)
	return http.ListenAndServe(addr, mux)
}
