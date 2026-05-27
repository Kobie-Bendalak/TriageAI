package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

var Version = "dev"

// ── Prometheus metrics ────────────────────────────────────────────────────────

var (
	metricErrorsCaught = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "triageai_errors_caught_total",
		Help: "Total log lines matching error filters, by repo and service.",
	}, []string{"repo", "service"})

	metricDispatches = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "triageai_dispatches_total",
		Help: "Total repair dispatches, by repo, service and tier (tier0/tier1/tier2).",
	}, []string{"repo", "service", "tier"})

	// outcome = "success" | "failure" | "escalated" (tier1 fell through to tier2)
	metricDispatchOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "triageai_dispatch_outcomes_total",
		Help: "Dispatch outcomes by repo, service, tier, and outcome (success/failure/escalated).",
	}, []string{"repo", "service", "tier", "outcome"})

	// error_type extracted from the matched log line (auth/oom/crash/timeout/other)
	metricErrorsByType = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "triageai_errors_by_type_total",
		Help: "Caught errors classified by type (auth/oom/crash/timeout/other), by repo and service.",
	}, []string{"repo", "service", "error_type"})

	metricDispatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "triageai_dispatch_duration_seconds",
		Help:    "Time from error detection to dispatch completion.",
		Buckets: prometheus.DefBuckets,
	}, []string{"repo", "tier"})

	metricLastDispatch = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "triageai_last_dispatch_timestamp",
		Help: "Unix timestamp of the most recent dispatch, by repo and service.",
	}, []string{"repo", "service"})
)

// ── Config ────────────────────────────────────────────────────────────────────

type PatternAction struct {
	Match  string `yaml:"match"`
	Action string `yaml:"action"`
}

type DispatchConfig struct {
	Patterns          []PatternAction `yaml:"patterns"`
	IntentURL         string          `yaml:"intent_url"`
	AgentCmd          string          `yaml:"agent_cmd"`
	DebounceSecs      int             `yaml:"debounce_secs"`
	MinIntervalSecs   int             `yaml:"min_interval_secs"`
	EscalateAfterSecs int             `yaml:"escalate_after_secs"`
	DryRun            bool            `yaml:"dry_run"`
}

type NotificationsConfig struct {
	DiscordWebhook string `yaml:"discord_webhook"`
}

type FiltersConfig struct {
	Match []string `yaml:"match"`
	Skip  []string `yaml:"skip"`
}

type RepoConfig struct {
	Path        string `yaml:"path"`
	Name        string `yaml:"name"`
	ComposeFile string `yaml:"compose_file"`
}

type Config struct {
	GatewayURL    string              `yaml:"gateway_url"`
	Model         string              `yaml:"model"`
	Repos         []RepoConfig        `yaml:"repos"`
	Filters       FiltersConfig       `yaml:"filters"`
	Notifications NotificationsConfig `yaml:"notifications"`
	Dispatch      DispatchConfig      `yaml:"dispatch"`
	Template      string              `yaml:"template"`
}

func defaultConfig() Config {
	return Config{
		GatewayURL: "http://localhost:7878",
		Model:      "",
		Filters: FiltersConfig{
			Match: []string{"WARN", "WARNING", "ERROR", "FATAL", "panic:", "CRIT"},
			Skip:  []string{"task queue depth", "Redis index race", "keyword_search", "DeadlineExceeded", "stream copy", "no changes to commit"},
		},
		Dispatch: DispatchConfig{
			AgentCmd:          "claude",
			DebounceSecs:      120,
			MinIntervalSecs:   30,
			EscalateAfterSecs: 60,
			DryRun:            false,
		},
		Template: defaultTemplate,
	}
}

const defaultTemplate = `You are an autonomous repair agent for a containerized service stack.

SERVICE: {{.Service}}
REPO: {{.RepoPath}}
TIME: {{.Timestamp}}

RECENT LOG ERRORS:
{{.ErrorLines}}

TASK:
1. Diagnose the root cause of the error above.
2. Apply the minimal fix (edit files, update config, restart the service via ` + "`" + `docker compose restart {{.Service}}` + "`" + `).
3. Verify the fix (check service health, tail logs briefly).
4. Print a one-paragraph summary of what you found and what you changed.

Constraints:
- Do NOT force-push, drop databases, or delete volumes.
- If the fix requires a secret you do not have, say so and stop.
- Prefer ` + "`" + `docker compose restart` + "`" + ` over full ` + "`" + `up -d` + "`" + ` unless dependencies changed.`

// ── Monitor state ─────────────────────────────────────────────────────────────

type ErrorBucket struct {
	Service     string
	Lines       []string
	FirstSeen   time.Time
	Fingerprint string
}

type TemplateData struct {
	Service    string
	RepoPath   string
	RepoName   string
	ErrorLines string
	Timestamp  string
}

type Monitor struct {
	cfg          Config
	repo         RepoConfig
	matchRe      *regexp.Regexp
	skipRes      []*regexp.Regexp
	patternRes   []*regexp.Regexp
	seen         map[string]time.Time
	lastDispatch time.Time
	mu           sync.Mutex
	tmpl         *template.Template
	httpClient   *http.Client
}

func newMonitor(cfg Config, repo RepoConfig) (*Monitor, error) {
	matchRe, err := regexp.Compile(`(?i)(` + strings.Join(cfg.Filters.Match, "|") + `)`)
	if err != nil {
		return nil, fmt.Errorf("match regex: %w", err)
	}

	var skipRes []*regexp.Regexp
	for _, s := range cfg.Filters.Skip {
		r, err := regexp.Compile(`(?i)` + regexp.QuoteMeta(s))
		if err != nil {
			return nil, fmt.Errorf("skip regex %q: %w", s, err)
		}
		skipRes = append(skipRes, r)
	}

	var patternRes []*regexp.Regexp
	for _, p := range cfg.Dispatch.Patterns {
		r, err := regexp.Compile(`(?i)` + p.Match)
		if err != nil {
			return nil, fmt.Errorf("dispatch pattern regex %q: %w", p.Match, err)
		}
		patternRes = append(patternRes, r)
	}

	tmplStr := cfg.Template
	if tmplStr == "" {
		tmplStr = defaultTemplate
	}
	tmpl, err := template.New("prompt").Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}

	return &Monitor{
		cfg:        cfg,
		repo:       repo,
		matchRe:    matchRe,
		skipRes:    skipRes,
		patternRes: patternRes,
		seen:       make(map[string]time.Time),
		tmpl:       tmpl,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (m *Monitor) shouldSkip(line string) bool {
	for _, r := range m.skipRes {
		if r.MatchString(line) {
			return true
		}
	}
	return false
}

func fingerprint(service, line string) string {
	if len(line) > 120 {
		line = line[:120]
	}
	return service + "|" + line
}

var (
	reAuth    = regexp.MustCompile(`(?i)401|auth(entication)?[_\s]?(error|fail)|invalid.*(key|token|credential)`)
	reOOM     = regexp.MustCompile(`(?i)ENOSPC|no space left|out.of.memory|OOM|killed`)
	reCrash   = regexp.MustCompile(`(?i)panic:|fatal|segfault|SIGSEGV|exit status [^0]`)
	reTimeout = regexp.MustCompile(`(?i)timeout|deadline.*exceeded|context canceled`)
)

func classifyErrorType(lines []string) string {
	joined := strings.Join(lines, " ")
	switch {
	case reAuth.MatchString(joined):
		return "auth"
	case reOOM.MatchString(joined):
		return "oom"
	case reCrash.MatchString(joined):
		return "crash"
	case reTimeout.MatchString(joined):
		return "timeout"
	default:
		return "other"
	}
}

func (m *Monitor) deduped(service, line string) bool {
	fp := fingerprint(service, line)
	debounce := time.Duration(m.cfg.Dispatch.DebounceSecs) * time.Second
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.seen[fp]; ok && time.Since(t) < debounce {
		return true
	}
	m.seen[fp] = time.Now()
	return false
}

func (m *Monitor) throttled() bool {
	minInterval := time.Duration(m.cfg.Dispatch.MinIntervalSecs) * time.Second
	m.mu.Lock()
	defer m.mu.Unlock()
	if time.Since(m.lastDispatch) < minInterval {
		return true
	}
	m.lastDispatch = time.Now()
	return false
}

// ── Tiered dispatch ───────────────────────────────────────────────────────────

func (m *Monitor) dispatch(bucket *ErrorBucket) {
	start := time.Now()
	joined := strings.Join(bucket.Lines, "\n")

	// Tier 0 — pattern match → shell action
	for i, r := range m.patternRes {
		if r.MatchString(joined) {
			tier := "tier0"
			action := m.cfg.Dispatch.Patterns[i].Action
			fmt.Printf("[triage] Tier-0 pattern match %q → %s\n", m.cfg.Dispatch.Patterns[i].Match, action)
			metricDispatches.WithLabelValues(m.repo.Name, bucket.Service, tier).Inc()
			metricLastDispatch.WithLabelValues(m.repo.Name, bucket.Service).SetToCurrentTime()
			if !m.cfg.Dispatch.DryRun {
				cmd := exec.Command("sh", "-c", action)
				cmd.Dir = m.repo.Path
				out, err := cmd.CombinedOutput()
				outcome := "success"
				summary := fmt.Sprintf("Tier-0 pattern fix for %s:\n```\n%s\n```", bucket.Service, string(out))
				if err != nil {
					outcome = "failure"
					summary += fmt.Sprintf("\n(exit error: %v)", err)
				}
				metricDispatchOutcomes.WithLabelValues(m.repo.Name, bucket.Service, tier, outcome).Inc()
				m.notify(summary, "warn")
			}
			metricDispatchDuration.WithLabelValues(m.repo.Name, tier).Observe(time.Since(start).Seconds())
			return
		}
	}

	// Build prompt
	var buf bytes.Buffer
	data := TemplateData{
		Service:    bucket.Service,
		RepoPath:   m.repo.Path,
		RepoName:   m.repo.Name,
		ErrorLines: joined,
		Timestamp:  bucket.FirstSeen.UTC().Format(time.RFC3339),
	}
	if err := m.tmpl.Execute(&buf, data); err != nil {
		fmt.Printf("[triage] template error: %v\n", err)
		return
	}
	prompt := buf.String()

	if m.cfg.Dispatch.DryRun {
		fmt.Printf("[triage] DRY RUN — prompt for %s:\n%s\n", bucket.Service, prompt)
		return
	}

	tier := m.classifyTier(joined)
	if tier == 1 {
		m.runTier1(bucket, prompt, start)
	} else {
		m.runTier2(bucket, prompt, start)
	}
}

func (m *Monitor) classifyTier(errorText string) int {
	intentURL := m.cfg.Dispatch.IntentURL
	if intentURL == "" {
		if strings.Count(errorText, "\n") <= 3 {
			return 1
		}
		return 2
	}

	payload, _ := json.Marshal(map[string]string{"text": errorText})
	resp, err := m.httpClient.Post(intentURL, "application/json", bytes.NewReader(payload))
	if err != nil || resp.StatusCode != 200 {
		return 1
	}
	defer resp.Body.Close()

	var result struct {
		Complexity string `json:"complexity"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 1
	}
	if strings.EqualFold(result.Complexity, "high") {
		return 2
	}
	return 1
}

func (m *Monitor) runTier1(bucket *ErrorBucket, prompt string, start time.Time) {
	tier := "tier1"
	fmt.Printf("[triage] Tier-1 LLM fix for %s via gateway\n", bucket.Service)
	metricDispatches.WithLabelValues(m.repo.Name, bucket.Service, tier).Inc()
	metricLastDispatch.WithLabelValues(m.repo.Name, bucket.Service).SetToCurrentTime()

	gatewayURL := m.cfg.GatewayURL
	if gatewayURL == "" {
		fmt.Println("[triage] no gateway_url configured, escalating to Tier-2")
		m.runTier2(bucket, prompt, start)
		return
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"model": m.cfg.Model,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"max_tokens": 1024,
	})

	req, _ := http.NewRequest("POST", gatewayURL+"/v1/messages", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		fmt.Printf("[triage] Tier-1 gateway error: %v — escalating to Tier-2\n", err)
		metricDispatchOutcomes.WithLabelValues(m.repo.Name, bucket.Service, tier, "escalated").Inc()
		m.runTier2(bucket, prompt, start)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		fmt.Printf("[triage] Tier-1 gateway %d — escalating to Tier-2\n", resp.StatusCode)
		metricDispatchOutcomes.WithLabelValues(m.repo.Name, bucket.Service, tier, "escalated").Inc()
		m.runTier2(bucket, prompt, start)
		return
	}

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &result); err != nil || len(result.Content) == 0 {
		metricDispatchOutcomes.WithLabelValues(m.repo.Name, bucket.Service, tier, "escalated").Inc()
		m.runTier2(bucket, prompt, start)
		return
	}

	metricDispatchOutcomes.WithLabelValues(m.repo.Name, bucket.Service, tier, "success").Inc()
	summary := fmt.Sprintf("**Tier-1 fix** for `%s`:\n%s", bucket.Service, result.Content[0].Text)
	m.notify(summary, "info")
	metricDispatchDuration.WithLabelValues(m.repo.Name, tier).Observe(time.Since(start).Seconds())
	fmt.Printf("[triage] Tier-1 complete for %s\n", bucket.Service)
}

func (m *Monitor) runTier2(bucket *ErrorBucket, prompt string, start time.Time) {
	tier := "tier2"
	agentCmd := m.cfg.Dispatch.AgentCmd
	if agentCmd == "" {
		agentCmd = "claude"
	}
	fmt.Printf("[triage] Tier-2 agentic fix for %s via %s\n", bucket.Service, agentCmd)
	metricDispatches.WithLabelValues(m.repo.Name, bucket.Service, tier).Inc()
	metricLastDispatch.WithLabelValues(m.repo.Name, bucket.Service).SetToCurrentTime()

	cmd := exec.Command(agentCmd, "-p", prompt,
		"--allowedTools", "Bash,Read,Edit,Write",
	)
	cmd.Dir = m.repo.Path

	out, err := cmd.CombinedOutput()
	outcome := "success"
	summary := fmt.Sprintf("**Tier-2 agent fix** for `%s`:\n%s", bucket.Service, string(out))
	if err != nil {
		outcome = "failure"
		summary += fmt.Sprintf("\n(exit: %v)", err)
	}
	metricDispatchOutcomes.WithLabelValues(m.repo.Name, bucket.Service, tier, outcome).Inc()
	m.notify(summary, "error")
	metricDispatchDuration.WithLabelValues(m.repo.Name, tier).Observe(time.Since(start).Seconds())
}

// ── Discord notification ──────────────────────────────────────────────────────

func (m *Monitor) notify(text, level string) {
	fmt.Printf("[triage] %s\n---\n%s\n---\n", level, text)

	webhook := m.cfg.Notifications.DiscordWebhook
	if webhook == "" {
		return
	}

	color := 0x57F287
	switch level {
	case "warn":
		color = 0xFEE75C
	case "error":
		color = 0xED4245
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"embeds": []map[string]interface{}{
			{
				"title":       "TriageAI fix applied",
				"description": truncate(text, 4000),
				"color":       color,
				"timestamp":   time.Now().UTC().Format(time.RFC3339),
			},
		},
	})

	resp, err := m.httpClient.Post(webhook, "application/json", bytes.NewReader(payload))
	if err != nil {
		fmt.Printf("[triage] discord notify error: %v\n", err)
		return
	}
	resp.Body.Close()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

// ── Log streaming ─────────────────────────────────────────────────────────────

func (m *Monitor) Watch() {
	composePath := m.repo.ComposeFile
	if composePath == "" {
		composePath = "docker-compose.yml"
	}
	args := []string{"compose", "-f", composePath, "logs", "-f", "--tail", "0"}
	cmd := exec.Command("docker", args...)
	cmd.Dir = m.repo.Path

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("[triage:%s] stdout pipe: %v\n", m.repo.Name, err)
		return
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		fmt.Printf("[triage:%s] start docker compose: %v\n", m.repo.Name, err)
		return
	}

	pending := make(map[string]*ErrorBucket)
	flush := time.NewTicker(5 * time.Second)
	defer flush.Stop()

	scanner := bufio.NewScanner(stdout)
	lineCh := make(chan string, 256)

	go func() {
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
		close(lineCh)
	}()

	for {
		select {
		case line, ok := <-lineCh:
			if !ok {
				return
			}
			m.processLine(line, pending)

		case <-flush.C:
			for svc, bucket := range pending {
				if time.Since(bucket.FirstSeen) > 3*time.Second && len(bucket.Lines) > 0 {
					if !m.throttled() {
						go m.dispatch(bucket)
					}
					delete(pending, svc)
				}
			}
		}
	}
}

func (m *Monitor) processLine(raw string, pending map[string]*ErrorBucket) {
	// docker compose logs format (no flags): "service-1  | message"
	// with --timestamps: "2026-05-27T12:00:00.000000000Z service-1  | message"
	line := raw

	// Strip leading RFC3339Nano timestamp
	if len(line) > 30 && line[10] == 'T' {
		if idx := strings.IndexByte(line, ' '); idx > 10 && idx < 40 {
			line = line[idx+1:]
		}
	}

	// Parse service name from "service-1  | message" or "service-1 | message"
	service := "unknown"
	message := line
	var rawSvc string
	if idx := strings.Index(line, "  | "); idx > 0 {
		rawSvc = strings.TrimSpace(line[:idx])
		message = line[idx+4:]
	} else if idx := strings.Index(line, " | "); idx > 0 {
		rawSvc = strings.TrimSpace(line[:idx])
		message = line[idx+3:]
	}
	// Strip container numeric suffix: "gateway-go-1" → "gateway-go"
	if rawSvc != "" {
		if i := strings.LastIndexByte(rawSvc, '-'); i > 0 {
			suffix := rawSvc[i+1:]
			allDigits := len(suffix) > 0
			for _, c := range suffix {
				if c < '0' || c > '9' {
					allDigits = false
					break
				}
			}
			if allDigits {
				rawSvc = rawSvc[:i]
			}
		}
		service = rawSvc
	}

	if !m.matchRe.MatchString(message) {
		return
	}
	if m.shouldSkip(message) {
		return
	}
	if m.deduped(service, message) {
		return
	}

	fmt.Printf("[triage:%s] caught: %s\n", m.repo.Name, message)
	metricErrorsCaught.WithLabelValues(m.repo.Name, service).Inc()
	metricErrorsByType.WithLabelValues(m.repo.Name, service, classifyErrorType([]string{message})).Inc()

	bucket, ok := pending[service]
	if !ok {
		bucket = &ErrorBucket{
			Service:     service,
			FirstSeen:   time.Now(),
			Fingerprint: fingerprint(service, message),
		}
		pending[service] = bucket
	}
	if len(bucket.Lines) < 20 {
		bucket.Lines = append(bucket.Lines, message)
	}
}

// ── Commands ──────────────────────────────────────────────────────────────────

func cmdInit() {
	if _, err := os.Stat("triage.yaml"); err == nil {
		fmt.Println("triage.yaml already exists")
		return
	}

	wd, _ := os.Getwd()
	example := fmt.Sprintf(`gateway_url: "http://localhost:7878"
model: ""

repos:
  - path: %s
    name: my-stack
    compose_file: docker-compose.yml

filters:
  match: [WARN, WARNING, ERROR, FATAL, "panic:"]
  skip:
    - "task queue depth"
    - "Redis index race"
    - "keyword_search"
    - "DeadlineExceeded"
    - "stream copy"
    - "no changes to commit"

notifications:
  discord_webhook: ""

dispatch:
  patterns:
    - match: "401|authentication_error"
      action: "make refresh-token"
  intent_url: "http://localhost:8765/classify-intent"
  agent_cmd: "claude"
  debounce_secs: 120
  min_interval_secs: 30
  escalate_after_secs: 60
  dry_run: false
`, wd)

	if err := os.WriteFile("triage.yaml", []byte(example), 0644); err != nil {
		fmt.Printf("error writing triage.yaml: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Created triage.yaml — edit repos[] and notifications before running `triage watch`.")
}

func cmdWatch(cfgPath, metricsAddr string) {
	cfg := loadConfig(cfgPath)

	if len(cfg.Repos) == 0 {
		fmt.Println("No repos configured. Add repos[] to triage.yaml.")
		os.Exit(1)
	}

	// Start metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
			fmt.Fprint(w, `{"status":"ok"}`)
		})
		fmt.Printf("[triage] metrics at http://%s/metrics\n", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			fmt.Printf("[triage] metrics server error: %v\n", err)
		}
	}()

	var wg sync.WaitGroup
	for _, repo := range cfg.Repos {
		r := repo
		if r.Name == "" {
			r.Name = filepath.Base(r.Path)
		}
		mon, err := newMonitor(cfg, r)
		if err != nil {
			fmt.Printf("[triage] config error for repo %s: %v\n", r.Name, err)
			os.Exit(1)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Printf("[triage] watching %s (%s)\n", r.Name, r.Path)
			mon.Watch()
		}()
	}
	wg.Wait()
}

func cmdTest(cfgPath string) {
	cfg := loadConfig(cfgPath)
	if len(cfg.Repos) == 0 {
		fmt.Println("No repos in config.")
		os.Exit(1)
	}
	r := cfg.Repos[0]
	if r.Name == "" {
		r.Name = filepath.Base(r.Path)
	}
	mon, err := newMonitor(cfg, r)
	if err != nil {
		fmt.Printf("config error: %v\n", err)
		os.Exit(1)
	}
	mon.notify("TriageAI test notification — if you see this, Discord is wired up correctly.", "info")
	fmt.Println("Test notification sent.")
}

func loadConfig(path string) Config {
	cfg := defaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("cannot read %s: %v\n", path, err)
		os.Exit(1)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		fmt.Printf("cannot parse %s: %v\n", path, err)
		os.Exit(1)
	}
	if v := os.Getenv("TRIAGE_GATEWAY_URL"); v != "" {
		cfg.GatewayURL = v
	}
	if v := os.Getenv("TRIAGE_DISCORD_WEBHOOK"); v != "" {
		cfg.Notifications.DiscordWebhook = v
	} else if v := os.Getenv("DISCORD_WEBHOOK_URL"); v != "" {
		// Fall back to the shared Gabagool env var so you only need one secret
		cfg.Notifications.DiscordWebhook = v
	}
	if os.Getenv("TRIAGE_DRY_RUN") == "1" {
		cfg.Dispatch.DryRun = true
	}
	return cfg
}

// ── Entry point ───────────────────────────────────────────────────────────────

func usage() {
	fmt.Printf(`TriageAI %s — autonomous log monitor and repair agent

USAGE:
  triage [--config PATH] [--metrics-addr ADDR] <command>

COMMANDS:
  init    Create a starter triage.yaml in the current directory
  watch   Stream docker compose logs and dispatch fixes
  test    Send a test Discord notification

FLAGS:
  --config PATH          Path to config file (default: triage.yaml)
  --metrics-addr ADDR    Prometheus metrics listen address (default: 127.0.0.1:9112)
  --version              Print version

ENVIRONMENT:
  TRIAGE_GATEWAY_URL       Override gateway_url from config
  TRIAGE_DISCORD_WEBHOOK   Override notifications.discord_webhook
  TRIAGE_DRY_RUN=1         Print prompts without dispatching

`, Version)
}

func main() {
	cfgPath := "triage.yaml"
	metricsAddr := "127.0.0.1:9112"
	args := os.Args[1:]

	var filtered []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--config", "-config":
			if i+1 < len(args) {
				cfgPath = args[i+1]
				i++
			}
		case "--metrics-addr", "-metrics-addr":
			if i+1 < len(args) {
				metricsAddr = args[i+1]
				i++
			}
		case "--version", "-version":
			fmt.Printf("triage %s\n", Version)
			return
		case "--help", "-h":
			usage()
			return
		default:
			filtered = append(filtered, args[i])
		}
	}
	args = filtered

	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	switch args[0] {
	case "init":
		cmdInit()
	case "watch":
		cmdWatch(cfgPath, metricsAddr)
	case "test":
		cmdTest(cfgPath)
	default:
		fmt.Printf("unknown command: %s\n", args[0])
		usage()
		os.Exit(1)
	}
}
