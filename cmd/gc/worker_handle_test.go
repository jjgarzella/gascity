package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/convergence"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/shellquote"
	"github.com/gastownhall/gascity/internal/worker"
)

type failingSessionLookupStore struct {
	beads.Store
	err error
}

func (s *failingSessionLookupStore) Get(string) (beads.Bead, error) {
	return beads.Bead{}, s.err
}

func (s *failingSessionLookupStore) List(beads.ListQuery) ([]beads.Bead, error) {
	return nil, s.err
}

func TestWorkerHandleForSessionWithConfigUsesResolvedProviderOnFirstStart(t *testing.T) {
	skipSlowCmdGCTest(t, "waits through stale session-key detection; run make test-cmd-gc-process for full coverage")
	t.Run("fresh fallback prompt provider receipt", assertResolvedWorkerRuntimeFreshFallbackPromptProviderReceipt)
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
resume_flag = "--resume"
resume_style = "flag"
session_id_flag = "--session-id"
ready_prompt_prefix = "stub-ready>"
ready_delay_ms = 250

[providers.stub.env]
STUB_ENV = "present"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}

	sp := runtime.NewFake()
	mgr := newSessionManagerWithConfig(cityDir, store, sp, cfg)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{BeadOnly: true, Template: "worker", Title: "Probe", Command: "", WorkDir: t.TempDir(), Provider: "stub", Transport: "", Resume: session.ProviderResume{
		SessionIDFlag: "--old-session-id",
	}})
	if err != nil {
		t.Fatalf("CreateBeadOnly: %v", err)
	}
	if strings.TrimSpace(info.SessionKey) == "" {
		t.Fatal("SessionKey is empty")
	}

	handle, err := workerHandleForSessionWithConfig(cityDir, store, sp, cfg, info.ID)
	if err != nil {
		t.Fatalf("workerHandleForSessionWithConfig: %v", err)
	}
	if err := handle.Start(context.Background()); err != nil {
		t.Fatalf("handle.Start: %v", err)
	}

	start := sp.LastStartConfig(info.SessionName)
	if start == nil {
		t.Fatalf("LastStartConfig(%q) = nil", info.SessionName)
	}
	wantArg := "--session-id " + info.SessionKey
	if !strings.Contains(start.Command, "/bin/echo") || !strings.Contains(start.Command, wantArg) {
		t.Fatalf("start command = %q, want /bin/echo with %q", start.Command, wantArg)
	}
	if strings.Contains(start.Command, "--old-session-id") {
		t.Fatalf("start command = %q, still used stale session id flag", start.Command)
	}
	if start.ReadyPromptPrefix != "stub-ready>" {
		t.Fatalf("ReadyPromptPrefix = %q, want stub-ready>", start.ReadyPromptPrefix)
	}
	if start.ReadyDelayMs != 250 {
		t.Fatalf("ReadyDelayMs = %d, want 250", start.ReadyDelayMs)
	}
	if start.Env["STUB_ENV"] != "present" {
		t.Fatalf("Env[STUB_ENV] = %q, want present", start.Env["STUB_ENV"])
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesProviderLaunchCommand(t *testing.T) {
	cityDir := t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	claude := config.BuiltinProviders()["claude"]
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "claude",
		}},
		Providers: map[string]config.ProviderSpec{
			"claude": claude,
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if !strings.Contains(resolved.Command, "--dangerously-skip-permissions") {
		t.Fatalf("Command = %q, want unrestricted default", resolved.Command)
	}
	if !strings.Contains(resolved.Command, "--effort max") {
		t.Fatalf("Command = %q, want effort max default", resolved.Command)
	}
	if !strings.Contains(resolved.Command, "--settings") {
		t.Fatalf("Command = %q, want settings arg", resolved.Command)
	}
}

// TestResolvedWorkerRuntimeResumesPoolSessionPreservesLaunchFlags is a
// regression test for gastownhall/gascity#799: a pool-agent session
// resumed through the control-dispatcher path must reconstruct the full
// launch command (--dangerously-skip-permissions, --settings, schema
// defaults) even when the persisted session command is the bare
// provider name. The pre-fix path dropped those flags and caused pool
// workers resumed via `claude --resume <uuid>` to wedge on interactive
// permission prompts.
func TestResolvedWorkerRuntimeResumesPoolSessionPreservesLaunchFlags(t *testing.T) {
	cityDir := t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "settings.json"), []byte(`{"hooks":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	claude := config.BuiltinProviders()["claude"]
	maxActive := 3
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              "perspective_planner",
			Provider:          "claude",
			MaxActiveSessions: &maxActive,
		}},
		Providers: map[string]config.ProviderSpec{
			"claude": claude,
		},
	}

	// Simulate a pool-instance session bead whose persisted command is
	// the bare provider name — the shape produced before the April 2026
	// worker-boundary refactor when the API created the bead with
	// sessionCreateAgentCommand(resolved) before the reconciler synced
	// the full tp.Command.
	runtimeCfg, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "perspective_planner",
		Command:  "claude",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if runtimeCfg == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if !strings.Contains(runtimeCfg.Command, "--dangerously-skip-permissions") {
		t.Fatalf("resumed pool Command = %q, want --dangerously-skip-permissions", runtimeCfg.Command)
	}
	if !strings.Contains(runtimeCfg.Command, "--effort max") {
		t.Fatalf("resumed pool Command = %q, want --effort max default", runtimeCfg.Command)
	}
	if !strings.Contains(runtimeCfg.Command, "--settings") {
		t.Fatalf("resumed pool Command = %q, want --settings arg", runtimeCfg.Command)
	}
}

// TestResolvedWorkerRuntimeWithConfigSeedsCityRuntimeEnv is a regression
// test for upstream gastownhall/gascity#101 (re-opened): on session
// restart, the worker resolver reseeded the session env from
// resolved.Env (provider-only). That dropped the city-anchored env vars
// (GC_CITY, GC_CITY_PATH, GC_CITY_RUNTIME_DIR), so spawned/restarted
// agent sessions could not locate their city — bd commands failed,
// mailboxes resolved against the wrong path, and downstream tooling
// behaved as if no city was configured. The CLI-side defense in
// cmd/gc/main.go resolveContext (#2062) masked the symptom; this
// resolver-level fix is the root cause: the resolved runtime must
// always carry the city anchor vars so any restart path is sound.
func TestResolvedWorkerRuntimeWithConfigSeedsCityRuntimeEnv(t *testing.T) {
	cityDir := t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	claude := config.BuiltinProviders()["claude"]
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "claude",
		}},
		Providers: map[string]config.ProviderSpec{
			"claude": claude,
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}

	if got := resolved.SessionEnv["GC_CITY"]; got != cityDir {
		t.Errorf("SessionEnv[GC_CITY] = %q, want %q", got, cityDir)
	}
	if got := resolved.SessionEnv["GC_CITY_PATH"]; got != cityDir {
		t.Errorf("SessionEnv[GC_CITY_PATH] = %q, want %q", got, cityDir)
	}
	wantRuntimeDir := filepath.Join(cityDir, ".gc", "runtime")
	if got := resolved.SessionEnv["GC_CITY_RUNTIME_DIR"]; got != wantRuntimeDir {
		t.Errorf("SessionEnv[GC_CITY_RUNTIME_DIR] = %q, want %q", got, wantRuntimeDir)
	}
	// Identity-only contract (per Copilot review): the dispatcher trace
	// default must NOT be seeded by the resume reseed, because it has to
	// stay per-dispatcher-qualified (template_resolve.go owns the
	// qualified override). Seeding the city-uniform default here would
	// regress trace files for control-dispatcher sessions on restart.
	if got, present := resolved.SessionEnv["GC_CONTROL_DISPATCHER_TRACE_DEFAULT"]; present {
		t.Errorf("SessionEnv[GC_CONTROL_DISPATCHER_TRACE_DEFAULT] = %q present, want absent (identity-only reseed)", got)
	}
}

func TestResolvedWorkerRuntimeWithConfigIncludesProviderAuthPassthrough(t *testing.T) {
	cityDir := t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "test-anthropic-auth-token")
	t.Setenv("ANTHROPIC_BASE_URL", "https://ollama.example.test")
	t.Setenv("ANTHROPIC_DEFAULT_SONNET_MODEL", "kimi-k2.5")
	t.Setenv("CLAUDE_CODE_SUBAGENT_MODEL", "kimi-k2.5")
	t.Setenv("OLLAMA_API_KEY", "test-ollama-token")
	t.Setenv("GC_RIG", "caller-rig")
	t.Setenv("GC_SESSION_NAME", "caller-session")

	claude := config.BuiltinProviders()["claude"]
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "claude",
		}},
		Providers: map[string]config.ProviderSpec{
			"claude": claude,
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	for key, want := range map[string]string{
		"ANTHROPIC_AUTH_TOKEN":           "test-anthropic-auth-token",
		"ANTHROPIC_BASE_URL":             "https://ollama.example.test",
		"ANTHROPIC_DEFAULT_SONNET_MODEL": "kimi-k2.5",
		"CLAUDE_CODE_SUBAGENT_MODEL":     "kimi-k2.5",
		"OLLAMA_API_KEY":                 "test-ollama-token",
	} {
		if got := resolved.SessionEnv[key]; got != want {
			t.Errorf("SessionEnv[%s] = %q, want %q", key, got, want)
		}
		if got := resolved.Hints.Env[key]; got != want {
			t.Errorf("Hints.Env[%s] = %q, want %q", key, got, want)
		}
	}
	for _, key := range []string{"GC_RIG", "GC_SESSION_NAME"} {
		if got, ok := resolved.SessionEnv[key]; ok {
			t.Errorf("SessionEnv[%s] = %q, want absent caller context", key, got)
		}
		if got, ok := resolved.Hints.Env[key]; ok {
			t.Errorf("Hints.Env[%s] = %q, want absent caller context", key, got)
		}
	}
	if got := resolved.SessionEnv["GC_CITY"]; got != cityDir {
		t.Errorf("SessionEnv[GC_CITY] = %q, want %q", got, cityDir)
	}
	if got := resolved.Hints.Env["GC_CITY"]; got != cityDir {
		t.Errorf("Hints.Env[GC_CITY] = %q, want %q", got, cityDir)
	}
}

// TestResolvedWorkerRuntimeWithConfigCityAnchorsBeatConflictingProviderEnv
// pins the precedence contract: when the resolved provider env carries
// its own GC_CITY (e.g. left over from a stale pool entry, or a
// provider that hard-codes one), the city-anchored reseed must win.
// Without this assertion, future refactors could accidentally reverse
// the merge order and re-introduce upstream #101 from the other side.
func TestResolvedWorkerRuntimeWithConfigCityAnchorsBeatConflictingProviderEnv(t *testing.T) {
	cityDir := t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	claude := config.BuiltinProviders()["claude"]
	// Force a conflicting GC_CITY in the provider env so we can prove
	// the reseed wins. We can't reach into resolved.Env directly, so we
	// instead pin the worker's env on its own ProviderSpec via the
	// pool entry's runtime env section (which feeds resolved.Env).
	claude.Env = map[string]string{
		"GC_CITY":      "/wrong/city",
		"GC_CITY_PATH": "/wrong/city",
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "claude",
		}},
		Providers: map[string]config.ProviderSpec{
			"claude": claude,
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got := resolved.SessionEnv["GC_CITY"]; got != cityDir {
		t.Errorf("SessionEnv[GC_CITY] = %q, want %q (city anchor must win over provider env)", got, cityDir)
	}
	if got := resolved.SessionEnv["GC_CITY_PATH"]; got != cityDir {
		t.Errorf("SessionEnv[GC_CITY_PATH] = %q, want %q (city anchor must win over provider env)", got, cityDir)
	}
}

func TestResolvedWorkerRuntimeWithConfigSkipsCityAnchorsWhenCityPathEmpty(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "stub",
		}},
		Providers: map[string]config.ProviderSpec{
			"stub": {
				Command: "/bin/echo",
				Env: map[string]string{
					"GC_CITY":        "/provider/city",
					"PROVIDER_TOKEN": "ok",
				},
			},
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata("", cfg, session.Info{
		Template: "worker",
		WorkDir:  "/tmp/work",
	}, "", nil)
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if got := resolved.SessionEnv["GC_CITY"]; got != "/provider/city" {
		t.Fatalf("SessionEnv[GC_CITY] = %q, want provider value", got)
	}
	if got := resolved.SessionEnv["PROVIDER_TOKEN"]; got != "ok" {
		t.Fatalf("SessionEnv[PROVIDER_TOKEN] = %q, want ok", got)
	}
	if _, ok := resolved.SessionEnv["GC_CITY_PATH"]; ok {
		t.Fatalf("SessionEnv[GC_CITY_PATH] = %q, want absent when city path is empty", resolved.SessionEnv["GC_CITY_PATH"])
	}
	if _, ok := resolved.SessionEnv["GC_CITY_RUNTIME_DIR"]; ok {
		t.Fatalf("SessionEnv[GC_CITY_RUNTIME_DIR] = %q, want absent when city path is empty", resolved.SessionEnv["GC_CITY_RUNTIME_DIR"])
	}
}

func TestResolvedWorkerRuntimeWithConfigMergesWorkspaceEnv(t *testing.T) {
	cityDir := t.TempDir()
	cfg := &config.City{
		Workspace: config.Workspace{Env: map[string]string{
			"WORKSPACE_ONLY": "workspace",
			"SHARED_ENV":     "workspace",
			"BD_BIN":         "/tmp/pinned-bd",
		}},
		Agents: []config.Agent{{Name: "worker", Provider: "stub"}},
		Providers: map[string]config.ProviderSpec{
			"stub": {
				Command: "/bin/echo",
				Env: map[string]string{
					"PROVIDER_ONLY": "provider",
					"SHARED_ENV":    "provider",
				},
			},
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, session.Info{
		Template: "worker",
		WorkDir:  cityDir,
	}, "", nil)
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	for key, want := range map[string]string{
		"WORKSPACE_ONLY": "workspace",
		"BD_BIN":         "/tmp/pinned-bd",
		"PROVIDER_ONLY":  "provider",
		"SHARED_ENV":     "provider",
	} {
		if got := resolved.SessionEnv[key]; got != want {
			t.Errorf("SessionEnv[%s] = %q, want %q", key, got, want)
		}
		if got := resolved.Hints.Env[key]; got != want {
			t.Errorf("Hints.Env[%s] = %q, want %q", key, got, want)
		}
	}
}

// TestResolvedWorkerSessionConfigWithConfigSeedsCityAnchorsOnCreatePath
// covers the CLI session-create path (called by `gc session start` /
// `gc session new` etc. through newWorkerSessionHandleForResolvedRuntimeWithConfig).
// Before this fix, the create path passed resolved.Env directly as
// SessionEnv, so direct CLI creates landed without GC_CITY anchors —
// the same upstream #101 symptom as the resume path, just through a
// different door. Companion to the resume-path regression test above.
func TestResolvedWorkerSessionConfigWithConfigSeedsCityAnchorsOnCreatePath(t *testing.T) {
	cityDir := t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}

	cfg, err := resolvedWorkerSessionConfigWithConfig(
		cityDir,
		"",
		"",
		cityDir,
		"worker",
		"",
		"worker",
		"Worker",
		"",
		&config.ResolvedProvider{Name: "claude"},
		map[string]string{"session_origin": "test"},
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	env := cfg.Runtime.SessionEnv
	if got := env["GC_CITY"]; got != cityDir {
		t.Errorf("Runtime.SessionEnv[GC_CITY] = %q, want %q", got, cityDir)
	}
	if got := env["GC_CITY_PATH"]; got != cityDir {
		t.Errorf("Runtime.SessionEnv[GC_CITY_PATH] = %q, want %q", got, cityDir)
	}
	if env["GC_CITY_RUNTIME_DIR"] == "" {
		t.Error("Runtime.SessionEnv[GC_CITY_RUNTIME_DIR] = empty, want set")
	}
	if got, present := env["GC_CONTROL_DISPATCHER_TRACE_DEFAULT"]; present {
		t.Errorf("Runtime.SessionEnv[GC_CONTROL_DISPATCHER_TRACE_DEFAULT] = %q present, want absent (identity-only)", got)
	}
}

func TestResolvedWorkerSessionConfigWithConfigIncludesProviderAuthPassthrough(t *testing.T) {
	cityDir := t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "test-anthropic-auth-token")
	t.Setenv("ANTHROPIC_BASE_URL", "https://ollama.example.test")
	t.Setenv("ANTHROPIC_DEFAULT_SONNET_MODEL", "kimi-k2.5")
	t.Setenv("CLAUDE_CODE_SUBAGENT_MODEL", "kimi-k2.5")
	t.Setenv("OLLAMA_API_KEY", "test-ollama-token")
	t.Setenv("GC_RIG", "caller-rig")
	t.Setenv("GC_SESSION_NAME", "caller-session")

	cfg, err := resolvedWorkerSessionConfigWithConfig(
		cityDir,
		"",
		"",
		cityDir,
		"worker",
		"",
		"worker",
		"Worker",
		"",
		&config.ResolvedProvider{Name: "claude"},
		map[string]string{"session_origin": "test"},
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	env := cfg.Runtime.SessionEnv
	hintsEnv := cfg.Runtime.Hints.Env
	for key, want := range map[string]string{
		"ANTHROPIC_AUTH_TOKEN":           "test-anthropic-auth-token",
		"ANTHROPIC_BASE_URL":             "https://ollama.example.test",
		"ANTHROPIC_DEFAULT_SONNET_MODEL": "kimi-k2.5",
		"CLAUDE_CODE_SUBAGENT_MODEL":     "kimi-k2.5",
		"OLLAMA_API_KEY":                 "test-ollama-token",
	} {
		if got := env[key]; got != want {
			t.Errorf("Runtime.SessionEnv[%s] = %q, want %q", key, got, want)
		}
		if got := hintsEnv[key]; got != want {
			t.Errorf("Runtime.Hints.Env[%s] = %q, want %q", key, got, want)
		}
	}
	for _, key := range []string{"GC_RIG", "GC_SESSION_NAME"} {
		if got, ok := env[key]; ok {
			t.Errorf("Runtime.SessionEnv[%s] = %q, want absent caller context", key, got)
		}
		if got, ok := hintsEnv[key]; ok {
			t.Errorf("Runtime.Hints.Env[%s] = %q, want absent caller context", key, got)
		}
	}
	if got := env["GC_CITY"]; got != cityDir {
		t.Errorf("Runtime.SessionEnv[GC_CITY] = %q, want %q", got, cityDir)
	}
	if got := hintsEnv["GC_CITY"]; got != cityDir {
		t.Errorf("Runtime.Hints.Env[GC_CITY] = %q, want %q", got, cityDir)
	}
}

func TestShouldPreserveStoredRuntimeCommandForTransportRejectsExecutableOnlyMatch(t *testing.T) {
	if shouldPreserveStoredRuntimeCommandForTransport(
		"claude",
		"claude --settings /tmp/settings.json",
		"",
		nil,
	) {
		t.Fatal("shouldPreserveStoredRuntimeCommandForTransport() = true, want false")
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesStoredTemplateACPTransport(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"
session = "acp"

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)
	writeCatalogFile(t, cityDir, "mcp/filesystem.toml", `
name = "filesystem"
command = "/bin/mcp"
args = ["--stdio"]

[env]
TOKEN = "abc"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template:  "worker",
		Command:   "/bin/echo",
		Transport: "acp",
		WorkDir:   cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo acp"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
	if len(resolved.Hints.MCPServers) != 1 {
		t.Fatalf("Hints.MCPServers len = %d, want 1", len(resolved.Hints.MCPServers))
	}
	if got, want := resolved.Hints.MCPServers[0].Name, "filesystem"; got != want {
		t.Fatalf("Hints.MCPServers[0].Name = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigDoesNotInferConfiguredTransportWithoutStoredTemplateACPMetadata(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"
session = "acp"

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		Command:  "/bin/echo",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeTransportUsesResumeMetadataForLegacyACPWithSameCommand(t *testing.T) {
	resolved := &config.ResolvedProvider{
		Command:    "/bin/echo",
		ACPCommand: "/bin/echo",
	}

	got := resolvedWorkerRuntimeTransport(session.Info{
		Command: "/bin/echo",
	}, resolved, "acp", map[string]string{
		"resume_flag": "--resume",
	})
	if got != "acp" {
		t.Fatalf("resolvedWorkerRuntimeTransport() = %q, want acp", got)
	}
}

func TestResolvedWorkerRuntimeTransportUsesConfiguredTmuxForCommandOnlyBead(t *testing.T) {
	resolved := &config.ResolvedProvider{
		Name:        "kimi",
		Command:     "aimux",
		Args:        []string{"run", "kimi"},
		SupportsACP: true,
		ACPCommand:  "kimi-acp",
	}

	got := resolvedWorkerRuntimeTransport(session.Info{
		Template: "gascity/workflows.kimi",
		Provider: "kimi",
		Command:  "aimux run kimi -- --yolo --no-thinking --model kimi-k2.6",
	}, resolved, config.SessionTransportTmux, nil)
	if got != config.SessionTransportTmux {
		t.Fatalf("resolvedWorkerRuntimeTransport() = %q, want tmux", got)
	}
}

func TestResolvedWorkerRuntimeTransportUsesStoredACPCommandBeforeConfiguredTmux(t *testing.T) {
	resolved := &config.ResolvedProvider{
		Name:        "kimi",
		Command:     "aimux",
		Args:        []string{"run", "kimi"},
		SupportsACP: true,
		ACPCommand:  "kimi-acp",
		ACPArgs:     []string{"run", "kimi"},
	}

	got := resolvedWorkerRuntimeTransport(session.Info{
		Template: "gascity/workflows.kimi",
		Provider: "kimi",
		Command:  "kimi-acp run kimi --resume session-1",
	}, resolved, config.SessionTransportTmux, nil)
	if got != config.SessionTransportACP {
		t.Fatalf("resolvedWorkerRuntimeTransport() = %q, want acp", got)
	}
}

func TestResolvedWorkerRuntimeWithConfigErrorsForAmbiguousLegacyACPTransportWithSameCommand(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"
session = "acp"

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	_, err = resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		Command:  "/bin/echo",
		WorkDir:  cityDir,
	}, "")
	if err == nil || !strings.Contains(err.Error(), "legacy session transport is ambiguous") {
		t.Fatalf("resolvedWorkerRuntimeWithConfig() error = %v, want ambiguous legacy ACP transport", err)
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesStartedConfigHashForLegacyProviderACPWithSameCommand(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[providers.custom-acp]
command = "/bin/echo"
path_check = "true"
supports_acp = true
acp_command = "/bin/echo"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	cfg.PackMCPDir = filepath.Join(cityDir, "mcp")
	if err := os.MkdirAll(cfg.PackMCPDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(mcp): %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.PackMCPDir, "identity.template.toml"), []byte(`
name = "identity"
command = "/bin/mcp"
args = ["{{.AgentName}}"]
`), 0o644); err != nil {
		t.Fatalf("WriteFile(mcp): %v", err)
	}

	info := session.Info{
		Template: "custom-acp",
		Command:  "/bin/echo",
		Provider: "custom-acp",
		WorkDir:  cityDir,
	}
	resolved, _ := resolveWorkerRuntimeProviderWithConfig(cfg, info, "provider")
	mcpServers, err := resolvedRuntimeMCPServersWithConfig(
		cityDir,
		cfg,
		info.Alias,
		info.Template,
		info.Provider,
		info.WorkDir,
		"acp",
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedRuntimeMCPServersWithConfig: %v", err)
	}
	startedHash := runtime.CoreFingerprint(runtime.Config{
		Command:    resolved.ACPCommandString(),
		Env:        resolved.Env,
		MCPServers: mcpServers,
	})

	runtimeCfg, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, info, "provider", map[string]string{
		"started_config_hash": startedHash,
	})
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if runtimeCfg == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if len(runtimeCfg.Hints.MCPServers) != 1 {
		t.Fatalf("len(runtimeCfg.Hints.MCPServers) = %d, want 1", len(runtimeCfg.Hints.MCPServers))
	}
}

func TestResolveWorkerRuntimeProviderWithConfigProviderKindPrefersPersistedProvider(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Providers: map[string]config.ProviderSpec{
			"stored-provider": {
				Command: "true",
				Args:    []string{"stored"},
			},
			"template-provider": {
				Command: "true",
				Args:    []string{"template"},
			},
		},
	}
	info := session.Info{
		Template: "template-provider",
		Provider: "stored-provider",
	}

	resolved, _ := resolveWorkerRuntimeProviderWithConfig(cfg, info, "provider")
	if resolved == nil {
		t.Fatal("resolveWorkerRuntimeProviderWithConfig() = nil")
	}
	if got := resolved.Name; got != "stored-provider" {
		t.Fatalf("resolved.Name = %q, want stored-provider", got)
	}
}

func TestResolveWorkerRuntimeProviderWithConfigMetadataIdentifiesProviderSession(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{
			{Name: "worker", Dir: "myrig", Provider: "agent-provider"},
		},
		Providers: map[string]config.ProviderSpec{
			"stored-provider": {
				Command: "true",
				Args:    []string{"stored"},
			},
			"agent-provider": {
				Command: "true",
				Args:    []string{"agent"},
			},
		},
	}
	info := session.Info{
		Template: "myrig/worker",
		Provider: "stored-provider",
	}

	resolved, _ := resolveWorkerRuntimeProviderWithConfigAndMetadata(cfg, info, "", map[string]string{
		"session_origin": "manual",
	})
	if resolved == nil {
		t.Fatal("resolveWorkerRuntimeProviderWithConfigAndMetadata() = nil")
	}
	if got := resolved.Name; got != "stored-provider" {
		t.Fatalf("resolved.Name = %q, want stored-provider", got)
	}

	legacyAgentInfo := session.Info{
		Template: "myrig/worker",
		Provider: "agent-provider",
	}
	resolved, _ = resolveWorkerRuntimeProviderWithConfigAndMetadata(cfg, legacyAgentInfo, "", map[string]string{
		"session_origin": "manual",
	})
	if resolved == nil {
		t.Fatal("resolveWorkerRuntimeProviderWithConfigAndMetadata(legacy agent) = nil")
	}
	if got := resolved.Name; got != "agent-provider" {
		t.Fatalf("legacy agent resolved.Name = %q, want agent-provider", got)
	}

	resolved, _ = resolveWorkerRuntimeProviderWithConfigAndMetadata(cfg, info, "", map[string]string{
		"session_origin": "manual",
		"agent_name":     "myrig/worker",
	})
	if resolved == nil {
		t.Fatal("resolveWorkerRuntimeProviderWithConfigAndMetadata(agent) = nil")
	}
	if got := resolved.Name; got != "agent-provider" {
		t.Fatalf("agent resolved.Name = %q, want agent-provider", got)
	}
}

func workerPromptConfigFixture(t *testing.T, command, prompt string) (*config.City, session.Info) {
	t.Helper()
	cityDir := t.TempDir()
	promptPath := filepath.Join(cityDir, "prompts", "worker.md")
	if err := os.MkdirAll(filepath.Dir(promptPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(promptPath, []byte(prompt), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "prompt-city"},
		Agents: []config.Agent{{
			Name:           "worker",
			Provider:       "fake-codex",
			PromptTemplate: "prompts/worker.md",
		}},
		Providers: map[string]config.ProviderSpec{
			"fake-codex": {
				Command:           command,
				PromptMode:        "arg",
				ReadyPromptPrefix: "READY>",
			},
		},
	}
	info := session.Info{
		Template:            "worker",
		AgentName:           "worker",
		WorkDir:             cityDir,
		SessionName:         "prompt-worker",
		SessionNameMetadata: "prompt-worker",
	}
	return cfg, info
}

// workerPromptLaunchFixture records branch facts: these launch/file-ordering
// tests do not exercise Git discovery and must not invoke it for every row.
func workerPromptLaunchFixture(t *testing.T, prompt string) (*config.City, session.Info) {
	t.Helper()
	cfg, info := workerPromptConfigFixture(t, "/bin/echo", prompt)
	cfg.Agents[0].Dir = "prompt-rig"
	cfg.Rigs = []config.Rig{{Name: "prompt-rig", Path: info.WorkDir, DefaultBranch: "main"}}
	info.Template = cfg.Agents[0].QualifiedName()
	return cfg, info
}

func workerPromptFixture(t *testing.T, command, prompt string) (*config.City, session.Info, TemplateParams) {
	t.Helper()
	cfg, info := workerPromptConfigFixture(t, command, prompt)
	cityDir := info.WorkDir
	params := newAgentBuildParams(cfg.EffectiveCityName(), cityDir, cfg, nil, time.Now().UTC(), nil, nil)
	tp, err := resolveTemplateForSessionBeadInfo(params, &cfg.Agents[0], "worker", nil, info)
	if err != nil {
		t.Fatalf("resolveTemplateForSessionBeadInfo: %v", err)
	}
	return cfg, info, tp
}

func TestWorkerSessionCreateDeliversStartupPromptForFreshSessions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sessionIDFlag string
		prompt        string
		oversized     bool
	}{
		{name: "without provider key", prompt: "Fresh direct starts need the configured prompt."},
		{name: "with generated provider key", sessionIDFlag: "--session-id", prompt: "Fresh direct starts need the configured prompt."},
		{name: "oversized prompt uses nudge", prompt: strings.Repeat("x", maxPromptSuffixRawBytes), oversized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, fixtureInfo, rendered := workerPromptFixture(t, "/bin/echo", tc.prompt)
			providerSpec := cfg.Providers["fake-codex"]
			providerSpec.SessionIDFlag = tc.sessionIDFlag
			cfg.Providers["fake-codex"] = providerSpec
			resolved, err := config.ResolveProvider(&cfg.Agents[0], &cfg.Workspace, cfg.Providers, func(name string) (string, error) {
				return name, nil
			})
			if err != nil {
				t.Fatalf("resolve fake provider: %v", err)
			}

			sp := runtime.NewFake()
			handle, err := newWorkerSessionHandleForDirectCreateWithConfig(
				fixtureInfo.WorkDir,
				beads.NewMemStore(),
				sp,
				cfg,
				fixtureInfo.Alias,
				fixtureInfo.SessionName,
				fixtureInfo.Template,
				"direct startup",
				resolved.CommandString(),
				resolved.Name,
				fixtureInfo.WorkDir,
				"",
				resolved,
				map[string]string{"agent_name": fixtureInfo.AgentName},
			)
			if err != nil {
				t.Fatalf("build direct session handle: %v", err)
			}
			info, err := handle.Create(context.Background(), worker.CreateModeStarted)
			if err != nil {
				t.Fatalf("create direct session: %v", err)
			}
			started := sp.LastStartConfig(info.SessionName)
			if started == nil {
				t.Fatal("provider Start was not called")
			}
			if !strings.Contains(rendered.Prompt, tc.prompt) {
				t.Fatalf("rendered prompt omitted configured startup instructions (prompt bytes %d)", len(tc.prompt))
			}
			if tc.oversized {
				if started.PromptSuffix != "" || started.PromptFlag != "" || !strings.Contains(started.Nudge, tc.prompt) {
					t.Fatalf("oversized direct prompt route = suffix length %d flag %q nudge length %d; want nudge delivery", len(started.PromptSuffix), started.PromptFlag, len(started.Nudge))
				}
			} else if started.PromptSuffix == "" || started.PromptFlag != "" || started.Nudge != "" {
				t.Fatalf("direct start prompt route = suffix %q flag %q nudge %q; want argv delivery", started.PromptSuffix, started.PromptFlag, started.Nudge)
			}
			if got := started.Env[startupPromptDeliveredEnv]; got != "1" {
				t.Fatalf("direct startup delivery marker = %q, want 1", got)
			}
			if tc.sessionIDFlag != "" && !strings.Contains(started.Command, tc.sessionIDFlag) {
				t.Fatalf("fresh direct command = %q, want generated session-key flag %q", started.Command, tc.sessionIDFlag)
			}
		})
	}
}

// Direct handle construction and refused/deferred creates cannot prepare launch files.
func TestWorkerDirectCreateDefersPreparation(t *testing.T) {
	for _, operation := range []string{"construct", "deferred", "occupied", "orphan-blocked"} {
		for _, existing := range []bool{false, true} {
			for _, direct := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/existing=%t/direct=%t", operation, existing, direct), func(t *testing.T) {
					cfg, fixture := workerPromptLaunchFixture(t, "direct startup instructions")
					cfg.Agents[0].Provider = "claude"
					cfg.Providers["claude"] = config.ProviderSpec{Command: "/bin/echo", PromptMode: "arg"}
					cfg.Agents[0].InstallAgentHooks = []string{"claude", "codex"}
					resolved, err := config.ResolveProvider(&cfg.Agents[0], &cfg.Workspace, cfg.Providers, func(name string) (string, error) { return name, nil })
					if err != nil {
						t.Fatal(err)
					}
					overlay := t.TempDir()
					cfg.Agents[0].OverlayDir = overlay
					if err := os.WriteFile(filepath.Join(overlay, "direct-overlay.txt"), []byte("authorized overlay"), 0o644); err != nil {
						t.Fatal(err)
					}
					targets := []string{filepath.Join(fixture.WorkDir, ".gc/settings.json"), filepath.Join(fixture.WorkDir, ".codex/hooks.json"), filepath.Join(fixture.WorkDir, "direct-overlay.txt")}
					const sentinel = `{"sentinel":"preserve exact bytes"}`
					if existing {
						for _, target := range targets {
							if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(target, []byte(sentinel), 0o644); err != nil {
								t.Fatal(err)
							}
						}
					}
					store := beads.NewMemStore()
					sp := runtime.NewFake()
					var provider runtime.Provider = sp
					if operation == "orphan-blocked" {
						orphans := newProcessTableSweepProvider(runtime.LiveRuntime{SessionID: "gc-1", City: fixture.WorkDir, PID: 1234})
						orphans.terminateErr[1234] = fmt.Errorf("scripted unconfirmed orphan")
						provider = orphans
						sp = orphans.Fake
					}

					if operation == "occupied" {
						if _, err := store.Create(beads.Bead{Type: "session", Title: "occupied", Metadata: map[string]string{"session_name": "direct-purity", "state": "active", "template": fixture.Template}}); err != nil {
							t.Fatal(err)
						}
					}
					build := newWorkerSessionHandleForResolvedRuntimeWithConfig
					if direct {
						build = newWorkerSessionHandleForDirectCreateWithConfig
					}
					handle, err := build(fixture.WorkDir, store, provider, cfg, "", "direct-purity", fixture.Template, "title", resolved.CommandString(), resolved.Name, fixture.WorkDir, "", resolved, map[string]string{"agent_name": "worker"})
					if err != nil {
						t.Fatal(err)
					}
					switch operation {
					case "deferred":
						if _, err := handle.Create(context.Background(), worker.CreateModeDeferred); err != nil {
							t.Fatal(err)
						}
					case "occupied", "orphan-blocked":
						_, err := handle.Create(context.Background(), worker.CreateModeStarted)
						if err == nil {
							t.Fatal("ineligible create was not refused")
						}
						if operation == "orphan-blocked" && !strings.Contains(err.Error(), "orphan cleanup") {
							t.Fatalf("orphan refusal=%v", err)
						}
					}
					if sp.CountCalls("Start", "direct-purity") != 0 {
						t.Fatal("non-starting request called Provider.Start")
					}
					for _, target := range targets {
						data, err := os.ReadFile(target)
						if existing {
							if err != nil || string(data) != sentinel {
								t.Errorf("%s rewrote existing startup file %s: %q,%v", operation, target, data, err)
							}
						} else if !os.IsNotExist(err) {
							t.Errorf("%s generated startup file %s before authorized start: %d bytes,%v", operation, target, len(data), err)
						}
					}
				})
			}
		}
	}
}

func TestWorkerDirectCreateUsesConcretePromptContext(t *testing.T) {
	for _, tc := range []struct {
		name              string
		workspaceProvider string
		startCommand      bool
	}{
		{name: "named selected provider"},
		{name: "unnamed start command with different workspace", workspaceProvider: "fake-codex", startCommand: true},
		{name: "unnamed start command without workspace provider", startCommand: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, fixture := workerPromptLaunchFixture(t, "WORK={{.WorkDir}} ID={{.AgentName}} PROVIDER={{.ProviderKey}}")
			if err := os.Rename(filepath.Join(fixture.WorkDir, cfg.Agents[0].PromptTemplate), filepath.Join(fixture.WorkDir, "prompts/direct.template.md")); err != nil {
				t.Fatal(err)
			}
			cfg.Agents[0].PromptTemplate = "prompts/direct.template.md"
			cfg.Agents[0].WorkDir = filepath.Join(fixture.WorkDir, "configured-must-remain-unused")
			cfg.Workspace.Provider = tc.workspaceProvider
			selected := &config.Agent{Provider: "selected"}
			cfg.Providers["selected"] = config.ProviderSpec{Command: "/bin/echo", PromptMode: "flag", PromptFlag: "--prompt"}
			expectedProvider := "selected"
			if tc.startCommand {
				cfg.Agents[0].Provider = "claude"
				cfg.Agents[0].StartCommand = "/bin/echo"
				cfg.Agents[0].PromptMode = "flag"
				cfg.Agents[0].PromptFlag = "--prompt"
				selected = &cfg.Agents[0]
				expectedProvider = "claude"
			}
			configuredProvider := cfg.Agents[0].Provider
			resolved, err := config.ResolveProvider(selected, &cfg.Workspace, cfg.Providers, func(name string) (string, error) { return name, nil })
			if err != nil {
				t.Fatal(err)
			}
			if tc.startCommand && resolved.Name != "" {
				t.Fatalf("start_command provider unexpectedly named: %q", resolved.Name)
			}
			actualDir := filepath.Join(fixture.WorkDir, "selected-runtime-dir")
			sp := runtime.NewFake()
			handle, err := newWorkerSessionHandleForDirectCreateWithConfig(fixture.WorkDir, beads.NewMemStore(), sp, cfg, "", "direct-context", fixture.Template, "title", resolved.CommandString(), expectedProvider, actualDir, "", resolved, map[string]string{"agent_name": "concrete-owner"})
			if err != nil {
				t.Fatal(err)
			}
			info, err := handle.Create(context.Background(), worker.CreateModeStarted)
			if err != nil {
				t.Fatal(err)
			}
			launch := sp.LastStartConfig(info.SessionName)
			if launch == nil || launch.WorkDir != actualDir || launch.PromptFlag != "--prompt" {
				t.Fatalf("direct selected launch context=%#v", launch)
			}
			if info.Provider != expectedProvider {
				t.Fatalf("direct provider persisted=%q want=%q", info.Provider, expectedProvider)
			}
			for _, want := range []string{"WORK=" + actualDir, "ID=concrete-owner", "PROVIDER=" + info.Provider} {
				if !strings.Contains(launch.PromptSuffix, want) {
					t.Errorf("direct prompt omitted concrete context %q: %s", want, launch.PromptSuffix)
				}
			}
			if cfg.Agents[0].Provider != configuredProvider {
				t.Fatal("prompt resolution mutated template provider")
			}
			if _, err := os.Stat(cfg.Agents[0].WorkDir); !os.IsNotExist(err) {
				t.Fatalf("selected context prepared configured directory: %v", err)
			}
		})
	}
}

func TestResolvedWorkerRuntimeFreshFallbackPromptDelivery(t *testing.T) {
	cfg, info, rendered := workerPromptFixture(t, "/bin/echo", "Fresh bootstrap instructions.")
	if !strings.Contains(rendered.Prompt, "Fresh bootstrap instructions.") {
		t.Fatalf("rendered prompt = %q, want configured startup instructions", rendered.Prompt)
	}

	fresh, err := resolvedWorkerRuntimeWithConfig(info.WorkDir, cfg, info, "")
	if err != nil {
		t.Fatalf("resolve fresh fallback: %v", err)
	}
	if fresh.Hints.PromptSuffix == "" || fresh.Hints.PromptFlag != "" || fresh.Hints.Nudge != "" {
		t.Fatalf("fresh prompt route = suffix %q flag %q nudge %q; want argv delivery", fresh.Hints.PromptSuffix, fresh.Hints.PromptFlag, fresh.Hints.Nudge)
	}
	if got := fresh.Hints.Env[startupPromptDeliveredEnv]; got != "1" {
		t.Fatalf("fresh delivery marker = %q, want 1 so SessionStart does not duplicate the prompt", got)
	}

	resumeInfo := info
	resumeInfo.SessionKey = "provider-session-key"
	resumed, err := resolvedWorkerRuntimeWithConfig(info.WorkDir, cfg, resumeInfo, "")
	if err != nil {
		t.Fatalf("resolve valid resume: %v", err)
	}
	if resumed.Hints.PromptSuffix != "" || resumed.Hints.PromptFlag != "" || resumed.Hints.Nudge != "" {
		t.Fatalf("valid resume received startup prompt route: suffix %q flag %q nudge %q", resumed.Hints.PromptSuffix, resumed.Hints.PromptFlag, resumed.Hints.Nudge)
	}
	if got := resumed.Hints.Env[startupPromptDeliveredEnv]; got != "" {
		t.Fatalf("valid resume delivery marker = %q, want empty", got)
	}
}

// Session observation and rejected lifecycle work must leave startup files alone.
func TestWorkerFreshFallbackDoesNotPrepareFilesBeforeStart(t *testing.T) {
	for _, operation := range []string{"observe", "refused attach", "stale conditional kill"} {
		t.Run(operation, func(t *testing.T) {
			cfg, fixture, _ := workerPromptFixture(t, "/bin/echo", "read-only bootstrap")
			cityDir := fixture.WorkDir
			cfg.Agents[0].Provider = "claude"
			cfg.Providers["claude"] = config.ProviderSpec{Command: "/bin/echo", PromptMode: "arg"}
			cfg.Agents[0].InstallAgentHooks = []string{"claude", "codex"}
			overlay := t.TempDir()
			cfg.Agents[0].OverlayDir = overlay
			const sentinel = "startup-overlay.txt"
			if err := os.WriteFile(filepath.Join(overlay, sentinel), []byte("authorized start overlay"), 0o644); err != nil {
				t.Fatal(err)
			}
			store := beads.NewMemStore()
			sp := runtime.NewFake()
			mgr := session.NewManagerWithOptions(store, sp)
			info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
				BeadOnly: true, Template: "worker", Command: "/bin/echo", WorkDir: cityDir, Provider: "claude",
				ExtraMeta: map[string]string{"agent_name": "worker", "session_origin": "manual"},
			})
			if err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "observe":
				_, err = workerObserveSessionTargetWithRuntimeHintsWithConfig(cityDir, store, sp, cfg, info.ID, nil)
			case "refused attach":
				if err := store.SetMetadataBatch(info.ID, map[string]string(session.DrainAckStopPendingPatch(time.Now().UTC()))); err != nil {
					t.Fatal(err)
				}
				handle, buildErr := workerHandleForSessionWithConfig(cityDir, store, sp, cfg, info.ID)
				if buildErr != nil {
					t.Fatal(buildErr)
				}
				if attachErr := handle.Attach(context.Background()); attachErr == nil {
					t.Fatal("draining session attach succeeded")
				}
			case "stale conditional kill":
				var stopped bool
				stopped, err = workerKillSessionIfCurrentWithConfig(cityDir, store, sp, cfg, info.ID, runtime.Identity{SessionID: "stale-session", RuntimeEpoch: "1"})
				if stopped {
					t.Fatal("stale conditional kill stopped a runtime")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, rel := range []string{sentinel, ".gc/settings.json", ".codex/hooks.json"} {
				if b, err := os.ReadFile(filepath.Join(cityDir, rel)); !os.IsNotExist(err) {
					t.Errorf("%s prepared %s before start: bytes=%q err=%v", operation, rel, b, err)
				}
			}
			if sp.CountCalls("Start", info.SessionName) != 0 || sp.CountCalls("Stop", info.SessionName) != 0 {
				t.Fatal("observation or rejected lifecycle work invoked runtime start/stop")
			}
			// The resolved hints still carry legitimate workdir staging to the start boundary.
			resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, info, "")
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.StageSessionWorkDir(resolved.Hints); err != nil {
				t.Fatal(err)
			}
			if b, err := os.ReadFile(filepath.Join(cityDir, sentinel)); err != nil || string(b) != "authorized start overlay" {
				t.Fatalf("start staging lost overlay: bytes=%q err=%v", b, err)
			}
		})
	}
}

func TestResolvedWorkerRuntimeFreshFallbackUsesPersistedPromptContext(t *testing.T) {
	for _, persistedAtRoot := range []bool{true, false} {
		t.Run(fmt.Sprintf("persisted at scope root=%t", persistedAtRoot), func(t *testing.T) {
			cfg, info, _ := workerPromptFixture(t, "/bin/echo", "WORKDIR={{.WorkDir}} IDENTITY={{.AgentName}} {{define \"directory-fragment\"}}FRAGMENT={{.WorkDir}}{{end}}")
			cityDir := info.WorkDir
			if err := os.Rename(filepath.Join(cityDir, cfg.Agents[0].PromptTemplate), filepath.Join(cityDir, "prompts/context.template.md")); err != nil {
				t.Fatal(err)
			}
			cfg.Agents[0].PromptTemplate = "prompts/context.template.md"
			cfg.Session.Provider = "subprocess"
			cfg.PackSkillsDir = filepath.Join(cityDir, "skills")
			cfg.Agents[0].Provider = "codex"
			cfg.Agents[0].Scope = "city"
			cfg.Agents[0].InjectFragments = []string{"directory-fragment"}
			cfg.Providers["codex"] = config.ProviderSpec{Command: "/bin/echo", PromptMode: "arg"}
			otherDir := filepath.Join(cityDir, "session-worktree")
			if err := os.MkdirAll(otherDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if persistedAtRoot {
				cfg.Agents[0].WorkDir = otherDir
			} else {
				cfg.Agents[0].WorkDir = cityDir
				info.WorkDir = otherDir
			}
			for rel, content := range map[string]string{
				"pack.toml":            "[pack]\nname = \"skills\"\nversion = \"0.1.0\"\nschema = 2\n",
				"skills/plan/SKILL.md": "---\nname: plan\ndescription: Plan the work\n---\nbody\n",
			} {
				path := filepath.Join(cityDir, rel)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, info, "")
			if err != nil {
				t.Fatal(err)
			}
			if resolved.WorkDir != info.WorkDir || resolved.Hints.WorkDir != info.WorkDir {
				t.Fatalf("runtime directory changed: runtime=%q hints=%q persisted=%q", resolved.WorkDir, resolved.Hints.WorkDir, info.WorkDir)
			}
			for _, want := range []string{"WORKDIR=" + info.WorkDir, "FRAGMENT=" + info.WorkDir, "IDENTITY=" + info.AgentName} {
				if !strings.Contains(resolved.Hints.PromptSuffix, want) {
					t.Errorf("prompt missing persisted context %q: %s", want, resolved.Hints.PromptSuffix)
				}
			}
			// Subprocess only receives stage-1 skills at the scope root; no stage-2 hook runs.
			if got := strings.Contains(resolved.Hints.PromptSuffix, "`plan` — Plan the work"); got != persistedAtRoot {
				t.Errorf("assigned-skills context present=%t, want %t for persisted workdir %q", got, persistedAtRoot, info.WorkDir)
			}
		})
	}
}

func TestResolvedWorkerRuntimeFreshFallbackOversizedPromptUsesNudge(t *testing.T) {
	cfg, info, _ := workerPromptFixture(t, "/bin/echo", strings.Repeat("x", maxPromptSuffixRawBytes))
	resolved, err := resolvedWorkerRuntimeWithConfig(info.WorkDir, cfg, info, "")
	if err != nil {
		t.Fatalf("resolve oversized fresh fallback: %v", err)
	}
	if resolved.Hints.PromptSuffix != "" || resolved.Hints.PromptFlag != "" {
		t.Fatalf("oversized prompt used argv: suffix length %d flag %q", len(resolved.Hints.PromptSuffix), resolved.Hints.PromptFlag)
	}
	if !strings.Contains(resolved.Hints.Nudge, strings.Repeat("x", maxPromptSuffixRawBytes)) {
		t.Fatalf("oversized prompt nudge length = %d, want configured prompt bytes", len(resolved.Hints.Nudge))
	}
	if got := resolved.Hints.Env[startupPromptDeliveredEnv]; got != "1" {
		t.Fatalf("oversized nudge delivery marker = %q, want 1", got)
	}
}

func assertResolvedWorkerRuntimeFreshFallbackPromptProviderReceipt(t *testing.T) {
	t.Helper()
	cityDir := t.TempDir()
	fakeCodex := filepath.Join(cityDir, "fake-codex")
	argsReceipt := filepath.Join(cityDir, "provider-argv")
	acceptedReceipt := filepath.Join(cityDir, "provider-accepted")
	stdinReceipt := filepath.Join(cityDir, "provider-stdin")
	markerReceipt := filepath.Join(cityDir, "provider-delivery-marker")
	const directInstruction = "Fake Codex must receive the rendered startup prompt."
	script := `#!/bin/sh
set -eu
printf '%s\0' "$@" > "$GC_PROVIDER_ARGV"
last=
for arg do last=$arg; done
if [ -n "$last" ]; then printf 'accepted' > "$GC_PROVIDER_ACCEPTED"; else printf 'rejected' > "$GC_PROVIDER_ACCEPTED"; fi
: > "$GC_PROVIDER_STDIN"
printf '%s' "${GC_STARTUP_PROMPT_DELIVERED:-}" > "$GC_PROVIDER_MARKER"
printf 'READY>\n'
IFS= read -r ignored || true
`
	if err := os.WriteFile(fakeCodex, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, info, rendered := workerPromptFixture(t, fakeCodex, directInstruction)
	if !strings.Contains(rendered.Prompt, directInstruction) {
		t.Fatalf("rendered prompt = %q, want configured fake-provider instruction", rendered.Prompt)
	}
	resolved, err := resolvedWorkerRuntimeWithConfig(info.WorkDir, cfg, info, "")
	if err != nil {
		t.Fatalf("resolve fresh fallback runtime: %v", err)
	}
	if resolved.Hints.PromptSuffix == "" || resolved.Hints.Nudge != "" {
		t.Fatalf("selected fresh prompt route = suffix length %d, nudge length %d; want argv route", len(resolved.Hints.PromptSuffix), len(resolved.Hints.Nudge))
	}
	if got := resolved.Hints.Env[startupPromptDeliveredEnv]; got != "1" {
		t.Fatalf("startup delivery marker = %q, want 1", got)
	}
	expectedArgs := shellquote.Split(resolved.Hints.PromptSuffix)
	if len(expectedArgs) != 1 {
		t.Fatalf("decode selected prompt suffix %q: args=%q", resolved.Hints.PromptSuffix, expectedArgs)
	}

	launch := resolved.Hints
	launch.Command = resolved.Command
	launch.WorkDir = info.WorkDir
	launch.Env = make(map[string]string, len(resolved.Hints.Env)+4)
	for key, value := range resolved.Hints.Env {
		launch.Env[key] = value
	}
	launch.Env["GC_PROVIDER_ARGV"] = argsReceipt
	launch.Env["GC_PROVIDER_ACCEPTED"] = acceptedReceipt
	launch.Env["GC_PROVIDER_STDIN"] = stdinReceipt
	launch.Env["GC_PROVIDER_MARKER"] = markerReceipt
	socket := fmt.Sprintf("gcprompt-%d-%x", os.Getpid(), time.Now().UnixNano())
	provider, err := newSessionProviderForCityByName(cfg, "tmux", config.SessionConfig{
		Socket:       socket,
		SetupTimeout: "5s",
	}, cfg.Workspace.Name, info.WorkDir)
	if err != nil {
		t.Fatalf("construct tmux session provider: %v", err)
	}
	teardown, ok := provider.(interface{ TeardownServer() error })
	if !ok {
		t.Fatalf("tmux session provider %T does not expose server teardown", provider)
	}
	const sessionName = "fresh-prompt-receipt"
	t.Cleanup(func() {
		_ = provider.Stop(sessionName)
		_ = teardown.TeardownServer()
	})
	if err := provider.Start(context.Background(), sessionName, launch); err != nil {
		t.Fatalf("provider Start: %v", err)
	}

	argv, err := os.ReadFile(argsReceipt)
	if err != nil {
		t.Fatalf("read provider argv receipt: %v", err)
	}
	args := bytes.Split(bytes.TrimSuffix(argv, []byte{0}), []byte{0})
	if len(args) == 0 || string(args[len(args)-1]) != expectedArgs[0] {
		t.Fatalf("fake Codex final argv = %q, want rendered prompt argument %q", args, expectedArgs[0])
	}
	accepted, err := os.ReadFile(acceptedReceipt)
	if err != nil || string(accepted) != "accepted" {
		t.Fatalf("fake Codex acceptance receipt = %q, %v; want accepted", accepted, err)
	}
	stdin, err := os.ReadFile(stdinReceipt)
	if err != nil || len(stdin) != 0 {
		t.Fatalf("fake Codex stdin receipt = %q, %v; want empty for argv delivery", stdin, err)
	}
	marker, err := os.ReadFile(markerReceipt)
	if err != nil || string(marker) != "1" {
		t.Fatalf("fake Codex startup delivery marker = %q, %v; want 1", marker, err)
	}

	// Exercise the direct CLI create path through the same fake process. This
	// catches cases where the resolver builds a prompt route but the actual
	// worker creation path fails to pass it to Provider.Start.
	directArgsReceipt := filepath.Join(cityDir, "direct-provider-argv")
	directAcceptedReceipt := filepath.Join(cityDir, "direct-provider-accepted")
	directStdinReceipt := filepath.Join(cityDir, "direct-provider-stdin")
	directMarkerReceipt := filepath.Join(cityDir, "direct-provider-delivery-marker")
	providerSpec := cfg.Providers["fake-codex"]
	providerSpec.SessionIDFlag = "--session-id"
	providerSpec.Env = map[string]string{
		"GC_PROVIDER_ARGV":     directArgsReceipt,
		"GC_PROVIDER_ACCEPTED": directAcceptedReceipt,
		"GC_PROVIDER_STDIN":    directStdinReceipt,
		"GC_PROVIDER_MARKER":   directMarkerReceipt,
	}
	cfg.Providers["fake-codex"] = providerSpec
	directResolved, err := config.ResolveProvider(&cfg.Agents[0], &cfg.Workspace, cfg.Providers, func(name string) (string, error) {
		return name, nil
	})
	if err != nil {
		t.Fatalf("resolve direct-create fake provider: %v", err)
	}
	directHandle, err := newWorkerSessionHandleForDirectCreateWithConfig(
		info.WorkDir,
		beads.NewMemStore(),
		provider,
		cfg,
		info.Alias,
		"direct-prompt-receipt",
		info.Template,
		"direct startup receipt",
		directResolved.CommandString(),
		directResolved.Name,
		info.WorkDir,
		"",
		directResolved,
		map[string]string{"agent_name": info.AgentName},
	)
	if err != nil {
		t.Fatalf("build direct-create worker handle: %v", err)
	}
	if _, err := directHandle.Create(context.Background(), worker.CreateModeStarted); err != nil {
		t.Fatalf("create direct fake-provider session: %v", err)
	}
	directArgv, err := os.ReadFile(directArgsReceipt)
	if err != nil {
		t.Fatalf("read direct-create provider argv receipt: %v", err)
	}
	directArgs := bytes.Split(bytes.TrimSuffix(directArgv, []byte{0}), []byte{0})
	if len(directArgs) < 3 || string(directArgs[0]) != "--session-id" || len(directArgs[1]) == 0 {
		t.Fatalf("direct fake Codex argv = %q, want generated --session-id key before the prompt", directArgs)
	}
	foundPrompt := false
	for _, arg := range directArgs {
		if strings.Contains(string(arg), directInstruction) {
			foundPrompt = true
			break
		}
	}
	if !foundPrompt {
		t.Fatalf("direct fake Codex argv = %q, want rendered startup prompt instruction %q", directArgs, directInstruction)
	}
	directAccepted, err := os.ReadFile(directAcceptedReceipt)
	if err != nil || string(directAccepted) != "accepted" {
		t.Fatalf("direct fake Codex acceptance receipt = %q, %v; want accepted", directAccepted, err)
	}
	directMarker, err := os.ReadFile(directMarkerReceipt)
	if err != nil || string(directMarker) != "1" {
		t.Fatalf("direct fake Codex startup delivery marker = %q, %v; want 1", directMarker, err)
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesCurrentAgentProviderOverStaleSessionProvider(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{
			{Name: "worker", Dir: "myrig", Provider: "agent-provider"},
		},
		Providers: map[string]config.ProviderSpec{
			"stale-provider": {
				Command: "true",
				Args:    []string{"stale"},
			},
			"agent-provider": {
				Command: "true",
				Args:    []string{"agent"},
			},
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata("", cfg, session.Info{
		Template: "myrig/worker",
		Provider: "stale-provider",
		WorkDir:  "/tmp/work",
	}, "", map[string]string{
		"session_origin": "ephemeral",
		"agent_name":     "myrig/worker",
	})
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if got := resolved.Provider; got != "agent-provider" {
		t.Fatalf("resolved.Provider = %q, want agent-provider", got)
	}
}

func TestResolvedWorkerRuntimeWithConfigKeepsDefaultTransportWithoutExplicitACPTemplate(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		Command:  "/bin/echo",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesStoredACPTransportForProviderSession(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[providers.opencode]
command = "/bin/echo"
path_check = "true"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template:  "opencode",
		Command:   "/bin/echo",
		Transport: "acp",
		WorkDir:   cityDir,
	}, "provider")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo acp"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesStoredACPTransportForLegacyProviderSessionWithoutMetadata(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[providers.opencode]
command = "/bin/echo"
path_check = "true"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "opencode",
		Command:  "/bin/echo acp",
		WorkDir:  cityDir,
	}, "provider")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo acp"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesStoredACPTransportForLegacyProviderSessionOnACPEnabledCustomProvider(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[providers.custom-acp]
command = "/bin/echo"
path_check = "true"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "custom-acp",
		Command:  "/bin/echo acp",
		WorkDir:  cityDir,
	}, "provider")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo acp"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesProviderACPDefaultForAgentTemplateWithoutSessionOverride(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
dir = "myrig"
provider = "custom-acp"

[providers.custom-acp]
command = "/bin/echo"
path_check = "true"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "myrig/worker",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo acp"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigReplaysTemplateOverridesOnResume(t *testing.T) {
	cityDir := t.TempDir()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Dir:      "myrig",
			Provider: "custom",
		}},
		Providers: map[string]config.ProviderSpec{
			"custom": {
				Command:       "/bin/echo",
				ResumeCommand: "/bin/echo --resume {{.SessionKey}} --effort low",
				ResumeFlag:    "--resume",
				ResumeStyle:   "flag",
				PathCheck:     "true",
				OptionsSchema: []config.ProviderOption{{
					Key:  "effort",
					Type: "select",
					Choices: []config.OptionChoice{{
						Value:    "high",
						FlagArgs: []string{"--effort", "high"},
					}, {
						Value:    "low",
						FlagArgs: []string{"--effort", "low"},
					}},
				}},
			},
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, session.Info{
		Template:   "myrig/worker",
		Command:    "/bin/echo",
		WorkDir:    cityDir,
		SessionKey: "abc-123",
	}, "", map[string]string{
		"template_overrides": `{"effort":"high","initial_message":"hello"}`,
	})
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if got, want := resolved.Command, "/bin/echo --effort high"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
	if got, want := resolved.Resume.ResumeCommand, "/bin/echo --resume {{.SessionKey}} --effort high"; got != want {
		t.Fatalf("Resume.ResumeCommand = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigFallsBackToStoredCommandWhenTemplateOverridesInvalid(t *testing.T) {
	cityDir := t.TempDir()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Dir:      "myrig",
			Provider: "custom",
		}},
		Providers: map[string]config.ProviderSpec{
			"custom": {
				Command:   "/bin/echo",
				PathCheck: "true",
			},
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, session.Info{
		Template: "myrig/worker",
		Command:  "/bin/echo --stored",
		WorkDir:  cityDir,
	}, "", map[string]string{
		"template_overrides": `{`,
	})
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if got, want := resolved.Command, "/bin/echo --stored"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

func TestWorkerHandleForSessionWithConfigUsesResolvedProviderOnResume(t *testing.T) {
	skipSlowCmdGCTest(t, "waits through stale session-key detection; run make test-cmd-gc-process for full coverage")
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
resume_flag = "--resume"
resume_style = "flag"
session_id_flag = "--session-id"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}

	sp := runtime.NewFake()
	mgr := newSessionManagerWithConfig(cityDir, store, sp, cfg)
	info, err := mgr.CreateSession(
		context.Background(), session.CreateOptions{Template: "worker", Title: "Probe", Command: "legacy-agent", WorkDir: t.TempDir(), Provider: "stub", Env: nil, Resume: session.ProviderResume{
			ResumeFlag:    "--old-resume",
			ResumeStyle:   "flag",
			SessionIDFlag: "--session-id",
		}, Hints: runtime.Config{}, ExtraMeta: map[string]string{"session_origin": "manual"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("Suspend: %v", err)
	}

	handle, err := workerHandleForSessionWithConfig(cityDir, store, sp, cfg, info.ID)
	if err != nil {
		t.Fatalf("workerHandleForSessionWithConfig: %v", err)
	}

	sp.Calls = nil
	if err := handle.Start(context.Background()); err != nil {
		t.Fatalf("handle.Start: %v", err)
	}

	start := sp.LastStartConfig(info.SessionName)
	if start == nil {
		t.Fatalf("LastStartConfig(%q) = nil", info.SessionName)
	}
	wantArg := "--resume " + info.SessionKey
	if !strings.Contains(start.Command, "/bin/echo") || !strings.Contains(start.Command, wantArg) {
		t.Fatalf("start command = %q, want /bin/echo with %q", start.Command, wantArg)
	}
	if strings.Contains(start.Command, "--old-resume") {
		t.Fatalf("start command = %q, still used stale resume flag", start.Command)
	}
}

func TestWorkerHandleForSessionTargetWithConfigResolvesSessionName(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
resume_flag = "--resume"
resume_style = "flag"
session_id_flag = "--session-id"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}

	sp := runtime.NewFake()
	mgr := newSessionManagerWithConfig(cityDir, store, sp, cfg)
	info, err := mgr.CreateSession(
		context.Background(), session.CreateOptions{Template: "worker", Title: "Probe", Command: "", WorkDir: t.TempDir(), Provider: "stub", Env: nil, Resume: session.ProviderResume{ResumeFlag: "--resume", ResumeStyle: "flag", SessionIDFlag: "--session-id"}, Hints: runtime.Config{}, ExtraMeta: map[string]string{"session_origin": "manual"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	handle, err := workerHandleForSessionTargetWithConfig(cityDir, store, sp, cfg, info.SessionName)
	if err != nil {
		t.Fatalf("workerHandleForSessionTargetWithConfig: %v", err)
	}
	if err := handle.Kill(context.Background()); err != nil {
		t.Fatalf("handle.Kill: %v", err)
	}
	// The live event recorder now wired into the CLI factory (#5859) makes
	// Kill's worker.operation telemetry re-enrich session identity after the
	// runtime mutation, which issues a trailing IsRunning probe — so the Stop
	// call is no longer guaranteed to be last. Find it instead of assuming it.
	stop, ok := lastRuntimeCallByMethod(sp.Calls, "Stop")
	if !ok || stop.Name != info.SessionName {
		t.Fatalf("runtime calls = %#v, want a Stop %q", sp.Calls, info.SessionName)
	}
}

// lastRuntimeCallByMethod returns the last call matching method, if any.
func lastRuntimeCallByMethod(calls []runtime.Call, method string) (runtime.Call, bool) {
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Method == method {
			return calls[i], true
		}
	}
	return runtime.Call{}, false
}

// TestWorkerObserveSessionTargetWithConfigDoesNotFetchSessionBeadMoreThanTwice
// guards the dedup invariant. The Observe path used to load the same session
// bead five times per call (resolve, factory.Get, factory metadata Get,
// LiveObservation Get, ObserveRuntime Get); this PR collapses that to two:
// once for ResolveSessionBeadByExactID and once for LiveObservation's
// freshness re-load. Each redundant fetch is a `bd show` CLI fork on real
// (non-mem) stores, so the supervisor's nudge poll loop pays for every Get
// directly in idle-city CPU.
func TestWorkerObserveSessionTargetWithConfigDoesNotFetchSessionBeadMoreThanTwice(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	backing, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	sp := runtime.NewFake()
	mgr := newSessionManagerWithConfig(cityDir, backing, sp, cfg)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{Template: "worker", Title: "Probe", Command: "/bin/echo", WorkDir: t.TempDir(), Provider: "stub", Env: nil, Resume: session.ProviderResume{}, Hints: runtime.Config{Command: "/bin/echo"}, ExtraMeta: map[string]string{"session_origin": "manual"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	store := &sessionGetSpyStore{Store: backing}
	obs, err := workerObserveSessionTargetWithConfig(cityDir, store, sp, cfg, info.ID)
	if err != nil {
		t.Fatalf("workerObserveSessionTargetWithConfig: %v", err)
	}
	if !obs.Running {
		t.Fatalf("obs.Running = false, want true after Create started runtime")
	}

	var hits int
	for _, id := range store.getIDs {
		if id == info.ID {
			hits++
		}
	}
	if hits > 2 {
		t.Fatalf("store.Get(%q) called %d times, want at most 2; all Get IDs: %v", info.ID, hits, store.getIDs)
	}
}

func TestWorkerObserveSessionTargetWithConfigFallsBackToRunningRuntimeHandle(t *testing.T) {
	sp := runtime.NewFake()
	if err := sp.Start(context.Background(), "mayor", runtime.Config{Command: "echo"}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := &config.City{
		Workspace: config.Workspace{Name: "city"},
		Agents: []config.Agent{
			{Name: "mayor", MaxActiveSessions: intPtr(1)},
		},
	}

	target := cliSessionName("/home/user/city", cfg.Workspace.Name, "mayor", cfg.Workspace.SessionTemplate)
	obs, err := workerObserveSessionTargetWithConfig("/home/user/city", nil, sp, cfg, target)
	if err != nil {
		t.Fatalf("workerObserveSessionTargetWithConfig: %v", err)
	}
	if !obs.Running {
		t.Fatalf("obs.Running = false, want true for %q", target)
	}
}

func TestWorkerObserveSessionTargetWithConfigIgnoresStoreLookupFailuresForRuntimeFallback(t *testing.T) {
	sp := runtime.NewFake()
	if err := sp.Start(context.Background(), "mayor", runtime.Config{Command: "echo"}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg := &config.City{
		Workspace: config.Workspace{Name: "city"},
		Agents: []config.Agent{
			{Name: "mayor", MaxActiveSessions: intPtr(1)},
		},
	}

	target := cliSessionName("/home/user/city", cfg.Workspace.Name, "mayor", cfg.Workspace.SessionTemplate)
	store := &failingSessionLookupStore{err: fmt.Errorf("store lookup failed")}
	obs, err := workerObserveSessionTargetWithConfig("/home/user/city", store, sp, cfg, target)
	if err != nil {
		t.Fatalf("workerObserveSessionTargetWithConfig: %v", err)
	}
	if !obs.Running {
		t.Fatalf("obs.Running = false, want true for %q when runtime session is live", target)
	}
}

func TestWorkerKillSessionTargetWithConfigResolvesRuntimeSessionMeta(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	sp := runtime.NewFake()
	mgr := newSessionManagerWithConfig(cityDir, store, sp, cfg)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{Template: "worker", Title: "Probe", Command: "stub", WorkDir: t.TempDir(), Provider: "stub", Env: nil, Resume: session.ProviderResume{}, Hints: runtime.Config{}, ExtraMeta: map[string]string{"session_origin": "manual"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := workerKillSessionTargetWithConfig(cityDir, store, sp, cfg, info.SessionName); err != nil {
		t.Fatalf("workerKillSessionTargetWithConfig: %v", err)
	}
	// See TestWorkerHandleForSessionTargetWithConfigResolvesSessionName: the
	// live recorder wired into the CLI factory (#5859) adds a trailing
	// IsRunning enrichment probe after Kill, so Stop is no longer last.
	stop, ok := lastRuntimeCallByMethod(sp.Calls, "Stop")
	if !ok || stop.Name != info.SessionName {
		t.Fatalf("runtime calls = %#v, want a Stop %q", sp.Calls, info.SessionName)
	}
}

func TestWorkerDeliveryIntentForSubmitIntent(t *testing.T) {
	tests := []struct {
		name   string
		intent session.SubmitIntent
		want   worker.DeliveryIntent
	}{
		{name: "default", intent: session.SubmitIntentDefault, want: worker.DeliveryIntentDefault},
		{name: "follow up", intent: session.SubmitIntentFollowUp, want: worker.DeliveryIntentFollowUp},
		{name: "interrupt now", intent: session.SubmitIntentInterruptNow, want: worker.DeliveryIntentInterruptNow},
		{name: "empty defaults", intent: "", want: worker.DeliveryIntentDefault},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workerDeliveryIntentForSubmitIntent(tt.intent); got != tt.want {
				t.Fatalf("workerDeliveryIntentForSubmitIntent(%q) = %q, want %q", tt.intent, got, tt.want)
			}
		})
	}
}

func TestWorkerNudgeDeliveryForMode(t *testing.T) {
	tests := []struct {
		name string
		mode nudgeDeliveryMode
		want worker.NudgeDelivery
		ok   bool
	}{
		{name: "immediate", mode: nudgeDeliveryImmediate, want: worker.NudgeDeliveryImmediate, ok: true},
		{name: "wait idle", mode: nudgeDeliveryWaitIdle, want: worker.NudgeDeliveryWaitIdle, ok: true},
		{name: "queue", mode: nudgeDeliveryQueue, want: "", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := workerNudgeDeliveryForMode(tt.mode)
			if ok != tt.ok {
				t.Fatalf("workerNudgeDeliveryForMode(%q) ok = %v, want %v", tt.mode, ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("workerNudgeDeliveryForMode(%q) = %q, want %q", tt.mode, got, tt.want)
			}
		})
	}
}

func TestResolvedWorkerSessionConfigWithConfigFallsBackToResolvedProviderNameForCommand(t *testing.T) {
	cfg, err := resolvedWorkerSessionConfigWithConfig(
		"",
		"",
		"",
		"/tmp/work",
		"worker",
		"",
		"worker",
		"Worker",
		"",
		&config.ResolvedProvider{
			Name: "custom-provider",
		},
		map[string]string{"session_origin": "test"},
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	if got, want := cfg.Runtime.Command, "custom-provider"; got != want {
		t.Fatalf("Runtime.Command = %q, want %q", got, want)
	}
	if got, want := cfg.Runtime.Provider, "custom-provider"; got != want {
		t.Fatalf("Runtime.Provider = %q, want %q", got, want)
	}
}

func TestResolvedWorkerSessionConfigWithConfigFallsBackToProviderArgForCommand(t *testing.T) {
	cfg, err := resolvedWorkerSessionConfigWithConfig(
		"",
		"",
		"legacy-provider",
		"/tmp/work",
		"worker",
		"",
		"worker",
		"Worker",
		"",
		&config.ResolvedProvider{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	if got, want := cfg.Runtime.Command, "legacy-provider"; got != want {
		t.Fatalf("Runtime.Command = %q, want %q", got, want)
	}
	if got, want := cfg.Runtime.Provider, "legacy-provider"; got != want {
		t.Fatalf("Runtime.Provider = %q, want %q", got, want)
	}
}

func TestResolvedWorkerSessionConfigWithConfigPersistsStoredMCPMetadata(t *testing.T) {
	cfg, err := resolvedWorkerSessionConfigWithConfig(
		"",
		"",
		"legacy-provider",
		"/tmp/work",
		"worker",
		"",
		"worker",
		"Worker",
		"acp",
		&config.ResolvedProvider{
			Name: "custom-provider",
		},
		map[string]string{
			"session_origin": "test",
			"agent_name":     "myrig/worker-adhoc-123",
		},
		[]runtime.MCPServerConfig{{
			Name:      "filesystem",
			Transport: runtime.MCPTransportStdio,
			Command:   "/bin/mcp",
			Args:      []string{"--stdio"},
		}},
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	if got, want := cfg.Metadata[session.MCPIdentityMetadataKey], "myrig/worker-adhoc-123"; got != want {
		t.Fatalf("Metadata[mcp_identity] = %q, want %q", got, want)
	}
	if got := cfg.Metadata[session.MCPServersSnapshotMetadataKey]; got == "" {
		t.Fatal("Metadata[mcp_servers_snapshot] = empty, want persisted snapshot")
	}
}

func TestResolvedWorkerSessionConfigWithConfigSkipsStoredMCPMetadataForTmuxTransport(t *testing.T) {
	cfg, err := resolvedWorkerSessionConfigWithConfig(
		"",
		"",
		"legacy-provider",
		"/tmp/work",
		"worker",
		"",
		"worker",
		"Worker",
		"",
		&config.ResolvedProvider{
			Name: "custom-provider",
		},
		map[string]string{
			"session_origin": "test",
			"agent_name":     "myrig/worker-adhoc-123",
		},
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	if got := cfg.Metadata[session.MCPIdentityMetadataKey]; got != "" {
		t.Fatalf("Metadata[mcp_identity] = %q, want empty for tmux transport", got)
	}
	if got := cfg.Metadata[session.MCPServersSnapshotMetadataKey]; got != "" {
		t.Fatalf("Metadata[mcp_servers_snapshot] = %q, want empty for tmux transport", got)
	}
}

func TestResolvedWorkerRuntimeWithConfigFallsBackToCityPathAndSyncsHintsWorkDir(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
ready_prompt_prefix = "stub-ready>"
ready_delay_ms = 250
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	runtimeCfg, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if runtimeCfg == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := runtimeCfg.WorkDir, cityDir; got != want {
		t.Fatalf("WorkDir = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Hints.WorkDir, cityDir; got != want {
		t.Fatalf("Hints.WorkDir = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Provider, "stub"; got != want {
		t.Fatalf("Provider = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Command, "/bin/echo"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
}

// TestResolvedWorkerRuntimeWithConfigPopulatesSessionLiveOnResume guards the
// ga-vtkhi fix: the `gc session attach` resume path must carry the agent's
// session_live commands (with templates expanded) into Hints.SessionLive so
// the recreated tmux runtime re-applies the status-bar/keybinding theme the
// same way reconciler-started sessions do.
func TestResolvedWorkerRuntimeWithConfigPopulatesSessionLiveOnResume(t *testing.T) {
	cityDir := t.TempDir()
	rigDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, fmt.Sprintf(`[workspace]
name = "test-city"

[beads]
provider = "file"

[[rigs]]
name = "myrig"
path = %q

[[agent]]
name = "worker"
provider = "stub"
session_live = ["theme apply {{.Session}}"]

[[agent]]
name = "rig-worker"
dir = "myrig"
provider = "stub"
session_live = ["theme rig={{.Rig}} root={{.RigRoot}} base={{.AgentBase}} city={{.CityName}} work={{.WorkDir}} config={{.ConfigDir}}"]

[[agent]]
name = "polecat"
dir = "myrig"
provider = "stub"
session_live = ["theme agent={{.Agent}} base={{.AgentBase}} rig={{.Rig}} root={{.RigRoot}}"]

[[agent]]
name = "plain"
provider = "stub"

[providers.stub]
command = "/bin/echo"
`, rigDir))

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	tests := []struct {
		name string
		info session.Info
		want []string
	}{
		{
			name: "session name template",
			info: session.Info{
				Template:    "worker",
				AgentName:   "worker",
				SessionName: "test-city__worker",
			},
			want: []string{"theme apply test-city__worker"},
		},
		{
			name: "rig template context",
			info: session.Info{
				Template:    "myrig/rig-worker",
				AgentName:   "myrig/rig-worker",
				SessionName: "test-city__myrig__rig-worker",
				WorkDir:     filepath.Join(rigDir, "agents", "rig-worker"),
			},
			want: []string{"theme rig=myrig root=" + rigDir + " base=rig-worker city=test-city work=" + filepath.Join(rigDir, "agents", "rig-worker") + " config=" + cityDir},
		},
		{
			name: "pool instance uses concrete agent name",
			info: session.Info{
				Template:    "myrig/polecat",
				AgentName:   "myrig/polecat__furiosa-1",
				SessionName: "test-city__myrig__polecat__furiosa-1",
			},
			want: []string{"theme agent=myrig/polecat__furiosa-1 base=polecat__furiosa-1 rig=myrig root=" + rigDir},
		},
		{
			name: "missing session live stays empty",
			info: session.Info{
				Template:    "plain",
				AgentName:   "plain",
				SessionName: "test-city__plain",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtimeCfg, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, tt.info, "")
			if err != nil {
				t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
			}
			if runtimeCfg == nil {
				t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
			}
			if got, want := runtimeCfg.Hints.SessionLive, tt.want; !slicesEqual(got, want) {
				t.Fatalf("Hints.SessionLive = %v, want %v", got, want)
			}
		})
	}
}

func TestResolvedWorkerRuntimeWithConfigIgnoresMCPResolutionErrorForACPResume(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"
session = "acp"

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)
	writeCatalogFile(t, cityDir, "mcp/filesystem.toml", `
name = "filesystem"
command = [broken
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template:  "worker",
		Transport: "acp",
		WorkDir:   cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo acp"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
	if len(resolved.Hints.MCPServers) != 0 {
		t.Fatalf("Hints.MCPServers len = %d, want 0", len(resolved.Hints.MCPServers))
	}
}

func TestResolvedWorkerRuntimeWithConfigIgnoresMCPResolutionErrorWithoutACPTransport(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
`)
	writeCatalogFile(t, cityDir, "mcp/filesystem.toml", `
name = "filesystem"
command = [broken
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "worker",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if got, want := resolved.Command, "/bin/echo"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
	if len(resolved.Hints.MCPServers) != 0 {
		t.Fatalf("Hints.MCPServers len = %d, want 0", len(resolved.Hints.MCPServers))
	}
}

func TestResolvedWorkerRuntimeWithConfigUsesStoredAgentNameForResumeMCPMaterialization(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "ant"
dir = "myrig"
provider = "stub"
session = "acp"
work_dir = ".gc/worktrees/{{.Rig}}/ants/{{.AgentBase}}"
min_active_sessions = 0
max_active_sessions = 4

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)
	writeCatalogFile(t, cityDir, "mcp/identity.template.toml", `
name = "identity"
command = "/bin/mcp"
args = ["{{.AgentName}}", "{{.WorkDir}}", "{{.TemplateName}}"]
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	workDir := filepath.Join(cityDir, ".gc", "worktrees", "myrig", "ants", "ant")
	resolved, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template:  "myrig/ant",
		Alias:     "ant",
		AgentName: "myrig/ant-adhoc-123",
		Transport: "acp",
		WorkDir:   workDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}
	if len(resolved.Hints.MCPServers) != 1 {
		t.Fatalf("Hints.MCPServers len = %d, want 1", len(resolved.Hints.MCPServers))
	}
	if got, want := resolved.Hints.MCPServers[0].Args[0], "myrig/ant-adhoc-123"; got != want {
		t.Fatalf("Args[0] = %q, want %q", got, want)
	}
	if got, want := resolved.Hints.MCPServers[0].Args[1], workDir; got != want {
		t.Fatalf("Args[1] = %q, want %q", got, want)
	}
	if got, want := resolved.Hints.MCPServers[0].Args[2], "myrig/ant"; got != want {
		t.Fatalf("Args[2] = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigFallsBackToStoredMCPServersWhenCatalogBreaks(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "ant"
dir = "myrig"
provider = "stub"
session = "acp"
work_dir = ".gc/worktrees/{{.Rig}}/ants/{{.AgentBase}}"
min_active_sessions = 0
max_active_sessions = 4

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)
	writeCatalogFile(t, cityDir, "mcp/identity.template.toml", `
name = "identity"
command = [broken
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	workDir := filepath.Join(cityDir, ".gc", "worktrees", "myrig", "ants", "ant")
	metadata, err := session.WithStoredMCPMetadata(nil, "myrig/ant-adhoc-123", []runtime.MCPServerConfig{{
		Name:      "identity",
		Transport: runtime.MCPTransportStdio,
		Command:   "/bin/mcp",
		Args:      []string{"myrig/ant-adhoc-123", workDir, "myrig/ant"},
	}})
	if err != nil {
		t.Fatalf("WithStoredMCPMetadata: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, session.Info{
		Template:  "myrig/ant",
		Alias:     "ant",
		AgentName: "myrig/ant-adhoc-123",
		Transport: "acp",
		WorkDir:   workDir,
	}, "", metadata)
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if len(resolved.Hints.MCPServers) != 1 {
		t.Fatalf("Hints.MCPServers len = %d, want 1", len(resolved.Hints.MCPServers))
	}
	if got, want := resolved.Hints.MCPServers[0].Args[0], "myrig/ant-adhoc-123"; got != want {
		t.Fatalf("Args[0] = %q, want %q", got, want)
	}
	if got, want := resolved.Hints.MCPServers[0].Args[1], workDir; got != want {
		t.Fatalf("Args[1] = %q, want %q", got, want)
	}
	if got, want := resolved.Hints.MCPServers[0].Args[2], "myrig/ant"; got != want {
		t.Fatalf("Args[2] = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigFallsBackToRuntimeMCPServersSnapshotWhenCatalogBreaks(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "ant"
dir = "myrig"
provider = "stub"
session = "acp"
work_dir = ".gc/worktrees/{{.Rig}}/ants/{{.AgentBase}}"
min_active_sessions = 0
max_active_sessions = 4

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)
	writeCatalogFile(t, cityDir, "mcp/identity.template.toml", `
name = "identity"
command = [broken
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	workDir := filepath.Join(cityDir, ".gc", "worktrees", "myrig", "ants", "ant")
	servers := []runtime.MCPServerConfig{{
		Name:      "identity",
		Transport: runtime.MCPTransportHTTP,
		Command:   "/bin/mcp",
		Args:      []string{"--api-key", "super-secret"},
		Env: map[string]string{
			"API_TOKEN": "super-secret",
		},
		URL: "https://user:pass@example.invalid/mcp?token=abc123",
		Headers: map[string]string{
			"Authorization": "Bearer secret",
		},
	}}
	metadata, err := session.WithStoredMCPMetadata(nil, "myrig/ant-adhoc-123", servers)
	if err != nil {
		t.Fatalf("WithStoredMCPMetadata: %v", err)
	}
	if err := session.PersistRuntimeMCPServersSnapshot(cityDir, "sess-1", servers); err != nil {
		t.Fatalf("PersistRuntimeMCPServersSnapshot: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, session.Info{
		ID:        "sess-1",
		Template:  "myrig/ant",
		Alias:     "ant",
		AgentName: "myrig/ant-adhoc-123",
		Transport: "acp",
		WorkDir:   workDir,
	}, "", metadata)
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if len(resolved.Hints.MCPServers) != 1 {
		t.Fatalf("Hints.MCPServers len = %d, want 1", len(resolved.Hints.MCPServers))
	}
	if got, want := resolved.Hints.MCPServers[0].Args[1], "super-secret"; got != want {
		t.Fatalf("Args[1] = %q, want %q", got, want)
	}
	if got, want := resolved.Hints.MCPServers[0].Env["API_TOKEN"], "super-secret"; got != want {
		t.Fatalf("Env[API_TOKEN] = %q, want %q", got, want)
	}
	if got, want := resolved.Hints.MCPServers[0].Headers["Authorization"], "Bearer secret"; got != want {
		t.Fatalf("Headers[Authorization] = %q, want %q", got, want)
	}
}

func TestResolvedWorkerRuntimeWithConfigFallsBackToSanitizedStoredMCPServersWhenRuntimeSnapshotMissing(t *testing.T) {
	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "ant"
dir = "myrig"
provider = "stub"
session = "acp"
work_dir = ".gc/worktrees/{{.Rig}}/ants/{{.AgentBase}}"
min_active_sessions = 0
max_active_sessions = 4

[providers.stub]
command = "/bin/echo"
supports_acp = true
acp_command = "/bin/echo"
acp_args = ["acp"]
`)
	writeCatalogFile(t, cityDir, "mcp/identity.template.toml", `
name = "identity"
command = [broken
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	workDir := filepath.Join(cityDir, ".gc", "worktrees", "myrig", "ants", "ant")
	metadata, err := session.WithStoredMCPMetadata(nil, "myrig/ant-adhoc-123", []runtime.MCPServerConfig{{
		Name:      "identity",
		Transport: runtime.MCPTransportHTTP,
		Command:   "/bin/mcp",
		Args:      []string{"--serve", "--api-key", "super-secret"},
		Env: map[string]string{
			"API_TOKEN": "super-secret",
		},
		URL: "https://user:pass@example.invalid/mcp?token=abc123",
		Headers: map[string]string{
			"Authorization": "Bearer secret",
		},
	}})
	if err != nil {
		t.Fatalf("WithStoredMCPMetadata: %v", err)
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, session.Info{
		ID:        "sess-1",
		Template:  "myrig/ant",
		Alias:     "ant",
		AgentName: "myrig/ant-adhoc-123",
		Transport: "acp",
		WorkDir:   workDir,
	}, "", metadata)
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	if len(resolved.Hints.MCPServers) != 1 {
		t.Fatalf("Hints.MCPServers len = %d, want 1", len(resolved.Hints.MCPServers))
	}
	if got, want := resolved.Hints.MCPServers[0].Args, []string{"--serve"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("Args = %#v, want %#v", got, want)
	}
	if len(resolved.Hints.MCPServers[0].Env) != 0 {
		t.Fatalf("Env = %#v, want empty", resolved.Hints.MCPServers[0].Env)
	}
	if len(resolved.Hints.MCPServers[0].Headers) != 0 {
		t.Fatalf("Headers = %#v, want empty", resolved.Hints.MCPServers[0].Headers)
	}
	if got, want := resolved.Hints.MCPServers[0].URL, "https://example.invalid/mcp"; got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func TestWorkerSessionRuntimeResolverWithConfigFallsBackToProviderNameWhenResolvedCommandMissing(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "stub",
		}},
		Providers: map[string]config.ProviderSpec{
			"stub": {},
		},
	}

	resolver := workerSessionRuntimeResolverWithConfig(t.TempDir(), cfg)
	if resolver == nil {
		t.Fatal("workerSessionRuntimeResolverWithConfig() = nil")
	}

	runtimeCfg, err := resolver(session.Info{Template: "worker"}, "", nil)
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	if runtimeCfg == nil {
		t.Fatal("resolver() = nil")
	}
	if got, want := runtimeCfg.Command, "stub"; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Provider, "stub"; got != want {
		t.Fatalf("Provider = %q, want %q", got, want)
	}
}

func TestWorkerSessionRuntimeResolverWithConfigFallsBackToPersistedRuntimeOnIncompleteResolvedConfig(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "stub",
		}},
		Providers: map[string]config.ProviderSpec{
			"stub": {
				ReadyPromptPrefix: "resolved-ready>",
				ReadyDelayMs:      321,
			},
		},
	}

	resolver := workerSessionRuntimeResolverWithConfig(t.TempDir(), cfg)
	if resolver == nil {
		t.Fatal("workerSessionRuntimeResolverWithConfig() = nil")
	}

	info := session.Info{
		Template:      "worker",
		Command:       "persisted-worker --dangerously-skip-permissions",
		Provider:      "persisted-provider",
		WorkDir:       "/tmp/persisted-workdir",
		ResumeFlag:    "--resume-persisted",
		ResumeStyle:   "subcommand",
		ResumeCommand: "persisted resume {{.SessionKey}}",
	}

	runtimeCfg, err := resolver(info, "", nil)
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	if runtimeCfg == nil {
		t.Fatal("resolver() = nil")
	}
	if got, want := runtimeCfg.Command, info.Command; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Provider, info.Provider; got != want {
		t.Fatalf("Provider = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.WorkDir, info.WorkDir; got != want {
		t.Fatalf("WorkDir = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Resume.ResumeFlag, info.ResumeFlag; got != want {
		t.Fatalf("Resume.ResumeFlag = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Resume.ResumeStyle, info.ResumeStyle; got != want {
		t.Fatalf("Resume.ResumeStyle = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Resume.ResumeCommand, info.ResumeCommand; got != want {
		t.Fatalf("Resume.ResumeCommand = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Hints.WorkDir, info.WorkDir; got != want {
		t.Fatalf("Hints.WorkDir = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Hints.ReadyPromptPrefix, "resolved-ready>"; got != want {
		t.Fatalf("Hints.ReadyPromptPrefix = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Hints.ReadyDelayMs, 321; got != want {
		t.Fatalf("Hints.ReadyDelayMs = %d, want %d", got, want)
	}
}

func TestWorkerSessionRuntimeResolverWithConfigFallsBackToPersistedProviderWhenCommandMissing(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "resolved-provider",
		}},
		Providers: map[string]config.ProviderSpec{
			"resolved-provider": {
				ReadyPromptPrefix: "resolved-ready>",
			},
		},
	}

	resolver := workerSessionRuntimeResolverWithConfig(t.TempDir(), cfg)
	if resolver == nil {
		t.Fatal("workerSessionRuntimeResolverWithConfig() = nil")
	}

	info := session.Info{
		Template: "worker",
		Provider: "persisted-provider",
	}

	runtimeCfg, err := resolver(info, "", nil)
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	if runtimeCfg == nil {
		t.Fatal("resolver() = nil")
	}
	if got, want := runtimeCfg.Command, info.Provider; got != want {
		t.Fatalf("Command = %q, want %q", got, want)
	}
	if got, want := runtimeCfg.Provider, info.Provider; got != want {
		t.Fatalf("Provider = %q, want %q", got, want)
	}
}

// TestWorkerSessionCreateHintsEnablesMouse locks ga-c4w finding #1 for the
// UNMANAGED `gc session new` direct-start path (controller down): the CLI builds
// its runtime hints via workerSessionCreateHints, NOT the internal/api
// sessionCreateHints seam the original fix patched. Without MouseOn here the
// wheel→scrollback feature never reaches `gc session new` when the city runs
// unmanaged. Pool/headless agents never use this function — they resolve MouseOn
// via the reconciler's templateParamsToConfig (guarded by
// TestResolveTemplateHeadlessAgentStaysMouseOff), so this stays poll-safe.
func TestWorkerSessionCreateHintsEnablesMouse(t *testing.T) {
	hints := workerSessionCreateHints(&config.ResolvedProvider{Name: "stub"})
	if !hints.MouseOn {
		t.Error("workerSessionCreateHints().MouseOn = false, want true (gc session new unmanaged-direct wheel→scrollback, ga-c4w)")
	}
}

// piVllmRigCity builds an in-memory city with a single rig-scoped agent
// "myrig/polecat" running a custom provider whose base = "builtin:pi", plus a
// rig overlay dir carrying the per-provider/pi/ hooks. It mirrors the real
// pi-vllm hybrid shape used in gc-6bw8o. The qualified template name
// ("myrig/polecat") is the identity the resume path persists for a rig agent.
func piVllmRigCity(t *testing.T) (cityDir, overlayDir string, cfg *config.City) {
	t.Helper()
	cityDir = t.TempDir()
	gcDir := filepath.Join(cityDir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	overlayDir = filepath.Join(cityDir, "packs", "myrig", "overlay")
	// per-provider/pi/.pi/extensions/gc-hooks.js is the slot OverlayProviderNames
	// must resolve to for the harness to stage its ready-signal hook.
	if err := os.MkdirAll(filepath.Join(overlayDir, "per-provider", "pi", ".pi", "extensions"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(overlayDir, "per-provider", "pi", ".pi", "extensions", "gc-hooks.js"), []byte("// hook"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := "builtin:pi"
	cfg = &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              "polecat",
			Provider:          "pi-vllm",
			Scope:             "rig",
			Dir:               "myrig",
			InstallAgentHooks: []string{"pi"}, // declares the per-provider/pi/ overlay slot
		}},
		Providers: map[string]config.ProviderSpec{
			// Explicit command so resolution does not depend on a real `pi`
			// binary in the test host PATH; base still yields BuiltinAncestor=pi.
			"pi-vllm": {Base: &base, Command: "/bin/echo"},
		},
		Rigs:           []config.Rig{{Name: "myrig", Path: filepath.Join(cityDir, "myrig")}},
		RigOverlayDirs: map[string][]string{"myrig": {overlayDir}},
	}
	return cityDir, overlayDir, cfg
}

// TestResolvedWorkerRuntimeStagesProviderOverlayForRigBasePiProvider is the
// regression test for gc-6bw8o. The worker resume resolver
// (resolvedWorkerRuntimeWithConfigAndMetadata) builds runtime.Config directly
// and never routes through resolveTemplate, so before the fix it left
// ProviderOverlayName/PackOverlayDirs empty. OverlayProviderNames then fell back
// to ProviderName="" and the per-provider/pi/ hooks (gc-hooks.js) never staged,
// the harness never signaled ready, and the controller churned into a
// fall-back-to-claude loop. The fix populates these via applyWorkerOverlayHints,
// mirroring the reconciler create path's sourcing in template_resolve.go.
func TestResolvedWorkerRuntimeStagesProviderOverlayForRigBasePiProvider(t *testing.T) {
	cityDir, overlayDir, cfg := piVllmRigCity(t)

	runtimeCfg, err := resolvedWorkerRuntimeWithConfig(cityDir, cfg, session.Info{
		Template: "myrig/polecat",
		Command:  "pi-vllm",
		WorkDir:  cityDir,
	}, "")
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfig: %v", err)
	}
	if runtimeCfg == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfig() = nil")
	}

	// Concrete provider name drives the per-provider overlay slot.
	if got := strings.TrimSpace(runtimeCfg.Hints.ProviderOverlayName); got != "pi-vllm" {
		t.Fatalf("resume Hints.ProviderOverlayName = %q, want %q (concrete provider name)", got, "pi-vllm")
	}
	// Launch family is the base (pi), mirroring the reconciler create path.
	if got := strings.TrimSpace(runtimeCfg.Hints.ProviderName); got != "pi" {
		t.Fatalf("resume Hints.ProviderName = %q, want %q (launch family)", got, "pi")
	}
	// Rig overlay dir must be staged so per-provider/pi/ hooks reach the workdir.
	if len(runtimeCfg.Hints.PackOverlayDirs) == 0 {
		t.Fatalf("resume Hints.PackOverlayDirs is empty, want the rig overlay dir %q", overlayDir)
	}
	foundOverlay := false
	for _, od := range runtimeCfg.Hints.PackOverlayDirs {
		if od == overlayDir {
			foundOverlay = true
		}
	}
	if !foundOverlay {
		t.Fatalf("resume Hints.PackOverlayDirs = %v, want to include rig overlay dir %q", runtimeCfg.Hints.PackOverlayDirs, overlayDir)
	}
	// Sanity: the effective overlay slot list must be non-empty, otherwise
	// StageProviderOverlayDir would copy no per-provider/<slot>/ content.
	slots := runtime.OverlayProviderNames(runtimeCfg.Hints)
	if len(slots) == 0 {
		t.Fatal("runtime.OverlayProviderNames(resume Hints) is empty; per-provider overlay would never stage")
	}

	// End-to-end proof of the gc-6bw8o regression: actually stage the workdir
	// and confirm the per-provider/pi/ hook lands. Stage into a fresh temp
	// workdir rather than the city dir so the assertion is unambiguous.
	stageCfg := runtimeCfg.Hints
	stageCfg.WorkDir = t.TempDir()
	if err := runtime.StageSessionWorkDir(stageCfg); err != nil {
		t.Fatalf("StageSessionWorkDir: %v", err)
	}
	hookPath := filepath.Join(stageCfg.WorkDir, ".pi", "extensions", "gc-hooks.js")
	if _, err := os.Stat(hookPath); err != nil {
		t.Fatalf("per-provider/pi hook not staged at %s: %v", hookPath, err)
	}
}

// TestResolvedWorkerSessionConfigStagesProviderOverlayForRigBasePiProvider
// covers the CLI create path for the same gc-6bw8o rig pi-vllm agent. It first
// documents the gap (resolvedWorkerSessionConfigWithConfig, which only sees
// `resolved`, leaves the overlay fields empty), then asserts that
// applyWorkerOverlayHints — the exact call
// newWorkerSessionHandleForResolvedRuntimeWithConfig makes onto
// sessionCfg.Runtime.Hints before handing the spec to the worker factory —
// populates them so the per-provider/pi/ hooks would stage. The end-to-end
// create wiring is hard to assert (the resulting worker.Handle does not expose
// its Hints); the resume-path test above is the integration regression proof.
func TestResolvedWorkerSessionConfigStagesProviderOverlayForRigBasePiProvider(t *testing.T) {
	cityDir, overlayDir, cfg := piVllmRigCity(t)

	resolved, _ := resolveWorkerRuntimeProviderWithConfigAndMetadata(cfg, session.Info{Template: "myrig/polecat"}, "", nil)
	if resolved == nil {
		t.Fatal("resolveWorkerRuntimeProviderWithConfigAndMetadata() = nil")
	}

	sessionCfg, err := resolvedWorkerSessionConfigWithConfig(
		cityDir,
		resolved.CommandString(),
		"pi-vllm",
		cityDir,
		"myrig/polecat",
		"",
		"myrig/polecat",
		"polecat",
		"",
		resolved,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	// The bare builder must NOT set overlay fields — that is the gap the create
	// call site closes. If this ever starts populating them on its own, the
	// create-path applyWorkerOverlayHints call is redundant and this guard flags it.
	if got := strings.TrimSpace(sessionCfg.Runtime.Hints.ProviderOverlayName); got != "" {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig set ProviderOverlayName = %q on its own, expected empty (overlay is caller-applied)", got)
	}

	// Apply the overlay hints exactly as
	// newWorkerSessionHandleForResolvedRuntimeWithConfig does before the factory call.
	applyWorkerOverlayHints(&sessionCfg.Runtime.Hints, cfg, cityDir, "myrig/polecat", resolved)

	if got := strings.TrimSpace(sessionCfg.Runtime.Hints.ProviderOverlayName); got != "pi-vllm" {
		t.Fatalf("create Hints.ProviderOverlayName = %q, want %q", got, "pi-vllm")
	}
	if len(sessionCfg.Runtime.Hints.PackOverlayDirs) == 0 {
		t.Fatalf("create Hints.PackOverlayDirs is empty, want rig overlay dir %q", overlayDir)
	}
	if slots := runtime.OverlayProviderNames(sessionCfg.Runtime.Hints); len(slots) == 0 {
		t.Fatal("runtime.OverlayProviderNames(create Hints) is empty; per-provider overlay would never stage")
	}
}

// The CLI create and resume resolvers build session env from
// providerProcessPassthroughEnv() directly — neither routes through
// passthroughEnv() or convergence.ScrubTokenEnv, so cmd_start.go's GC_-sweep
// guard and template_resolve.go's re-pin do not cover them. The controller
// token must still arrive present-and-empty: the session inherits an
// environment that already carries it, so an absent key is an inherited key.
// resolved.Env carries a literal because it is config-authored and merges after
// the baseline.
func TestResolvedWorkerSessionConfigWithConfigWithholdsControllerToken(t *testing.T) {
	cityDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityDir, ".gc"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(convergence.TokenEnvVar, "super-secret-controller-token")

	cfg, err := resolvedWorkerSessionConfigWithConfig(
		cityDir,
		"",
		"",
		cityDir,
		"worker",
		"",
		"worker",
		"Worker",
		"",
		&config.ResolvedProvider{
			Name: "claude",
			Env:  map[string]string{convergence.TokenEnvVar: "provider-literal"},
		},
		map[string]string{"session_origin": "test"},
		nil,
	)
	if err != nil {
		t.Fatalf("resolvedWorkerSessionConfigWithConfig: %v", err)
	}
	for name, env := range map[string]map[string]string{
		"Runtime.SessionEnv": cfg.Runtime.SessionEnv,
		"Runtime.Hints.Env":  cfg.Runtime.Hints.Env,
	} {
		assertControllerTokenWithheld(t, name, env)
	}
}

func TestResolvedWorkerRuntimeWithConfigWithholdsControllerToken(t *testing.T) {
	cityDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityDir, ".gc"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(convergence.TokenEnvVar, "super-secret-controller-token")

	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:     "worker",
			Provider: "stub",
		}},
		Providers: map[string]config.ProviderSpec{
			"stub": {
				Command: "/bin/echo",
				Env:     map[string]string{convergence.TokenEnvVar: "provider-literal"},
			},
		},
	}

	resolved, err := resolvedWorkerRuntimeWithConfigAndMetadata(cityDir, cfg, session.Info{
		Template: "worker",
		WorkDir:  cityDir,
	}, "", nil)
	if err != nil {
		t.Fatalf("resolvedWorkerRuntimeWithConfigAndMetadata: %v", err)
	}
	if resolved == nil {
		t.Fatal("resolvedWorkerRuntimeWithConfigAndMetadata() = nil")
	}
	for name, env := range map[string]map[string]string{
		"SessionEnv": resolved.SessionEnv,
		"Hints.Env":  resolved.Hints.Env,
	} {
		assertControllerTokenWithheld(t, name, env)
	}
}

func assertControllerTokenWithheld(t *testing.T, name string, env map[string]string) {
	t.Helper()
	val, ok := env[convergence.TokenEnvVar]
	if !ok {
		t.Errorf("%s omits %s; want present and empty so the session cannot inherit the controller's value", name, convergence.TokenEnvVar)
		return
	}
	if val != "" {
		t.Errorf("%s[%s] = %q, want empty", name, convergence.TokenEnvVar, val)
	}
}

// TestWorkerFactoryWithConfigRecordsWorkerOperationToCityEventLog is the
// assertion #5859 is about: the CLI-built worker.Factory carries a live event
// recorder, so a lifecycle op driven through it lands a worker.operation
// record in <city>/.gc/events.jsonl. Before the recorder was wired in, the
// factory held none and the record was dropped on the floor.
func TestWorkerFactoryWithConfigRecordsWorkerOperationToCityEventLog(t *testing.T) {
	resetCLIFactoryRecorders(t)
	t.Setenv("GC_EVENTS", "")

	cityDir := t.TempDir()
	writePhase0InterfaceCity(t, cityDir, `[workspace]
name = "test-city"

[beads]
provider = "file"

[[agent]]
name = "worker"
provider = "stub"

[providers.stub]
command = "/bin/echo"
`)

	cfg, err := loadCityConfig(cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	store, err := openCityStoreAt(cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	sp := runtime.NewFake()
	mgr := newSessionManagerWithConfig(cityDir, store, sp, cfg)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template:  "worker",
		Title:     "Probe",
		Command:   "stub",
		WorkDir:   t.TempDir(),
		Provider:  "stub",
		Hints:     runtime.Config{},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	factory, err := workerFactoryWithConfig(cityDir, store, sp, cfg)
	if err != nil {
		t.Fatalf("workerFactoryWithConfig: %v", err)
	}
	handle, err := factory.SessionByID(info.ID)
	if err != nil {
		t.Fatalf("factory.SessionByID: %v", err)
	}
	if err := handle.Kill(context.Background()); err != nil {
		t.Fatalf("handle.Kill: %v", err)
	}

	eventsPath := filepath.Join(cityDir, ".gc", "events.jsonl")
	raw, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", eventsPath, err)
	}
	var found *events.Event
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var e events.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("unmarshaling %q: %v", line, err)
		}
		if e.Type == events.WorkerOperation {
			found = &e
			break
		}
	}
	if found == nil {
		t.Fatalf("no %q record in %s; log = %s", events.WorkerOperation, eventsPath, string(raw))
	}
	if !strings.Contains(string(found.Payload), info.SessionName) {
		t.Fatalf("%q payload = %s, want it to name session %q", events.WorkerOperation, string(found.Payload), info.SessionName)
	}
}

// stagingWorkerProvider keeps lifecycle in memory and runs the production file-staging boundary.
type stagingWorkerProvider struct{ *runtime.Fake }

func (p *stagingWorkerProvider) Start(ctx context.Context, name string, cfg runtime.Config) error {
	if err := runtime.StageSessionWorkDir(cfg); err != nil {
		return err
	}
	return p.Fake.Start(ctx, name, cfg)
}

func TestWorkerFreshFallbackStartPreparesHooks(t *testing.T) {
	for _, operation := range []string{"missing-key fallback", "direct no key", "direct generated key"} {
		t.Run(operation, func(t *testing.T) {
			cfg, fixture := workerPromptLaunchFixture(t, "authorized startup instructions")
			cityDir := fixture.WorkDir
			workDir := filepath.Join(cityDir, "persisted-session-dir")
			cfg.Agents[0].WorkDir = filepath.Join(cityDir, "configured-must-remain-absent")
			cfg.Agents[0].Provider = "claude"
			cfg.Providers["claude"] = config.ProviderSpec{Command: "/bin/echo", PromptMode: "arg"}
			cfg.Agents[0].InstallAgentHooks = []string{"claude", "codex"}
			overlay := t.TempDir()
			cfg.Agents[0].OverlayDir = overlay
			if err := os.WriteFile(filepath.Join(overlay, "sentinel.txt"), []byte("staged"), 0o644); err != nil {
				t.Fatal(err)
			}
			store := beads.NewMemStore()
			sp := &stagingWorkerProvider{runtime.NewFake()}
			var info session.Info
			var handle worker.Handle
			var err error
			if operation == "missing-key fallback" {
				mgr := session.NewManagerWithOptions(store, sp)
				info, err = mgr.CreateSession(context.Background(), session.CreateOptions{BeadOnly: true, Template: fixture.Template, Command: "/bin/echo", WorkDir: workDir, Provider: "claude", ExtraMeta: map[string]string{"agent_name": "worker", "session_origin": "manual"}})
				if err != nil {
					t.Fatal(err)
				}
				handle, err = workerHandleForSessionWithConfig(cityDir, store, sp, cfg, info.ID)
			} else {
				spec := cfg.Providers["claude"]
				spec.ResumeFlag = "--resume"
				if operation == "direct generated key" {
					spec.SessionIDFlag = "--session-id"
				}
				cfg.Providers["claude"] = spec
				resolved, resolveErr := config.ResolveProvider(&cfg.Agents[0], &cfg.Workspace, cfg.Providers, func(name string) (string, error) { return name, nil })
				if resolveErr != nil {
					t.Fatal(resolveErr)
				}
				if operation == "direct no key" {
					resolved.SessionIDFlag = ""
				} // override inherited Claude defaults in this explicit keyless fixture
				handle, err = newWorkerSessionHandleForDirectCreateWithConfig(cityDir, store, sp, cfg, "", "direct-hooks", fixture.Template, "title", resolved.CommandString(), resolved.Name, workDir, "", resolved, map[string]string{"agent_name": "worker"})
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, target := range []string{filepath.Join(cityDir, ".gc/settings.json"), filepath.Join(workDir, ".codex/hooks.json")} {
				if _, err := os.Stat(target); !os.IsNotExist(err) {
					t.Fatalf("handle construction prepared %s: %v", target, err)
				}
			}
			if operation == "missing-key fallback" {
				err = handle.Start(context.Background())
			} else {
				info, err = handle.Create(context.Background(), worker.CreateModeStarted)
			}
			if err != nil {
				t.Fatal(err)
			}
			if b, err := os.ReadFile(filepath.Join(workDir, "sentinel.txt")); err != nil || string(b) != "staged" {
				t.Fatalf("start overlay = %q, err=%v", b, err)
			}
			for _, target := range []string{filepath.Join(cityDir, ".gc/settings.json"), filepath.Join(workDir, ".codex/hooks.json")} {
				if data, err := os.ReadFile(target); err != nil || !json.Valid(data) {
					t.Errorf("authorized Start missing generated hooks/settings %s: %v", target, err)
				}
			}
			started := sp.LastStartConfig(info.SessionName)
			if started == nil || started.WorkDir != workDir {
				t.Fatalf("start = %#v, want persisted workdir %q", started, workDir)
			}
			settingsFound := false
			args := shellquote.Split(started.Command)
			for i, arg := range args {
				if arg == "--settings" && i+1 < len(args) && args[i+1] == filepath.Join(cityDir, ".gc/settings.json") {
					settingsFound = true
				}
			}
			if !settingsFound {
				t.Errorf("start command omitted projected settings: %q", started.Command)
			}
			if _, err := os.Stat(cfg.Agents[0].WorkDir); !os.IsNotExist(err) {
				t.Fatalf("start prepared configured directory instead of persisted directory: %v", err)
			}

			if operation == "direct no key" && info.SessionKey != "" {
				t.Fatalf("keyless fixture generated a provider key %q", info.SessionKey)
			}
			if operation == "direct generated key" {
				if info.SessionKey == "" || !strings.Contains(started.Command, "--session-id "+info.SessionKey) {
					t.Fatalf("fresh generated key lost: key=%q command=%q", info.SessionKey, started.Command)
				}
				const preserved = `{"preserve":"genuine resume"}`
				for _, target := range []string{filepath.Join(cityDir, ".gc/settings.json"), filepath.Join(workDir, ".codex/hooks.json")} {
					if err := os.WriteFile(target, []byte(preserved), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				if err := sp.Stop(info.SessionName); err != nil {
					t.Fatal(err)
				} // virtual runtime only
				// Legacy keyed records may lack completion metadata. Missing fields alone
				// must never be treated as authorization for fresh-create preparation.
				if err := store.SetMetadataBatch(info.ID, map[string]string{"creation_complete_at": "", "started_config_hash": ""}); err != nil {
					t.Fatal(err)
				}
				resumeCtx, cancelResume := context.WithCancel(context.Background())
				defer cancelResume()
				waitCalls := 0
				resumed, err := workerHandleForSessionWithStaleKeyDetectionWaiter(cityDir, store, sp, cfg, info.ID, func(waitCtx context.Context, name string) error {
					waitCalls++
					if waitCtx != resumeCtx || name != info.SessionName {
						t.Errorf("resume waiter context/session changed: context matches=%t session=%q want=%q", waitCtx == resumeCtx, name, info.SessionName)
					}
					return waitCtx.Err()
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := resumed.Start(resumeCtx); err != nil {
					t.Fatal(err)
				}
				if waitCalls != 1 {
					t.Fatalf("resume waiter calls=%d want1", waitCalls)
				}
				launch := sp.LastStartConfig(info.SessionName)
				if launch == nil || !strings.Contains(launch.Command, "--resume "+info.SessionKey) || launch.PromptSuffix != "" || launch.PromptFlag != "" || launch.Nudge != "" || launch.Env[startupPromptDeliveredEnv] != "" {
					t.Fatalf("genuine keyed resume replayed startup: %#v", launch)
				}
				for _, target := range []string{filepath.Join(cityDir, ".gc/settings.json"), filepath.Join(workDir, ".codex/hooks.json")} {
					if data, err := os.ReadFile(target); err != nil || string(data) != preserved {
						t.Fatalf("keyed resume rewrote hooks/settings %s: %q,%v", target, data, err)
					}
				}
			}
		})
	}
}

func TestWorkerFreshFallbackSuppressedT3BridgePrompt(t *testing.T) {
	cfg, info, _ := workerPromptFixture(t, "/bin/echo", "unused startup instructions")
	cfg.Session.Provider = "t3bridge"
	cfg.Agents[0].Name = config.ControlDispatcherAgentName
	cfg.Agents[0].Provider = ""
	cfg.Agents[0].StartCommand = config.ControlDispatcherStartCommandFor(config.ControlDispatcherAgentName)
	info.Template = config.ControlDispatcherAgentName
	info.AgentName = config.ControlDispatcherAgentName
	resolved, err := resolvedWorkerRuntimeWithConfig(info.WorkDir, cfg, info, "")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Hints.PromptSuffix != "" || resolved.Hints.PromptFlag != "" || resolved.Hints.Nudge != "" || resolved.Hints.Env[startupPromptDeliveredEnv] != "" {
		t.Fatal("suppressed startup prompt selected delivery or acquired a delivery marker")
	}
}

func TestWorkerRuntimeStartPreparerKeepsMetadataBackedACPRoute(t *testing.T) {
	cfg, info, _ := workerPromptFixture(t, "/bin/echo", "ACP startup instructions")
	cityDir := info.WorkDir
	cfg.Agents[0].Provider = "claude"
	cfg.Providers["claude"] = config.ProviderSpec{Command: "/bin/echo"}
	prepare := workerRuntimeStartPreparerWithConfig(cityDir, cfg)
	start := runtime.Config{Command: "/bin/echo", WorkDir: cityDir, ProviderName: "claude", InstallAgentHooks: []string{"claude", "codex"}}
	persisted := session.PersistedResponse{Metadata: map[string]string{session.MCPIdentityMetadataKey: "session-mcp-binding"}}
	info.MCPIdentity = persisted.Metadata[session.MCPIdentityMetadataKey]
	if err := prepare(info, persisted, &start, false); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{".gc/settings.json", ".codex/hooks.json"} {
		if _, err := os.Stat(filepath.Join(cityDir, rel)); !os.IsNotExist(err) {
			t.Fatalf("ACP route acquired CLI startup preparation at %s: %v", rel, err)
		}
	}
	if start.Command != "/bin/echo" {
		t.Fatalf("ACP command changed: %q", start.Command)
	}
}

func TestResolvedWorkerRuntimeFreshFallbackOversizedLogIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, runtime, mode string
		wantError           bool
	}{
		{name: "nudge fallback", runtime: "tmux", mode: "nudge-fallback"},
		{name: "unsupported runtime", runtime: "subprocess", mode: "hard-fail", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, info, _ := workerPromptFixture(t, "/bin/echo", strings.Repeat("x", maxPromptSuffixRawBytes))
			cfg.Session.Provider = tc.runtime
			info.AgentName = "persisted-worker-42"
			info.SessionName = "persisted-runtime-session"
			info.SessionNameMetadata = info.SessionName
			handler := &captureHandler{}
			previous := slog.Default()
			slog.SetDefault(slog.New(handler))
			t.Cleanup(func() { slog.SetDefault(previous) })
			_, err := resolvedWorkerRuntimeWithConfig(info.WorkDir, cfg, info, "")
			if (err != nil) != tc.wantError {
				t.Fatalf("runtime resolution error = %v, want error=%t", err, tc.wantError)
			}
			records := oversizedPromptDeliveryLogRecords(handler.records)
			if len(records) != 1 {
				t.Fatalf("oversized delivery records = %d, want 1", len(records))
			}
			fields := recordFields(records[0])
			for field, want := range map[string]string{
				"agent": info.AgentName, "session": info.SessionName, "effective_mode": tc.mode,
			} {
				if got := fields[field].String(); got != want {
					t.Errorf("log field %s = %q, want %q (template=%q)", field, got, want, info.Template)
				}
			}
		})
	}
}

func TestWorkerDirectCreateUsesSelectedTransport(t *testing.T) {
	for _, tc := range []struct {
		name        string
		transport   string
		loaderError bool
	}{
		{name: "shell"},
		{name: "acp", transport: "acp"},
		{name: "catalog error before preparation", transport: "acp", loaderError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, fixture := workerPromptLaunchFixture(t, "ACP protocol owns the initial request")
			supported := true
			cfg.Agents[0].Provider = "claude"
			cfg.Agents[0].Session = "acp"
			cfg.Agents[0].InstallAgentHooks = []string{"claude", "codex"}
			cfg.Providers["claude"] = config.ProviderSpec{Command: "/bin/echo", ACPCommand: "/bin/echo", SupportsACP: &supported, PromptMode: "arg"}
			resolved, err := config.ResolveProvider(&cfg.Agents[0], &cfg.Workspace, cfg.Providers, func(name string) (string, error) { return name, nil })
			if err != nil {
				t.Fatal(err)
			}
			metadata := map[string]string{"agent_name": "worker", "sentinel": "selected-context"}
			if tc.transport == "acp" {
				metadata[session.MCPIdentityMetadataKey] = "catalog-owner"
			}
			catalog := []runtime.MCPServerConfig{{Name: "filesystem", Transport: runtime.MCPTransportStdio, Command: "/scripted/mcp", Args: []string{"--stdio", "selected-context"}, Env: map[string]string{"SELECTED": "context"}}}
			loaderErr := fmt.Errorf("loading effective MCP: scripted catalog error")
			calls := 0
			loadMCP := func(cityPath string, gotConfig *config.City, alias, template, provider, workDir, transport string, gotMeta map[string]string) ([]runtime.MCPServerConfig, error) {
				calls++
				if cityPath != fixture.WorkDir || gotConfig != cfg || alias != "direct-alias" || template != fixture.Template || provider != resolved.Name || workDir != fixture.WorkDir || transport != tc.transport || !reflect.DeepEqual(gotMeta, metadata) {
					t.Errorf("MCP loader received wrong selected context: city=%q config=%p alias=%q template=%q provider=%q workdir=%q transport=%q metadata=%v", cityPath, gotConfig, alias, template, provider, workDir, transport, gotMeta)
				}
				// The production resolver keeps this transport guard before catalog I/O.
				if transport != "acp" {
					return nil, nil
				}
				if tc.loaderError {
					return nil, loaderErr
				}
				return catalog, nil
			}
			store := beads.NewMemStore()
			sp := runtime.NewFake()
			handle, err := newWorkerSessionHandleForDirectCreateWithMCPResolver(fixture.WorkDir, store, sp, cfg, "direct-alias", "direct-acp", fixture.Template, "title", resolved.CommandString(), resolved.Name, fixture.WorkDir, tc.transport, resolved, metadata, loadMCP)
			if calls != 1 {
				t.Fatalf("MCP loader calls=%d want1", calls)
			}
			targets := []string{filepath.Join(fixture.WorkDir, ".gc/settings.json"), filepath.Join(fixture.WorkDir, ".codex/hooks.json")}
			if tc.loaderError {
				if handle != nil || !errors.Is(err, loaderErr) {
					t.Fatalf("loader error wrapping changed: handle=%v err=%v want=%v", handle != nil, err, loaderErr)
				}
				if sp.CountCalls("Start", "direct-acp") != 0 {
					t.Fatal("loader failure called Provider.Start")
				}
				for _, target := range targets {
					if _, err := os.Stat(target); !os.IsNotExist(err) {
						t.Fatalf("loader failure prepared %s: %v", target, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			info, err := handle.Create(context.Background(), worker.CreateModeStarted)
			if err != nil {
				t.Fatal(err)
			}
			launch := sp.LastStartConfig(info.SessionName)
			if launch == nil || info.Transport != tc.transport || info.WorkDir != fixture.WorkDir || launch.WorkDir != fixture.WorkDir || info.Provider != resolved.Name || info.AgentName != "worker" {
				t.Fatalf("selected launch identity/transport/workdir changed: info=%+v launch=%#v", info, launch)
			}
			if tc.transport == "acp" {
				if launch.PromptSuffix != "" || launch.PromptFlag != "" || launch.Nudge != "" || launch.Env[startupPromptDeliveredEnv] != "" {
					t.Fatal("ACP direct create replayed shell startup prompt")
				}
				if !reflect.DeepEqual(launch.MCPServers, runtime.NormalizeMCPServerConfigs(catalog)) {
					t.Fatalf("selected MCP catalog not forwarded: got=%#v want=%#v", launch.MCPServers, catalog)
				}
			} else {
				if launch.PromptSuffix == "" || launch.Env[startupPromptDeliveredEnv] != "1" {
					t.Fatal("selected shell transport lost initial prompt")
				}
				if len(launch.MCPServers) != 0 {
					t.Fatalf("shell acquired ACP catalog: %#v", launch.MCPServers)
				}
			}
			for _, target := range targets {
				data, err := os.ReadFile(target)
				if tc.transport == "acp" {
					if !os.IsNotExist(err) {
						t.Fatalf("ACP direct create prepared shell hook/settings %s: %v", target, err)
					}
				} else if err != nil || !json.Valid(data) {
					t.Fatalf("selected shell transport omitted valid requested setup %s: %v", target, err)
				}
			}
			if tc.transport != "acp" {
				args := shellquote.Split(launch.Command)
				found := false
				for i, arg := range args {
					if arg == "--settings" && i+1 < len(args) && args[i+1] == filepath.Join(fixture.WorkDir, ".gc/settings.json") {
						found = true
					}
				}
				if !found {
					t.Fatalf("shell command omitted settings argument: %q", launch.Command)
				}
			}
		})
	}
}
