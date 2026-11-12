package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMakefileTestEnvIgnoresUserGitConfiguration(t *testing.T) {
	repoRoot := repoRoot(t)
	makefile, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}

	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, ".gitconfig"), []byte("[commit]\n\tgpgsign = true\n"), 0o644); err != nil {
		t.Fatalf("write poisoned global git config: %v", err)
	}

	testMakefile := filepath.Join(t.TempDir(), "Makefile")
	content := string(makefile) + `
.PHONY: print-test-env-git
print-test-env-git:
	@$(TEST_ENV) sh -c 'printf "global=%s\nnosystem=%s\ngitdir=%s\ngpgsign=%s\n" "$$GIT_CONFIG_GLOBAL" "$$GIT_CONFIG_NOSYSTEM" "$${GIT_DIR-unset}" "$$(git config --global --get commit.gpgsign 2>/dev/null || printf unset)"'
`
	if err := os.WriteFile(testMakefile, []byte(content), 0o644); err != nil {
		t.Fatalf("write test Makefile: %v", err)
	}

	cmd := makeCommand("--no-print-directory", "-f", testMakefile, "print-test-env-git")
	cmd.Dir = repoRoot
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"USER=" + os.Getenv("USER"),
		"SHELL=/bin/sh",
		"GIT_DIR=/poison/.git",
		// Makefile-internal `go env` probes must not download a toolchain
		// into the isolated HOME: the module cache's read-only files would
		// defeat t.TempDir cleanup.
		"GOTOOLCHAIN=local",
		"GOMODCACHE=" + filepath.Join(os.TempDir(), "gc-makefile-git-gomodcache"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("make print-test-env-git failed: %v\n%s", err, out)
	}
	for _, want := range []string{
		"nosystem=1",
		"gitdir=unset",
		"gpgsign=unset",
	} {
		if !strings.Contains(string(out), want+"\n") {
			t.Errorf("TEST_ENV output missing %q:\n%s", want, out)
		}
	}

	// The global config is a real, writable, seeded file outside the user's
	// HOME — not the user's own config and not an unwritable sentinel (a
	// /dev/null sentinel breaks ensure_beads_role's global write path).
	var globalPath string
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "global="); ok {
			globalPath = v
		}
	}
	if globalPath == "" {
		t.Fatalf("TEST_ENV output missing global= line:\n%s", out)
	}
	if strings.HasPrefix(globalPath, home+string(os.PathSeparator)) {
		t.Errorf("GIT_CONFIG_GLOBAL %q resolves under the poisoned HOME %q", globalPath, home)
	}
	info, err := os.Stat(globalPath)
	if err != nil {
		t.Fatalf("GIT_CONFIG_GLOBAL %q does not exist: %v", globalPath, err)
	}
	if info.Mode().Perm()&0o200 == 0 {
		t.Errorf("GIT_CONFIG_GLOBAL %q is not writable (mode %v)", globalPath, info.Mode())
	}
}

func TestShardTestEnvsIgnoreUserGitConfiguration(t *testing.T) {
	repoRoot := repoRoot(t)
	for _, path := range []string{
		"scripts/test-local-parallel",
		"scripts/test-go-test-shard",
		"scripts/test-integration-shard",
	} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(repoRoot, path))
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			content := string(data)
			for _, pin := range []string{
				"GIT_CONFIG_NOSYSTEM=1",
				`GIT_CONFIG_GLOBAL="$gc_test_gitconfig"`,
				`gc_test_gitconfig="$("$repo_root/scripts/test-gitconfig-path")"`,
			} {
				if got := strings.Count(content, pin); got != 1 {
					t.Errorf("%s has %d occurrences of %q, want 1", path, got, pin)
				}
			}
			// ga-cesmzs: only test-local-parallel crosses a subprocess boundary
			// (the xargs fan-out worker), so only it must export the variable.
			if path == "scripts/test-local-parallel" {
				if got := strings.Count(content, "\nexport gc_test_gitconfig\n"); got != 1 {
					t.Errorf("%s must export gc_test_gitconfig for the xargs fan-out workers (found %d)", path, got)
				}
			}
		})
	}
}

// TestFanOutWorkerReceivesExportedGitConfigGlobal exercises the real
// run_fan_out xargs/bash dispatch end to end instead of asserting on source
// text: it extracts the live gc_test_gitconfig preamble and run_fan_out
// function body from the current file content and runs them under a minimal
// synthetic harness. The fake tool and owned login profile prove that a child
// job retains the runner's explicitly supplied PATH. The prior login-shell
// invocation is run as a negative control against the same job. This also
// catches an unexported gc_test_gitconfig (ga-9t7vpl) by the worker actually
// failing, a class of regression TestShardTestEnvsIgnoreUserGitConfiguration
// above cannot detect since it only counts a string, not the variable's export
// scope.
func TestFanOutWorkerReceivesExportedGitConfigGlobal(t *testing.T) {
	repoRoot := repoRoot(t)
	scriptPath := filepath.Join(repoRoot, "scripts", "test-local-parallel")
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read %s: %v", scriptPath, err)
	}
	content := string(data)

	const assignLine = `gc_test_gitconfig="$("$repo_root/scripts/test-gitconfig-path")"`
	if !strings.Contains(content, assignLine) {
		t.Fatalf("%s: gc_test_gitconfig assignment line not found (expected exact text %q)", scriptPath, assignLine)
	}
	preamble := assignLine
	if strings.Contains(content, "\nexport gc_test_gitconfig\n") {
		preamble += "\nexport gc_test_gitconfig"
	}

	const fanOutOpen = "run_fan_out() {\n"
	startIdx := strings.Index(content, fanOutOpen)
	if startIdx == -1 {
		t.Fatalf("%s: run_fan_out() function not found", scriptPath)
	}
	bodyStart := startIdx + len(fanOutOpen)
	endIdx := strings.Index(content[bodyStart:], "\n}\n")
	if endIdx == -1 {
		t.Fatalf("%s: run_fan_out() closing brace not found", scriptPath)
	}
	fanOutBody := content[bodyStart : bodyStart+endIdx]

	const currentChildShell = `bash -c "$command"`
	const loginChildShell = `bash -lc "$command"`
	currentShellCount := strings.Count(fanOutBody, currentChildShell)
	loginShellCount := strings.Count(fanOutBody, loginChildShell)
	var loginControlBody string
	switch {
	case currentShellCount == 1 && loginShellCount == 0:
		loginControlBody = strings.Replace(fanOutBody, currentChildShell, loginChildShell, 1)
	case currentShellCount == 0 && loginShellCount == 1:
		loginControlBody = fanOutBody
	default:
		t.Fatalf("%s: run_fan_out() has %d non-login and %d login child-shell invocations; want exactly one", scriptPath, currentShellCount, loginShellCount)
	}

	logDir := t.TempDir()
	homeDir := t.TempDir()
	toolsDir := t.TempDir()
	loginToolsDir := t.TempDir()
	profile := "PATH=" + shellQuote(loginToolsDir) + "\nexport PATH\n"
	if err := os.WriteFile(filepath.Join(homeDir, ".bash_profile"), []byte(profile), 0o644); err != nil {
		t.Fatalf("write owned login profile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(toolsDir, "gc-path-sentinel"), []byte("#!/bin/sh\nprintf '%s\\n' 'PATH_SENTINEL=preserved'\n"), 0o755); err != nil {
		t.Fatalf("write PATH sentinel: %v", err)
	}
	if err := os.WriteFile(filepath.Join(loginToolsDir, "gc-path-sentinel"), []byte("#!/bin/sh\nprintf '%s\\n' 'PATH_SENTINEL=login-fallback'\nexit 23\n"), 0o755); err != nil {
		t.Fatalf("write login PATH fallback sentinel: %v", err)
	}
	probeCmd := `printf 'CHILD_PATH=%s\n' "$PATH"; if [ -n "${GIT_CONFIG_GLOBAL:-}" ] && [ -f "$GIT_CONFIG_GLOBAL" ] && [ -w "$GIT_CONFIG_GLOBAL" ]; then printf "GIT_CONFIG_GLOBAL_OK=%s\n" "$GIT_CONFIG_GLOBAL"; else printf "GIT_CONFIG_GLOBAL_MISSING\n"; exit 1; fi; gc-path-sentinel`
	childPath := toolsDir + string(os.PathListSeparator) + os.Getenv("PATH")

	lines := []string{
		"#!/usr/bin/env bash",
		"set -euo pipefail",
		"repo_root=" + shellQuote(repoRoot),
		preamble,
		"run_fan_out() {",
		fanOutBody,
		"}",
		"run_fan_out_login_control() {",
		loginControlBody,
		"}",
		`gate_fd=""`,
		"local_jobs=1",
		"jobspecs=(" + shellQuote("probe-login::"+probeCmd) + ")",
		"export LOCAL_TEST_LOG_DIR=" + shellQuote(logDir),
		"export PATH=" + shellQuote(childPath),
		"export HOME=" + shellQuote(homeDir),
		"export TMPDIR=" + shellQuote(logDir),
		`export TEST_LOCAL_NICE=""`,
		"export TEST_LOCAL_GOPATH=" + shellQuote(goEnvValue(t, "GOPATH")),
		"export TEST_LOCAL_GOCACHE=" + shellQuote(goEnvValue(t, "GOCACHE")),
		"export TEST_LOCAL_GOMODCACHE=" + shellQuote(goEnvValue(t, "GOMODCACHE")),
		"export TEST_LOCAL_GOTMPDIR=" + shellQuote(goEnvValue(t, "GOTMPDIR")),
		"export TEST_LOCAL_GOROOT=" + shellQuote(goEnvValue(t, "GOROOT")),
		"set +e",
		"run_fan_out_login_control",
		"login_status=$?",
		"set -e",
		`if [[ "$login_status" -eq 0 ]]; then echo "login-shell control unexpectedly found the PATH sentinel" >&2; exit 1; fi`,
		"jobspecs=(" + shellQuote("probe-fixed::"+probeCmd) + ")",
		"set +e",
		"run_fan_out",
		"status=$?",
		"set -e",
		`exit "$status"`,
	}
	harnessPath := filepath.Join(t.TempDir(), "run_fan_out_harness.sh")
	if err := os.WriteFile(harnessPath, []byte(strings.Join(lines, "\n")+"\n"), 0o755); err != nil {
		t.Fatalf("write harness script: %v", err)
	}

	harness := testCommand("bash", harnessPath)
	harness.Env = []string{"PATH=" + childPath, "HOME=" + homeDir, "TMPDIR=" + logDir}
	out, runErr := harness.CombinedOutput()
	loginLog := filepath.Join(logDir, "probe-login.log")
	loginOut, loginReadErr := os.ReadFile(loginLog)
	probeLog := filepath.Join(logDir, "probe-fixed.log")
	probeOut, readErr := os.ReadFile(probeLog)

	if runErr != nil {
		t.Fatalf("run_fan_out worker did not receive the supplied PATH and usable GIT_CONFIG_GLOBAL: %v\nharness output:\n%s\nprobe log (err=%v):\n%s",
			runErr, out, readErr, probeOut)
	}
	if loginReadErr != nil {
		t.Fatalf("read login-control log %s: %v\nharness output:\n%s", loginLog, loginReadErr, out)
	}
	if !strings.Contains(string(loginOut), "GIT_CONFIG_GLOBAL_OK=") {
		t.Fatalf("login-control job did not receive GIT_CONFIG_GLOBAL before the sentinel lookup failed; log:\n%s\nharness output:\n%s", loginOut, out)
	}
	if !strings.Contains(string(loginOut), "CHILD_PATH="+loginToolsDir) || !strings.Contains(string(loginOut), "PATH_SENTINEL=login-fallback") || strings.Contains(string(loginOut), "PATH_SENTINEL=preserved") {
		t.Fatalf("login-shell control did not demonstrate owned PATH replacement and fallback sentinel selection; log:\n%s\nharness output:\n%s", loginOut, out)
	}
	if readErr != nil {
		t.Fatalf("read probe log %s: %v\nharness output:\n%s", probeLog, readErr, out)
	}
	if !strings.Contains(string(probeOut), "GIT_CONFIG_GLOBAL_OK=") {
		t.Fatalf("probe job did not confirm GIT_CONFIG_GLOBAL; probe log:\n%s\nharness output:\n%s", probeOut, out)
	}
	if !strings.Contains(string(probeOut), "CHILD_PATH="+childPath) || !strings.Contains(string(probeOut), "PATH_SENTINEL=preserved") || strings.Contains(string(probeOut), "PATH_SENTINEL=login-fallback") {
		t.Fatalf("probe job did not retain the supplied PATH and resolve its sentinel; probe log:\n%s\nharness output:\n%s", probeOut, out)
	}
}
