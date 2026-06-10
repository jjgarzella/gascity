package main

import (
	"bytes"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// builtinClaudeModelChoices extracts the value->FlagArgs map for the "model"
// option of the real builtin claude provider. The per-dispatch opt_model fold
// validates a work bead's opt_model against exactly these choices, so pinning
// them here turns "which opt_model strings are valid?" into an executable
// assertion instead of a guess.
func builtinClaudeModelChoices(t *testing.T) map[string][]string {
	t.Helper()
	rp := claudeEffortResolvedProvider()
	for _, opt := range rp.OptionsSchema {
		if opt.Key != "model" {
			continue
		}
		choices := make(map[string][]string, len(opt.Choices))
		for _, c := range opt.Choices {
			if c.Value == "" {
				continue // the "Default" (no flag) choice
			}
			choices[c.Value] = c.FlagArgs
		}
		return choices
	}
	t.Fatal("builtin claude provider has no model option in its OptionsSchema")
	return nil
}

// TestBuiltinClaudeOptModelValidStringsPinned is the executable form of
// gcs-f4j.8.2.1 DO step 1+2: list the valid opt_model strings accepted by the
// real builtin claude provider's options_schema and pin the cheap/deep tier
// strings the standard-reviewer lanes select. A typo'd tier name is silently
// skipped at spawn (it falls back to the agent default), so the exact accepted
// strings must be pinned, not guessed.
func TestBuiltinClaudeOptModelValidStringsPinned(t *testing.T) {
	choices := builtinClaudeModelChoices(t)

	// The complete set of valid opt_model strings for the claude provider.
	gotKeys := make([]string, 0, len(choices))
	for k := range choices {
		gotKeys = append(gotKeys, k)
	}
	wantKeys := map[string]bool{
		"fable-5": true, "opus": true, "opus-4-7": true, "sonnet": true, "haiku": true,
	}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("valid opt_model strings = %v, want exactly %v", gotKeys, wantKeys)
	}
	for _, k := range gotKeys {
		if !wantKeys[k] {
			t.Fatalf("unexpected opt_model choice %q (valid set: %v)", k, wantKeys)
		}
	}

	// Tier strings the lanes select map to the expected --model flag args.
	tierFlag := map[string][]string{
		"haiku":  {"--model", "claude-haiku-4-5-20251001"}, // cheap tier
		"sonnet": {"--model", "claude-sonnet-4-6"},         // mid tier
		"opus":   {"--model", "claude-opus-4-8"},           // deep tier
	}
	for tier, want := range tierFlag {
		got, ok := choices[tier]
		if !ok {
			t.Fatalf("tier string %q is not a valid claude opt_model choice", tier)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("opt_model=%q -> FlagArgs %v, want %v", tier, got, want)
		}
	}

	// A bogus tier string must be rejected by the schema (closed set), which is
	// what makes a typo degrade to the default rather than pass through.
	if _, err := config.ResolveExplicitOptions(claudeEffortResolvedProvider().OptionsSchema,
		map[string]string{"model": "definitely-not-a-tier"}); err == nil {
		t.Fatal("ResolveExplicitOptions accepted a bogus opt_model; want a rejection error")
	}
}

// TestBuildPreparedStartBuiltinClaudeOptModelTierTakesEffect is the executable
// form of DO step 3 (happy path): a work bead's opt_model flows through
// resolveTaskOptionOverrides -> the provider options schema -> the spawned
// session's launch command as the expected --model flag, against the REAL
// builtin claude provider (not a synthetic fixture). This is the deterministic
// counterpart to the live argv evidence captured upstream when Fable 5 was
// added (commit b9f68923d: a gascity-spawned session's argv showed
// `claude --model claude-fable-5`).
func TestBuildPreparedStartBuiltinClaudeOptModelTierTakesEffect(t *testing.T) {
	cases := []struct {
		optModel string
		wantFlag string
	}{
		{"haiku", "--model claude-haiku-4-5-20251001"},
		{"sonnet", "--model claude-sonnet-4-6"},
		{"opus", "--model claude-opus-4-8"},
	}
	for _, tc := range cases {
		t.Run(tc.optModel, func(t *testing.T) {
			candidate, cfg, store := newOptionSessionWithWork(
				t, claudeEffortResolvedProvider(), "claude",
				map[string]string{"model": tc.optModel},
			)
			prepared, err := buildPreparedStart(candidate, cfg, store)
			if err != nil {
				t.Fatalf("buildPreparedStart: %v", err)
			}
			if !strings.Contains(prepared.cfg.Command, tc.wantFlag) {
				t.Fatalf("command %q should carry per-dispatch %q", prepared.cfg.Command, tc.wantFlag)
			}
		})
	}
}

// TestBuildPreparedStartBuiltinClaudeInvalidOptModelDegradesWithLog is the
// executable form of DO step 3 (negative test): an unrecognized opt_model is
// dropped per-key (no --model flag reaches the launch command, so the session
// falls back to the agent default model) AND a log line records the skip — a
// typo must never silently pass through as a literal --model argument.
func TestBuildPreparedStartBuiltinClaudeInvalidOptModelDegradesWithLog(t *testing.T) {
	const bogus = "claude-opus-9000" // a real-looking model id that is NOT a valid choice key

	var logBuf bytes.Buffer
	prevFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(prevFlags)
	})

	candidate, cfg, store := newOptionSessionWithWork(
		t, claudeEffortResolvedProvider(), "claude",
		map[string]string{"model": bogus},
	)

	// Degrade: the invalid value is dropped, so no model override survives.
	overrides := resolveTaskOptionOverrides(store, claudeEffortResolvedProvider(), taskWorkDirAssignees(candidate, cfg)...)
	if _, present := overrides["model"]; present {
		t.Fatalf("invalid opt_model survived resolution: overrides=%v", overrides)
	}

	prepared, err := buildPreparedStart(candidate, cfg, store)
	if err != nil {
		t.Fatalf("buildPreparedStart: %v", err)
	}
	if strings.Contains(prepared.cfg.Command, "--model") {
		t.Fatalf("command %q must not carry a --model flag for an invalid opt_model", prepared.cfg.Command)
	}
	if strings.Contains(prepared.cfg.Command, bogus) {
		t.Fatalf("command %q leaked the bogus opt_model value", prepared.cfg.Command)
	}

	// Log line: the skip is observable, not swallowed.
	if logged := logBuf.String(); !strings.Contains(logged, "ignoring opt_model=") || !strings.Contains(logged, bogus) {
		t.Fatalf("expected a log line recording the ignored opt_model=%q, got: %q", bogus, logged)
	}
}
