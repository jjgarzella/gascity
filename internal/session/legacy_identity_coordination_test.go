package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
)

// Scripted provider-port facts prove manager coordination, not real runtime
// binding. The tagged test/integration/sessionidentity owner reads actual ACP
// sidecars. No process, socket or provider metadata write occurs in this double.
type legacyCoordinationProvider struct {
	*runtime.Fake
	values       map[string]string
	firstError   error
	exists       bool
	starts       int
	attestations []string
}

func (p *legacyCoordinationProvider) Start(ctx context.Context, name string, cfg runtime.Config) error {
	p.starts++
	if p.starts == 1 && p.firstError != nil {
		return p.firstError
	}
	if err := p.Fake.Start(ctx, name, cfg); err != nil {
		return err
	}
	if p.exists {
		return runtime.ErrSessionExists
	}
	return nil
}

func (p *legacyCoordinationProvider) GetMeta(name, key string) (string, error) {
	if value, ok := p.values[key]; ok {
		p.attestations = append(p.attestations, key)
		return value, nil
	}
	return p.Fake.GetMeta(name, key)
}

func legacyScriptedProvider(info Info) *legacyCoordinationProvider {
	return &legacyCoordinationProvider{Fake: runtime.NewFake(), values: map[string]string{
		"GC_SESSION_ID": info.ID, "GC_RUNTIME_EPOCH": info.Generation, "GC_INSTANCE_TOKEN": info.InstanceToken,
	}}
}

// Fail only the identity-disposition commit, allowing normal Start to complete
// its lifecycle state sync before exercising the retirement error boundary.
type legacyMarkerFailureStore struct{ beads.Store }

func (s legacyMarkerFailureStore) SetMetadataBatch(id string, patch map[string]string) error {
	if _, ok := patch[MetadataRuntimeIdentityDisposition]; ok {
		return errors.New("scripted retirement write failure")
	}
	return s.Store.SetMetadataBatch(id, patch)
}

func TestLegacyBoundStartCoordination(t *testing.T) {
	for _, runtimeOnly := range []bool{false, true} {
		for _, mode := range []string{"new-start", "startup-death-retry", "post-wait-retry"} {
			t.Run(fmt.Sprintf("%s/runtime-only=%t", mode, runtimeOnly), func(t *testing.T) {
				store := beads.NewMemStore()
				info := legacyAdoptionForTest(t, store)
				provider := legacyScriptedProvider(info)
				waits := 0
				mgr := NewManagerWithOptions(store, provider, WithStaleKeyDetectionWaiter(func(_ context.Context, name string) error {
					waits++
					return provider.Stop(name) // scripted death after the resume wait
				}))
				command := "scripted-command"
				if mode != "new-start" {
					if err := store.SetMetadataBatch(info.ID, map[string]string{"session_key": "stale", "resume_flag": "--resume"}); err != nil {
						t.Fatal(err)
					}
					command += " --resume stale"
				}
				if mode == "startup-death-retry" {
					provider.firstError = runtime.ErrSessionDiedDuringStartup
				}
				start := mgr.Start
				if runtimeOnly {
					start = mgr.StartRuntimeOnly
				}
				if err := start(context.Background(), info.ID, command, runtime.Config{}); err != nil {
					t.Fatal(err)
				}
				wantStarts := 1
				if mode != "new-start" {
					wantStarts = 2
				}
				wantWaits := 0
				if mode == "post-wait-retry" {
					wantWaits = 1
				}
				if provider.starts != wantStarts || waits != wantWaits || len(provider.attestations) != 3 {
					t.Fatalf("starts=%d waits=%d attestations=%v; want starts=%d waits=%d and full tuple", provider.starts, waits, provider.attestations, wantStarts, wantWaits)
				}
				current, persisted, err := NewStore(beads.SessionStore{Store: store}).GetPersistedResponse(info.ID)
				if err != nil {
					t.Fatal(err)
				}
				if persisted.Metadata[MetadataRuntimeIdentityDisposition] != "" {
					t.Fatal("successful scripted bound-start coordination retained legacy disposition")
				}
				if !provider.IsRunning(info.SessionName) {
					t.Fatal("successful startup lost its runtime")
				}
				cfg := provider.LastStartConfig(info.SessionName)
				for key, want := range provider.values {
					if cfg.Env[key] != want {
						t.Fatalf("requested identity %s=%q; want %q", key, cfg.Env[key], want)
					}
				}
				if mode != "new-start" && (current.SessionKey != "" || strings.Contains(cfg.Command, "--resume")) {
					t.Fatalf("fresh recovery retained stale key or resume command: key=%q command=%q", current.SessionKey, cfg.Command)
				}
				if runtimeOnly {
					if current.MetadataState != "asleep" {
						t.Fatalf("runtime-only committed lifecycle completion %q", current.MetadataState)
					}
					if applied, err := NewStore(beads.SessionStore{Store: store}).CommitStartedIfCurrent(current, MetadataPatch{"state": "active"}); err != nil || !applied {
						t.Fatalf("controller completion=%t,%v", applied, err)
					}
				} else if current.MetadataState != "active" {
					t.Fatalf("normal Start state=%q", current.MetadataState)
				}
			})
		}
	}
}

func TestLegacyRetirementPersistenceFailureAndConvergence(t *testing.T) {
	for _, runtimeOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(runtimeOnly), func(t *testing.T) {
			store := beads.NewMemStore()
			info := legacyAdoptionForTest(t, store)
			provider := legacyScriptedProvider(info)
			mgr := NewManagerWithOptions(legacyMarkerFailureStore{Store: store}, provider)
			start := mgr.Start
			if runtimeOnly {
				start = mgr.StartRuntimeOnly
			}
			if err := start(context.Background(), info.ID, "scripted-command", runtime.Config{}); !errors.Is(err, ErrStateSync) {
				t.Fatalf("retirement error=%v; want ErrStateSync", err)
			}
			if provider.starts != 1 || len(provider.attestations) != 3 || !provider.IsRunning(info.SessionName) {
				t.Fatal("failed marker commit did not retain the successfully started runtime")
			}
			for _, call := range provider.Calls {
				if call.Method == "Stop" {
					t.Fatal("persistence failure authorized teardown")
				}
			}
			mgr.store = store
			if err := start(context.Background(), info.ID, "scripted-command", runtime.Config{}); err != nil {
				t.Fatal(err)
			}
			current, err := store.Get(info.ID)
			if err != nil {
				t.Fatal(err)
			}
			if current.Metadata[MetadataRuntimeIdentityDisposition] != string(RuntimeIdentityUnboundLegacy) || provider.starts != 1 || len(provider.attestations) != 3 {
				t.Fatal("later convergence retried retirement or cleared protection")
			}
			identity := runtime.Identity{SessionID: info.ID, RuntimeEpoch: info.Generation, InstanceToken: info.InstanceToken}
			if stopped, err := mgr.KillRuntimeIfCurrent(info.ID, identity); stopped || !errors.Is(err, runtime.ErrConditionalStopUnsupported) {
				t.Fatalf("retained refusal=%t,%v", stopped, err)
			}
		})
	}
}

func TestLegacyExistingRuntimeConvergenceDoesNotRetire(t *testing.T) {
	for _, runtimeOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(runtimeOnly), func(t *testing.T) {
			store := beads.NewMemStore()
			info := legacyAdoptionForTest(t, store)
			provider := legacyScriptedProvider(info)
			provider.exists = true // runtime appeared between preflight and Start
			mgr := NewManagerWithOptions(store, provider)
			start := mgr.Start
			if runtimeOnly {
				start = mgr.StartRuntimeOnly
			}
			err := start(context.Background(), info.ID, "scripted-command", runtime.Config{})
			if runtimeOnly && !errors.Is(err, runtime.ErrSessionExists) || !runtimeOnly && err != nil {
				t.Fatalf("ErrSessionExists convergence=%v", err)
			}
			current, err := store.Get(info.ID)
			if err != nil {
				t.Fatal(err)
			}
			if provider.starts != 1 || len(provider.attestations) != 0 || current.Metadata[MetadataRuntimeIdentityDisposition] != string(RuntimeIdentityUnboundLegacy) {
				t.Fatal("existing runtime was treated as a new bound start")
			}
		})
	}
}
