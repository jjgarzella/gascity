package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/testutil"
)

type crossProcessRuntimeState struct {
	Generation           string `json:"generation"`
	InstanceToken        string `json:"instance_token"`
	Running              bool   `json:"running"`
	ReplacementLockOwned bool   `json:"replacement_lock_owned"`
	StoppedGeneration    string `json:"stopped_generation"`
}

type crossProcessConditionalProvider struct {
	*runtime.Fake
	statePath string
	sessionID string
	once      sync.Once
	childErr  error
}

func (p *crossProcessConditionalProvider) Stop(name string) error {
	state, err := readCrossProcessRuntimeState(p.statePath)
	if err != nil {
		return err
	}
	state.StoppedGeneration = state.Generation
	state.Running = false
	if err := writeCrossProcessRuntimeState(p.statePath, state); err != nil {
		return err
	}
	return p.Fake.Stop(name)
}

func (p *crossProcessConditionalProvider) StopIfCurrent(name string, identity runtime.Identity) (bool, error) {
	// The helper process enters the same session mutation lock after the
	// controller's typed-row check but before this provider compares identity.
	// A provider-owned compare-and-stop must observe the replacement and refuse.
	p.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testutil.ExecRaceTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrossProcessLifecycleReplacementHelper$")
		cmd.Env = append(os.Environ(),
			"GASCITY_LIFECYCLE_REPLACEMENT_HELPER=1",
			"GASCITY_LIFECYCLE_REPLACEMENT_STATE="+p.statePath,
			"GASCITY_LIFECYCLE_REPLACEMENT_SESSION="+p.sessionID,
		)
		if output, err := cmd.CombinedOutput(); err != nil {
			p.childErr = fmt.Errorf("replacement helper: %w: %s", err, strings.TrimSpace(string(output)))
		}
	})
	if p.childErr != nil {
		return false, p.childErr
	}
	state, err := readCrossProcessRuntimeState(p.statePath)
	if err != nil {
		return false, err
	}
	if state.Generation != identity.RuntimeEpoch || state.InstanceToken != identity.InstanceToken {
		// Mirror the external provider's new runtime in Fake so assertions see
		// a live replacement after the stale stop is refused.
		if err := p.Fake.Stop(name); err != nil {
			return false, err
		}
		if err := p.Start(context.Background(), name, runtime.Config{Env: map[string]string{
			"GC_SESSION_ID": p.sessionID, "GC_RUNTIME_EPOCH": state.Generation, "GC_INSTANCE_TOKEN": state.InstanceToken,
		}}); err != nil {
			return false, err
		}
		for key, value := range map[string]string{
			"GC_SESSION_ID": p.sessionID, "GC_RUNTIME_EPOCH": state.Generation, "GC_INSTANCE_TOKEN": state.InstanceToken,
		} {
			if err := p.SetMeta(name, key, value); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	return p.Fake.StopIfCurrent(name, identity)
}

func TestCrossProcessLifecycleReplacementHelper(t *testing.T) {
	if os.Getenv("GASCITY_LIFECYCLE_REPLACEMENT_HELPER") != "1" {
		return
	}
	statePath := os.Getenv("GASCITY_LIFECYCLE_REPLACEMENT_STATE")
	sessionID := os.Getenv("GASCITY_LIFECYCLE_REPLACEMENT_SESSION")
	if statePath == "" || sessionID == "" {
		t.Fatal("replacement helper is missing its state path or session ID")
	}
	if err := session.WithSessionMutationLock(sessionID, func() error {
		state, err := readCrossProcessRuntimeState(statePath)
		if err != nil {
			return err
		}
		state.Generation = "2"
		state.InstanceToken = "replacement-token"
		state.Running = true
		state.ReplacementLockOwned = true
		return writeCrossProcessRuntimeState(statePath, state)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStopDrainAckRuntimeIfCurrentRefusesCrossProcessReplacement(t *testing.T) {
	store := beads.NewMemStore()
	provider := &crossProcessConditionalProvider{Fake: runtime.NewFake(), statePath: filepath.Join(t.TempDir(), "runtime.json")}
	mgr := session.NewManagerWithOptions(store, provider)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "worker", Command: "test-cmd", WorkDir: t.TempDir(), Provider: "test",
		Resume: session.ProviderResume{}, Hints: runtime.Config{},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	provider.sessionID = info.ID
	if err := writeCrossProcessRuntimeState(provider.statePath, crossProcessRuntimeState{
		Generation: info.Generation, InstanceToken: info.InstanceToken, Running: true,
	}); err != nil {
		t.Fatalf("write original runtime identity: %v", err)
	}
	if err := store.SetMetadataBatch(info.ID, map[string]string(session.DrainAckStopPendingPatch(time.Now().UTC()))); err != nil {
		t.Fatalf("mark drain stop pending: %v", err)
	}

	stopped, err := stopDrainAckRuntimeIfCurrent("", store, provider, &config.City{}, info.ID, info.SessionName, info.InstanceToken, info.Generation)
	if err != nil {
		t.Fatalf("stop drain-ack runtime: %v", err)
	}
	if stopped {
		t.Fatal("stale conditional stop reported the replacement as stopped")
	}
	if provider.childErr != nil {
		t.Fatalf("replacement helper process: %v", provider.childErr)
	}
	state, err := readCrossProcessRuntimeState(provider.statePath)
	if err != nil {
		t.Fatalf("read runtime state: %v", err)
	}
	if !state.ReplacementLockOwned || !state.Running || state.StoppedGeneration != "" {
		t.Fatalf("cross-process replacement state = %+v; want live generation 2 and no stop", state)
	}
	if !provider.IsRunning(info.SessionName) {
		t.Fatal("cross-process replacement runtime was killed")
	}
}

func readCrossProcessRuntimeState(path string) (crossProcessRuntimeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return crossProcessRuntimeState{}, err
	}
	var state crossProcessRuntimeState
	if err := json.Unmarshal(data, &state); err != nil {
		return crossProcessRuntimeState{}, err
	}
	return state, nil
}

func writeCrossProcessRuntimeState(path string, state crossProcessRuntimeState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func TestQueueDrainAckAsyncStopSkipsReplacementGeneration(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	mgr := session.NewManagerWithOptions(store, sp)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "worker", Command: "test-cmd", WorkDir: t.TempDir(), Provider: "test",
		Resume: session.ProviderResume{}, Hints: runtime.Config{},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if info.InstanceToken == "" || info.Generation == "" {
		t.Fatalf("session identity = token %q generation %q, want both present", info.InstanceToken, info.Generation)
	}
	if err := sp.SetMeta(info.SessionName, "GC_INSTANCE_TOKEN", info.InstanceToken); err != nil {
		t.Fatalf("set original runtime token: %v", err)
	}
	// Keep the exact runtime epoch from the queued generation while the typed
	// stop-pending bead advances. This isolates the bead-generation fence.
	if err := sp.SetMeta(info.SessionName, "GC_RUNTIME_EPOCH", "1"); err != nil {
		t.Fatalf("set original runtime epoch: %v", err)
	}
	patch := map[string]string(session.DrainAckStopPendingPatch(time.Now().UTC()))
	patch["generation"] = "2"
	if err := store.SetMetadataBatch(info.ID, patch); err != nil {
		t.Fatalf("update replacement bead: %v", err)
	}

	tracker := &asyncStartTracker{}
	queueDrainAckAsyncStopForGeneration("", store, sp, &config.City{}, info.ID, info.SessionName, info.InstanceToken, info.Generation, nil, tracker, nil)
	if !tracker.wait(time.Second) {
		t.Fatal("async drain stop did not complete")
	}
	if !sp.IsRunning(info.SessionName) {
		t.Fatal("stale drain stop killed a runtime whose session bead advanced to a new generation")
	}
	if got, err := sp.GetMeta(info.SessionName, "GC_RUNTIME_EPOCH"); err != nil || got != "1" {
		t.Fatalf("runtime epoch = %q, %v; want 1", got, err)
	}
	bead, err := store.Get(info.ID)
	if err != nil {
		t.Fatalf("read replacement session bead: %v", err)
	}
	if bead.Metadata["generation"] != "2" || bead.Metadata["state"] != string(session.StateDraining) {
		t.Fatalf("replacement bead was changed by stale stop: generation=%q state=%q", bead.Metadata["generation"], bead.Metadata["state"])
	}
}

type replacementOnStopProvider struct {
	*runtime.Fake
	sessionID string
	token     string
	stops     int
}

func (p *replacementOnStopProvider) StopIfCurrent(name string, identity runtime.Identity) (bool, error) {
	p.stops++
	stopped, err := p.Fake.StopIfCurrent(name, identity)
	if err != nil || !stopped {
		return stopped, err
	}
	if p.stops == 1 {
		if err := p.Start(context.Background(), name, runtime.Config{Env: map[string]string{
			"GC_SESSION_ID":     p.sessionID,
			"GC_INSTANCE_TOKEN": p.token,
			"GC_RUNTIME_EPOCH":  "2",
		}}); err != nil {
			return false, err
		}
		if err := p.SetMeta(name, "GC_RUNTIME_EPOCH", "2"); err != nil {
			return false, err
		}
		if err := p.SetMeta(name, "GC_INSTANCE_TOKEN", p.token); err != nil {
			return false, err
		}
	}
	return true, nil
}

func TestQueueDrainAckAsyncStopDoesNotRekillReplacementRuntimeEpoch(t *testing.T) {
	store := beads.NewMemStore()
	fake := runtime.NewFake()
	sp := &replacementOnStopProvider{Fake: fake}
	mgr := session.NewManagerWithOptions(store, sp)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "worker", Command: "test-cmd", WorkDir: t.TempDir(), Provider: "test",
		Resume: session.ProviderResume{}, Hints: runtime.Config{},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	sp.sessionID = info.ID
	sp.token = info.InstanceToken
	patch := map[string]string(session.DrainAckStopPendingPatch(time.Now().UTC()))
	if err := store.SetMetadataBatch(info.ID, patch); err != nil {
		t.Fatalf("mark drain stop pending: %v", err)
	}
	if err := fake.SetMeta(info.SessionName, "GC_RUNTIME_EPOCH", info.Generation); err != nil {
		t.Fatalf("set original runtime epoch: %v", err)
	}
	if err := fake.SetMeta(info.SessionName, "GC_SESSION_ID", info.ID); err != nil {
		t.Fatalf("set original runtime session id: %v", err)
	}
	if err := fake.SetMeta(info.SessionName, "GC_INSTANCE_TOKEN", info.InstanceToken); err != nil {
		t.Fatalf("set original runtime token: %v", err)
	}
	tracker := &asyncStartTracker{}
	queueDrainAckAsyncStopForGeneration("", store, sp, &config.City{}, info.ID, info.SessionName, info.InstanceToken, info.Generation, nil, tracker, nil)
	if !tracker.wait(time.Second) {
		t.Fatal("async drain stop did not complete")
	}
	if !fake.IsRunning(info.SessionName) {
		t.Fatal("replacement runtime was not left running")
	}
	if got, err := fake.GetMeta(info.SessionName, "GC_RUNTIME_EPOCH"); err != nil || got != "2" {
		t.Fatalf("replacement runtime epoch = %q, %v; want 2", got, err)
	}
	bead, err := store.Get(info.ID)
	if err != nil {
		t.Fatalf("read drain-pending bead: %v", err)
	}
	if bead.Metadata["generation"] != info.Generation || bead.Metadata["state"] != string(session.StateDraining) {
		t.Fatalf("stale stop changed drain-pending bead: generation=%q state=%q", bead.Metadata["generation"], bead.Metadata["state"])
	}
}

type replacementBeforeConditionalStopProvider struct {
	*runtime.Fake
	replaceBeforeStop bool
}

func (p *replacementBeforeConditionalStopProvider) StopIfCurrent(name string, identity runtime.Identity) (bool, error) {
	if p.replaceBeforeStop {
		p.replaceBeforeStop = false
		if err := p.Stop(name); err != nil {
			return false, err
		}
		if err := p.Start(context.Background(), name, runtime.Config{}); err != nil {
			return false, err
		}
		if err := p.SetMeta(name, "GC_SESSION_ID", "replacement-session"); err != nil {
			return false, err
		}
		if err := p.SetMeta(name, "GC_INSTANCE_TOKEN", "replacement-token"); err != nil {
			return false, err
		}
		if err := p.SetMeta(name, "GC_RUNTIME_EPOCH", "2"); err != nil {
			return false, err
		}
	}
	return p.Fake.StopIfCurrent(name, identity)
}

func TestStopDrainAckRuntimeIfCurrentDoesNotKillReplacementBetweenIdentityReadAndStop(t *testing.T) {
	store := beads.NewMemStore()
	fake := runtime.NewFake()
	sp := &replacementBeforeConditionalStopProvider{Fake: fake}
	mgr := session.NewManagerWithOptions(store, sp)
	info, err := mgr.CreateSession(context.Background(), session.CreateOptions{
		Template: "worker", Command: "test-cmd", WorkDir: t.TempDir(), Provider: "test",
		Resume: session.ProviderResume{}, Hints: runtime.Config{},
		ExtraMeta: map[string]string{"session_origin": "manual"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := fake.SetMeta(info.SessionName, "GC_SESSION_ID", info.ID); err != nil {
		t.Fatalf("set session id: %v", err)
	}
	if err := fake.SetMeta(info.SessionName, "GC_INSTANCE_TOKEN", info.InstanceToken); err != nil {
		t.Fatalf("set original runtime token: %v", err)
	}
	if err := fake.SetMeta(info.SessionName, "GC_RUNTIME_EPOCH", info.Generation); err != nil {
		t.Fatalf("set original runtime epoch: %v", err)
	}
	if err := store.SetMetadataBatch(info.ID, map[string]string(session.DrainAckStopPendingPatch(time.Now().UTC()))); err != nil {
		t.Fatalf("mark drain stop pending: %v", err)
	}
	sp.replaceBeforeStop = true

	stopped, err := stopDrainAckRuntimeIfCurrent("", store, sp, &config.City{}, info.ID, info.SessionName, info.InstanceToken, info.Generation)
	if err != nil {
		t.Fatalf("conditional drain stop: %v", err)
	}
	if stopped {
		t.Fatal("stop reported the observed incarnation as stopped after a same-name replacement was installed")
	}
	if !fake.IsRunning(info.SessionName) {
		t.Fatal("drain stop killed the replacement installed after its identity reads")
	}
	if got, err := fake.GetMeta(info.SessionName, "GC_SESSION_ID"); err != nil || got != "replacement-session" {
		t.Fatalf("replacement session id = %q, %v; want replacement-session", got, err)
	}
}
