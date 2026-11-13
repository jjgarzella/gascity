package session

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/rollout/gate"
	"github.com/gastownhall/gascity/internal/runtime"
)

func legacyAdoptionForTest(t *testing.T, store beads.Store) Info {
	t.Helper()
	info, err := NewStore(beads.SessionStore{Store: store}).CreateSessionInfo(CreateSpec{
		Title: "legacy", AgentName: "test-agent", RuntimeIdentityDisposition: RuntimeIdentityUnboundLegacy,
		Metadata: map[string]string{
			"session_name": "legacy-replacement", "template": "test-agent", "state": "asleep",
			"provider": "test-provider", "transport": "acp", "work_dir": "/private/work",
			"generation": "2", "instance_token": "legacy-token",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// These are durable-commit inputs, not fabricated provider bindings. The tagged integration
// ACP owner separately proves the provider attestation that permits entry.
func TestLegacyIdentityRetirementFencesCurrentIncarnation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		patch  MetadataPatch
		closed bool
		want   bool
	}{
		{name: "current", want: true},
		{name: "new-generation-same-token", patch: MetadataPatch{"generation": "3"}},
		{name: "new-token", patch: MetadataPatch{"instance_token": "replacement"}},
		{name: "different-runtime-name", patch: MetadataPatch{"session_name": "replacement"}},
		{name: "suspended", patch: MetadataPatch{"state": "suspended"}},
		{name: "claim-retired", patch: MetadataPatch{"pending_create_claim": "true"}},
		{name: "closed", closed: true},
		{name: "unknown-disposition", patch: MetadataPatch{MetadataRuntimeIdentityDisposition: "future-protection"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backing := beads.NewMemStore()
			expected := legacyAdoptionForTest(t, backing)
			if err := backing.SetMetadataBatch(expected.ID, tc.patch); err != nil {
				t.Fatal(err)
			}
			if tc.closed {
				if err := backing.Close(expected.ID); err != nil {
					t.Fatal(err)
				}
			}
			store := &startFenceStore{Store: backing}
			store.check = func(id string) {
				t.Helper()
				sessionMutationLocksMu.Lock()
				entry := sessionMutationLocks[id]
				sessionMutationLocksMu.Unlock()
				if entry == nil {
					t.Fatal("retirement did not acquire the session mutation lock")
				}
				if entry.mu.TryLock() {
					entry.mu.Unlock()
					t.Fatal("retirement read/write escaped the mutation lock")
				}
			}
			front := NewStore(beads.SessionStore{Store: store})
			var applied bool
			err := WithSessionMutationLock(expected.ID, func() (err error) {
				applied, err = front.retireLegacyRuntimeIdentityLocked(expected)
				return err
			})
			if err != nil || applied != tc.want {
				t.Fatalf("retirement = %t, %v; want %t", applied, err, tc.want)
			}
			if store.reads != 1 || (store.writes == 1) != tc.want {
				t.Fatalf("reads=%d writes=%d", store.reads, store.writes)
			}
			current, err := backing.Get(expected.ID)
			if err != nil {
				t.Fatal(err)
			}
			if (current.Metadata[MetadataRuntimeIdentityDisposition] == "") != tc.want {
				t.Fatalf("unexpected disposition %q", current.Metadata[MetadataRuntimeIdentityDisposition])
			}
		})
	}
}

func TestLegacyIdentityRetirementRejectsWriteAfterReread(t *testing.T) {
	backing := beads.NewMemStore()
	if err := beads.StampOpenedStore(backing, "MemStore", gate.Auto, nil, nil); err != nil {
		t.Fatal(err)
	}
	expected := legacyAdoptionForTest(t, backing)
	racing := &afterGetStore{Store: backing, afterGet: func(id string) {
		if err := backing.SetMetadata(id, "generation", "3"); err != nil {
			t.Fatal(err)
		}
	}}
	front := NewStore(beads.SessionStore{Store: racing})
	if err := WithSessionMutationLock(expected.ID, func() error {
		applied, err := front.retireLegacyRuntimeIdentityLocked(expected)
		if applied || err != nil {
			t.Fatalf("superseded retirement = %t, %v", applied, err)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	current, err := backing.Get(expected.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Metadata[MetadataRuntimeIdentityDisposition] != string(RuntimeIdentityUnboundLegacy) {
		t.Fatal("superseded incarnation lost its protection")
	}
}

func TestLegacyControllerCommitDoesNotRetireDisposition(t *testing.T) {
	for _, superseded := range []bool{false, true} {
		t.Run(fmt.Sprintf("superseded=%t", superseded), func(t *testing.T) {
			store := beads.NewMemStore()
			expected := legacyAdoptionForTest(t, store)
			if superseded {
				if err := store.SetMetadata(expected.ID, "instance_token", "replacement"); err != nil {
					t.Fatal(err)
				}
			}
			front := NewStore(beads.SessionStore{Store: store})
			applied, err := front.CommitStartedIfCurrent(expected, MetadataPatch{"state": "active"})
			if err != nil || applied == superseded {
				t.Fatalf("controller commit = %t, %v", applied, err)
			}
			current, err := store.Get(expected.ID)
			if err != nil {
				t.Fatal(err)
			}
			if current.Metadata[MetadataRuntimeIdentityDisposition] != string(RuntimeIdentityUnboundLegacy) {
				t.Fatal("controller commit without a new bound start cleared legacy disposition")
			}
		})
	}
}

func TestLegacyStartWithoutNewBindingPreservesDisposition(t *testing.T) {
	for _, runtimeOnly := range []bool{false, true} {
		for _, mode := range []string{"no-attestation", "failed", "rejected", "orphan-blocked", "converged-old"} {
			t.Run(fmt.Sprintf("%s/runtime-only=%t", mode, runtimeOnly), func(t *testing.T) {
				store := beads.NewMemStore()
				info := legacyAdoptionForTest(t, store)
				fake := runtime.NewFake()
				var provider runtime.Provider = fake
				command := "fake-command"
				wantErr := false
				switch mode {
				case "failed":
					fake.StartErrors[info.SessionName] = errors.New("start rejected")
					wantErr = true
				case "rejected":
					command = ""
					wantErr = true
				case "orphan-blocked":
					orphans := &orphanScanProvider{Fake: fake}
					armUnconfirmedOrphan(orphans)
					provider = orphans
					wantErr = true
				case "converged-old":
					if err := fake.Start(context.Background(), info.SessionName, runtime.Config{}); err != nil {
						t.Fatal(err)
					}
				}
				mgr := NewManagerWithOptions(store, provider)
				start := mgr.Start
				if runtimeOnly {
					start = mgr.StartRuntimeOnly
				}
				if err := start(context.Background(), info.ID, command, runtime.Config{}); (err != nil) != wantErr {
					t.Fatalf("start error = %v; want error %t", err, wantErr)
				}
				current, err := store.Get(info.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.Metadata[MetadataRuntimeIdentityDisposition] != string(RuntimeIdentityUnboundLegacy) {
					t.Fatal("start without new provider binding cleared legacy disposition")
				}
			})
		}
	}
}
