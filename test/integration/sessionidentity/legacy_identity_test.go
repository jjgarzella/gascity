//go:build integration

package sessionidentity

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/runtime/acp"
	"github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/testutil"
)

// One real session/ACP composition: the old adopted runtime is already gone.
// Entry/retry/persistence branches belong to the fast session coordination tests.
func TestLegacyAdoptionBoundReplacementIsDrainable(t *testing.T) {
	store := beads.NewMemStore()
	front := session.NewStore(beads.SessionStore{Store: store})
	info, err := front.CreateSessionInfo(session.CreateSpec{
		Title: "legacy", AgentName: "test-agent", RuntimeIdentityDisposition: session.RuntimeIdentityUnboundLegacy,
		Metadata: map[string]string{"session_name": "legacy-replacement", "template": "test-agent", "state": "asleep", "provider": "test-provider", "transport": "acp", "work_dir": t.TempDir(), "generation": "2", "instance_token": "legacy-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := acp.NewProviderWithDir(filepath.Join(testutil.ShortTempDir(t, "legacy-acp-"), "r"), acp.Config{HandshakeTimeout: 10 * time.Second})
	t.Cleanup(func() { // this fixture's private directory/name only
		if err := provider.Stop(info.SessionName); err != nil {
			t.Errorf("cleaning up private ACP runtime: %v", err)
		}
	})
	const command = `exec python3 -u -c 'import sys,json
for line in sys.stdin:
 m=json.loads(line)
 if "id" in m: print(json.dumps({"jsonrpc":"2.0","id":m["id"],"result":{"sessionId":"private-fake"}}),flush=True)
'`
	mgr := session.NewManagerWithOptions(store, provider)
	if err := mgr.Start(context.Background(), info.ID, command, runtime.Config{}); err != nil {
		t.Fatal(err)
	}
	current, persisted, err := front.GetPersistedResponse(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	identity := runtime.Identity{SessionID: current.ID, RuntimeEpoch: current.Generation, InstanceToken: current.InstanceToken}
	for key, want := range map[string]string{"GC_SESSION_ID": identity.SessionID, "GC_RUNTIME_EPOCH": identity.RuntimeEpoch, "GC_INSTANCE_TOKEN": identity.InstanceToken} {
		if got, err := provider.GetMeta(info.SessionName, key); err != nil || got != want {
			t.Fatalf("actual provider sidecar %s=%q,%v; want %q", key, got, err, want)
		}
	}
	if got := persisted.Metadata[session.MetadataRuntimeIdentityDisposition]; got != "" {
		t.Fatalf("fully bound NEW provider incarnation %+v still has disposition %q", identity, got)
	}
	if stopped, err := mgr.KillRuntimeIfCurrent(info.ID, identity); err != nil || !stopped {
		t.Fatalf("new bound incarnation stop=%t,%v; want true,nil", stopped, err)
	}
}
