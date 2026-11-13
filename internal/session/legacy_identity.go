package session

import (
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
)

// retireLegacyIdentityAfterStart is reached only after this invocation started
// a NEW runtime and completed its startup checks, never on convergence or reads.
// The caller holds the existing per-session mutation lock. Requested launch Env
// is insufficient: the provider must report the identity it actually owns.
func (m *Manager) retireLegacyIdentityAfterStart(b beads.Bead, name string) error {
	if b.Metadata[MetadataRuntimeIdentityDisposition] != string(RuntimeIdentityUnboundLegacy) {
		return nil
	}
	expected := infoFromPersistedBead(b)
	if strings.TrimSpace(expected.InstanceToken) == "" {
		return nil
	}
	for key, want := range map[string]string{
		"GC_SESSION_ID":     expected.ID,
		"GC_RUNTIME_EPOCH":  CanonicalRuntimeGeneration(expected.Generation),
		"GC_INSTANCE_TOKEN": strings.TrimSpace(expected.InstanceToken),
	} {
		got, err := m.sp.GetMeta(name, key)
		if err != nil || got != want {
			return nil // unsupported or unbound: retain the fail-closed disposition
		}
	}
	front := NewStore(beads.SessionStore{Store: m.store})
	if _, err := front.retireLegacyRuntimeIdentityLocked(expected); err != nil {
		// Persistence failure does not authorize tearing down a running runtime.
		return fmt.Errorf("%w: retiring legacy runtime identity: %w", ErrStateSync, err)
	}
	return nil
}

// retireLegacyRuntimeIdentityLocked commits only an attested new incarnation.
// The caller must hold WithSessionMutationLock and have verified the provider's
// full identity after a genuinely new start. Unlike a pending-create lease, the
// session-scoped token alone is insufficient: generation must also still match.
// Conditional stores additionally fence writers outside the process-local lock;
// stores without that capability retain the existing external-writer window.
func (s *Store) retireLegacyRuntimeIdentityLocked(expected Info) (bool, error) {
	b, err := s.validatedBead(expected.ID)
	if err != nil {
		return false, err
	}
	current := infoFromPersistedBead(b)
	if current.ID != expected.ID || current.Closed ||
		current.SessionName != expected.SessionName ||
		CanonicalRuntimeGeneration(current.Generation) != CanonicalRuntimeGeneration(expected.Generation) ||
		strings.TrimSpace(current.InstanceToken) != strings.TrimSpace(expected.InstanceToken) ||
		!sameLifecycleFacts(expected, current) ||
		b.Metadata[MetadataRuntimeIdentityDisposition] != string(RuntimeIdentityUnboundLegacy) {
		return false, nil
	}
	patch := MetadataPatch{MetadataRuntimeIdentityDisposition: ""}
	writer, _, err := beads.ResolveConditionalWriter(s.store.Store)
	if err != nil {
		return false, err
	}
	if writer == nil {
		if err := s.ApplyPatch(expected.ID, patch); err != nil {
			return false, err
		}
		return true, nil
	}
	err = writer.UpdateIfMatch(expected.ID, b.Revision, beads.UpdateOpts{Metadata: map[string]string(patch)})
	if beads.IsPreconditionFailed(err) {
		return false, nil
	}
	return err == nil, err
}
