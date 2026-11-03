# Session Module

Read `REQUIREMENTS.md` before changing session behavior, tests, or callers that
depend on session lifecycle semantics. Read `PLAN.md` before changing session
ownership boundaries, deciders, command APIs, events, or extraction sequencing.
(`DESIGN.md` is the long-form architecture document; it has not landed yet and
is iterating under design-review bead `ga-unpr2y`.)

## Purpose

`internal/session` owns the session primitive: start, stop, prompt, observe,
identify, and project provider-backed sessions. Session state is bead-backed;
runtime state is observed through providers; work must survive session churn.

## Reconcile Rule

When code, tests, or docs disagree with `REQUIREMENTS.md`, do not silently pick
one. Reconcile them in the same change:

- If the requirement is right, update code and add or adjust tests.
- If current behavior is right, update `REQUIREMENTS.md` and cite proof.
- If the mismatch is ambiguous, stop and ask for the product rule.

New session behavior needs at least one scenario row in `REQUIREMENTS.md` with
evidence from tests, source, an issue, or a commit.

## Boundaries

- Use `ProjectLifecycle` and lifecycle helper APIs for read-side decisions.
- Inside `internal/session`, lifecycle transition/patch helpers may build
  mutations. Outside `internal/session`, call session-owned command APIs instead
  of applying patch maps or writing lifecycle metadata directly.
- Treat duplicated session decisions in API, CLI, worker, or runtime adapters as
  extraction candidates. Prefer moving the rule into `internal/session` and
  leaving the adapter to gather facts, execute actions, or render responses.
- Extract decisions one cluster at a time. Do not introduce a broad session
  facade before a narrow contract and tests prove it.
- Do not add production session lifecycle or identity metadata writes outside
  `internal/session`. Existing external `SetMetadata*`/bead mutation sites are
  extraction candidates unless they are tests, fixtures, or migration/doctor
  repair code.
- Keep provider-specific runtime behavior behind `internal/runtime`.
- Keep production CLI session creation and lifecycle operations on the
  `internal/worker/handle.go` boundary unless root `AGENTS.md` names an active
  exception.
- Do not make ordinary config names or `template:<name>` values act like live
  session targets.

## Startup Fencing Pitfalls

- `Store.Tx` groups rollback writes but cannot read, so it does not fence a
  stale creating snapshot against completion. `WithPendingCreateRollback` and
  `CommitStartedIfCurrent` share `WithSessionMutationLock` with incarnation
  allocation. Keep the fresh read and write inside that lock; provider calls
  and retired-session cleanup stay outside. This is an in-process fence, not
  protection against arbitrary external bead writers.
- `testing/synctest.Wait` does not consider `sync.Mutex.Lock` durably blocked.
  A test that pauses a lock holder on a channel and waits for a competing
  mutex waiter will hang. `store_start_fence_test.go` checks the shared lock
  directly at both read and write boundaries instead of relying on timing.
- `awake` is not proof that creation completed: the reconciler can heal a
  running, still-claimed creation to awake before provider Start returns
  (issue #6104 / PR #6105). Rollback must allow that state when its claim and
  identity remain current; active or a cleared claim still blocks rollback.
- A final atomic start commit may reject after the async refresh succeeded.
  Propagate that stale verdict to the provider-aware caller and clean up using
  the attempted identity outside the mutation lock. Persistence failure alone
  must not trigger runtime cleanup.
- Interactive attach checks lifecycle before start and after the provider
  terminal boundary. It must release the per-session mutation lock while the
  terminal is attached, then reject success if the lifecycle or runtime
  identity changed. Controller drain stops and confirm-dead re-kills recheck
  the typed session generation, instance token, and drain state under the
  process-local mutation lock, then require the provider's identity-conditional
  stop to compare and stop the same runtime incarnation across processes.
  A provider without that capability must fail closed. The instance token
  alone cannot identify a replacement runtime because it is session-scoped.
- A legacy runtime discovered during adoption has no safe provider-owned
  binding to the newly created bead ID and generation unless the provider can
  bind its immutable runtime handle atomically. Preserve its existing token,
  mark the adoption unbound through the session create front door, and block
  automatic conditional drain with an actionable unsupported-capability
  error. Never fill provider ID/epoch metadata with sequential name-based
  reads and writes.
- Direct `gc session new` starts are fresh provider conversations even when
  the provider accepts a generated session key. Route their rendered startup
  prompt through the canonical argv, flag, or nudge policy before Provider.Start
  and mark delivery only when a route was selected. A valid-key resume omits
  that initial prompt so it is not replayed into the existing conversation.
