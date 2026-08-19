# Phase 3 — Replication + Propose + Commit

**Goal:** Leaders `propose` commands; AppendEntries replicates logs; majority match advances `commit_index`; `Event::Committed` surfaces applied entries; `three_node` proposes and sees majority commit.

Plan: [`docs/superpowers/plans/2026-07-27-phase-3-replication.md`](superpowers/plans/2026-07-27-phase-3-replication.md)

---

## Delivered

| Area | Notes |
|------|--------|
| Follower AE | prev_log on every AE; conditional truncate; `last_new` commit clamp; Apply |
| Leader | `propose`, per-peer AE builder, pipeline depth 1, majority + current-term commit |
| Behaviour | `propose`, waker, `Event::Committed` |
| `three_node` | stable leader → propose `hello` → ≥2 nodes commit |

**Fig. 8 limitation:** No election no-op entry. A leader holding only prior-term entries will not advance `commit_index` until a current-term propose commits (covered by unit test).

---

## Done when

```bash
cargo test
cargo run --example echo_two_peers
cargo run --example three_node
```

---

## Out of scope

Snapshots, membership change, disk persistence, election no-op.

## Next

Phase 4 — snapshots / InstallSnapshot → [`phase-4.md`](phase-4.md).
