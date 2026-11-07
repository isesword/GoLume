# Charged prepared execution reuse (WIP checkpoint)

This checkpoint retains only owned fixed-width resident aggregate state and
bounded native partition/wave scratch. It does not restore a legacy executor,
retain source batches, or size scratch from total input rows or partitions.
The same native aggregation, state reset, NULL handling and result collector
are used on a cold or reused execution.

`engine.Options.PreparedCacheBytes` caps idle state across the whole engine.
`DefaultOptions` chooses 64 MiB; zero disables retention. The effective cap is
also no greater than a finite engine memory limit. Negative limits fail Prepare.
Each prepared plan can retain at most its permitted execution concurrency.
The cache evicts its least recently returned idle entry on admission pressure,
and engine-root allocation pressure can reclaim idle entries from other plans.
Active state is never evicted. An entry larger than the cap is freed instead.

Eligibility is intentionally narrow: internal query budgets created by repeated
PreparedQuery.Execute, resident hash strategies, and fixed-width group keys.
String and dictionary keys, sorted/spill strategies, public BatchStream
constructors, all externally supplied QueryBudgets and one-shot Engine.Execute
bypass reuse. Predicate workspaces, source buffers, output buffers and materialized
results are never cached. Caller-owned result rows remain independent of resets.

## Continuous accounting and ownership

A reusable entry owns two stable memory.ReusableAccounts: state and workspace.
Each allocation obtains its reservation from the appropriate account before
allocation. Table component tokens retain that account for their entire lifetime.
Detach removes query/operator ownership but leaves every byte charged at the
engine root. Attach admits the complete retained capacity into the next query's
hard cap without releasing/re-reserving root memory or allocating replacement
tokens. It cannot silently use a different root or bypass a smaller query limit.
A failure attaching either account destroys the exclusive entry before trying
the ordinary cold path. Buffers are dropped before their tokens are released.

No prior query, execution context, operator, source binding or output view remains
in an idle entry. Allocation callbacks refer to stable lanes whose current
context is cleared on detach. Partitioned-state plan/query fields are cleared.
No string arena is admitted, and only numeric owned backing arrays survive.

The global cache cap is enforced with admission credits held throughout detach,
checkout, and eviction. An entry remains credited until transfer completes or
all backing buffers have been destroyed. Thus overlapping returns and root
reclaimers cannot exceed the cap by making an in-transit entry invisible.
Cache locks are not held across account attachment, reservation or destruction.

Cancelled, failed, or incompletely consumed executions free their state.
PreparedQuery.Close rejects new work, waits accepted executions, purges its
idle entries, and joins in-flight eviction of those entries. Engine.Close waits
accepted consumers, purges all idle state including in-flight pressure victims,
unregisters reclamation and closes the scheduler. All concurrent Close callers
wait for the same cleanup. External caller-owned outputs are not invalidated.

## Diagnostics and scope

Engine.Memory().Used() can remain nonzero after a successful prepared execution
because idle buffers are still genuinely live and charged. Its completed query's
Used() is zero. Engine.PreparedCacheStats() exposes effective cap, admission
credits, available idle bytes/entries, hits, misses and evictions. Active query
peaks include full checked-out capacity, even if a particular execution uses
fewer groups. Cache-disabled tests continue to require zero execution retention.

As elsewhere, this is an explicit owned-buffer budget, not a whole-Go-runtime
RSS promise: fixed control objects, closure objects and reservation objects are
not estimated as payload buffers. All capacity-sized state, wave, histogram,
selection and pointer-array backings are charged. The cache itself uses intrusive
links rather than an uncharged array proportional to retained token count.

Performance acceptance remains open. This isolated candidate must be evaluated
as WIP, reuse-only, shared-kernel-only and combined controls on identical budgets,
input and concurrency. Correctness or a cache hit is not evidence of passing the
original end-to-end regression gate.

Current measured status: combined fixed 300-execution diagnostic median 13.338 ms
versus 17.309 ms for WIP 24d742d, but still above the prior exact 965 commit’s 10.04 ms formal
reference and 10.341 ms 3% limit. Full combined validation and independent scoped
reviews pass; formal overall performance acceptance remains open. Raw evidence
and reproducibility instructions are in
[validation/charged-reuse](../validation/charged-reuse/README.md).
