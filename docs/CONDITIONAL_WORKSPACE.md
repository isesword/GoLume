# Fixed-width conditional workspace reuse

CASE, COALESCE and IF can now consume each demanded branch result immediately
instead of keeping every branch result live until a final gather. A qualifying
conditional owns one private scratch evaluation state. It never borrows the
parent's storage owners or exports a view of the scratch state.

## Eligibility and lifetime contract

The conditional result and every instruction result in each direct region must
be fixed-width. Region instructions must also be cache-free: borrowed fixed-width
columns and the instructions already permitted by the liveness planner qualify.
Nested conditionals, JSON decoding, variable-width values, dictionary predicate
caches and instruction-specific string caches do not qualify. An outer
conditional containing these keeps its existing dedicated-region/gather path;
eligible inner conditionals may still benefit. IN is unchanged.

Each branch retains its independently compiled CSE scope and local liveness plan.
The shared scratch state grows logical-register and dense-owner metadata only as
nonempty demand reaches a region. An owner may retain several separately typed
arrays across branches; no byte reinterpretation, input mutation or in-place
kernel execution is introduced. Fixed-width capacities therefore depend on the
largest encountered region and its encountered types, not on the number of arms.
This is not a claim that all expressions have constant memory: a larger individual
arm, more encountered types, or fallback regions can require more storage.

No branch result survives the next branch evaluation. CASE consumes the condition
mask before evaluating THEN, copies selected THEN rows immediately, and records a
choice even when the chosen value is NULL. COALESCE copies and records only the
first valid value. IF copies each selected arm. Output validity and Boolean truth
are cleared at the start of each evaluation. Selection masks have zero tail bits,
and bitmap iteration copies only demanded rows. Unselected rows never become
visible results. Shared exported roots remain owned by the parent workspace.

## Admission, failure and cancellation

The public workspace's existing complete-program replacement admission contract
is unchanged. Only private qualifying region scratch uses incremental admission.
Every metadata or typed-buffer growth reserves before allocating; replacement
charges old and new capacities concurrently until the old allocation is dropped.
Partially admitted state stays owned and charged after a budget rejection and can
be reused on retry. A failed call returns no result; choices and validity are
rebuilt on the next call. Non-budget errors and cancellation retain the existing
close-and-release behavior. Empty demand does not evaluate or admit branch buffers.

Managed accounting includes every retained typed capacity, metadata allocation,
mask and nested state. Reservation bookkeeping, result wrappers and allocator/RSS
overhead remain outside that existing accounting contract. Warm fixed-shape
execution needs no new managed reservations. Closing the parent releases the
shared child exactly once.

## Validation and performance

See `validation/conditional-memory/` for before/after benchmarks, the validation
matrix, measured managed bytes and limitations. This stage is a memory-lifetime
optimization, not a promise of a speedup for every branch shape. Variable-width
streaming and cross-level nested scratch sharing remain future work.
