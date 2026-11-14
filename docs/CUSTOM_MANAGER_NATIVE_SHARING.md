# Explicit custom-manager native fan-out sharing

This is a new reconstruction, not the missing historical 13-file candidate.

A custom `memory.Manager` remains on the conservative guard path by default.
Merely embedding a built-in budget (and inheriting `TryReserveImmediate`) must
not opt a wrapper into sharing: its overridden `Reserve` may enforce additional
policy. To opt in, implement `ImmediateReservationManager` with identical
accounting/policy and construct `memory.NewNativeSharingManager(custom)`.
Pass that exact adapter as the pipeline manager. Wrappers around the adapter
must explicitly opt in again; ordinary Reserve/Used/Limit delegate unchanged.

Immediate admission must atomically charge the same budget, respect cancellation,
return a token charged for exactly the requested size, and never wait for
capacity, reclaim, spill, or invoke arbitrary callbacks. A clean capacity miss
returns nil,false,nil without charging. The adapter validates nil context and
returned token shape/size, releasing malformed live tokens before rejecting.
It cannot enforce a dishonest manager's actual accounting or nonblocking behavior.
Manager and Reservation must support the concurrency required by the pipeline.
The constructor itself performs no admission and creates no per-query storage.

Sharing retains the existing native-range ownership model: one shared payload,
independent per-branch binding graphs, release of payload only after last ACK,
and complete rollback before fallback. Non-opted-in managers, unavailable
immediate admission and any runtime Opaque use the original guard path. This
is not complete Opaque accounting or a process RSS guarantee. Existing query
budget context routing is unchanged and takes precedence over supplied managers.

This adds an opt-in API and correctness coverage; it does not claim recovery of
historical benchmark results or universal performance gains. Native ARM64
execution and remote CI are separate acceptance requirements.
