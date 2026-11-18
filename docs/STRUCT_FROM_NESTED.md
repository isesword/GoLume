# StructFrom bounded container composition (N7 candidate)

StructFrom may wrap the existing checked native recursive Struct, flat Struct-item
List and two-level Int64/String List profiles, alongside its previous twenty
scalar/primitive-List child types. This is composition of existing profiles, not
arbitrary recursive container admission. The resulting parent consumes one Struct
depth level. Existing depth-eight limits include that parent; a depth-eight
Struct cannot be wrapped in another Struct. Field aliases are literal identities.

Active output rows contain a present parent even when every child is NULL.
Children retain their own NULL/empty distinctions, including NULL List versus
empty List, NULL Struct item, and NULL inner List slots. Outer masks do not become
element masks. The result is owned by the expression workspace until its next
Evaluate or Close; owned Collect survives source/session closure. Shared getter
expressions must not make separately constructed outputs alias mutable owners.

The previous flat-only path and its budgets remain unchanged. Extended construction
must admit metadata and child-owned copies before publication, retain the previous
owner during replacement and release all incomplete owners on failure. Query
cancellation and budget failures remain errors, not unbudgeted copying fallbacks.

Container composition does not enable Decimal/Binary children, new terminals,
Map, a third List edge, recursive Struct items, selected-container expression
execution, nested keys. Resident Sort and TopK are separate consumer integrations; see
RESIDENT_NESTED_SORT.md and NESTED_TOPK.md. Scalar-key bounded external Sort
continues to require the configured finite memory/spill arrangement. Temporal
fields accepted by the previous constructor remain accepted in mixed results;
Parquet retains its own narrower native profile. IPC transport does not imply a
new native Parquet mapping for Date64/Time32/Time64/Duration.

Integration tests exercise getter → constructor → owned Collect and independent
IPC/native Parquet roundtrips, old twenty child types mixed with new Lists,
maximum wrapping depth, alias reuse, guarded scalar consumers, cancellation and
budget failure, plus actual multi-run bounded sorting. These are tests to run,
not claims that the candidate has passed. Historical failures and protected
full-repository gate restrictions remain in force. No publication, performance
improvement or completion of roadmap item18 is implied.
