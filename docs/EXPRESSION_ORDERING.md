# Expression ordering

`SortBy` and `TopKBy` accept scalar expressions directly, so an ordering key
does not need to become a user-visible `WithColumns` result:

```go
frame, err := session.FromSource(events).
    SortBy(
        lazy.DescExpr(lazy.Length(lazy.Col("message"))),
        lazy.AscExpr(lazy.Year(lazy.Col("event_time"))),
    ).
    Select("id", "message").
    CollectFrame(ctx)
```

`Sort` and `TopK` accept the same `SortExpr` values, so existing `Asc("name")`
and `Desc("score")` calls remain valid. `AscExprNullsFirst` and
`DescExprNullsFirst` select NULL-first placement; `AscExpr` and `DescExpr`
place NULL values last. NULL placement is independent of ascending or
descending value direction.

The planner keeps direct-column keys on the existing physical fast path. For a
computed key it evaluates a private temporary column in the physical plan,
uses it for stable Sort or bounded Top-K, then removes it before returning the
logical result. Top-K replays a key-only source containing only the expression
dependencies during its first pass, preserving the payload-read avoidance
contract.

Order keys must be scalar projection expressions whose result type is already
orderable by the physical engine. Filter predicates such as `Contains` cannot
be sort keys. `SortBy(...).Limit(k)` retains the existing rewrite to Top-K.
