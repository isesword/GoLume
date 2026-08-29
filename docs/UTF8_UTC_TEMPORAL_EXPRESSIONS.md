# UTF-8 strings and UTC temporal expressions

The lazy API supports Parquet `STRING`/UTF-8 `BYTE_ARRAY`, `DATE`, and UTC
`TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, and `TIMESTAMP_NANOS` columns. Timestamp
values are normalized to Unix epoch nanoseconds before predicate or projection
execution. Arrow RecordBatch timestamps use the same representation.

```go
out, err := session.ScanParquet("events.parquet")
    .Filter(lazy.Contains(lazy.Col("message"), lazy.Str("error")))
    .WithColumns(
        lazy.Year(lazy.Col("event_time")).Alias("year"),
        lazy.Length(lazy.Col("message")).Alias("characters"),
    )
    .SelectExprs(
        lazy.Col("year").Alias("year"),
        lazy.Col("characters").Alias("characters"),
    )
    .CollectFrame(ctx)
```

`Contains`, `StartsWith`, and `EndsWith` are case-sensitive UTF-8 predicates.
They do not perform Unicode normalization or locale-aware collation. `Length`
counts Unicode code points. A NULL input produces an unknown predicate result
and a NULL scalar result, following the engine's existing SQL three-valued
logic.

## Literal string cleansing and inspection

`ByteLength`, `Trim`, `TrimStart`, `TrimEnd`, `Lower`, `Upper`, `Find`,
`Replace`, `ReplaceAll`, and `Slice` are scalar expressions. They can be used
in `WithColumns` or `SelectExprs`; their NULL inputs propagate to NULL outputs.
Dictionary-encoded inputs are materialized as regular UTF-8 string vectors
after a transformation.

```go
clean := lazy.Trim(lazy.Col("label"))
frame = frame.WithColumns(
    clean.Alias("label"),
    lazy.Lower(clean).Alias("label_lower"),
    lazy.ReplaceAll(clean, lazy.Str("-"), lazy.Str("_")).Alias("key"),
    lazy.Slice(clean, lazy.Lit(-4), lazy.Lit(4)).Alias("suffix"),
    lazy.Find(clean, lazy.Str("错误")).Alias("error_at"),
    lazy.ByteLength(clean).Alias("encoded_bytes"),
)
```

`Trim*` removes Unicode whitespace. `Lower` and `Upper` use Go's Unicode
simple case mapping, with no locale tailoring or case-folding. `Replace` and
`ReplaceAll` use case-sensitive literal matching—their old and new values are
never regular expressions. `Find` returns a zero-based Unicode code-point
index, or `-1` when absent, so it agrees with `Length`; `ByteLength` exists for
the separate encoded-byte use case. `Slice` also counts Unicode code points.
Its negative offsets count from the end, out-of-range offsets return an empty
string, and negative lengths are an error.

`Year`, `Month`, and `Day` accept `date32`, `date64`, and `timestamp`; `Hour`,
`Minute`, and `Second` accept `timestamp`. All calendar extraction is UTC.
`lazy.Timestamp(v)` now expects UTC Unix nanoseconds.

## RFC3339 parsing and durations

An explicit cast from `string` to `timestamp` parses RFC3339/RFC3339Nano text
with `Z` or a numeric UTC offset and normalizes the result to UTC Unix
nanoseconds. A no-offset `YYYY-MM-DDTHH:MM:SS[.fraction]` input is accepted as
UTC. Named IANA zones, date-only strings, and values outside the signed
nanosecond range are rejected rather than depending on a process-local
timezone.

`duration` is an elapsed-time value, also stored as signed nanoseconds. Use a
Go `time.Duration` literal for expression construction, or cast a string in Go
duration syntax such as `"1h30m"`:

```go
frame = frame.WithColumns(
    lazy.Cast(lazy.Col("started_at"), memory.TypeTimestamp).Alias("started"),
    lazy.Cast(lazy.Col("elapsed_text"), memory.TypeDuration).Alias("elapsed"),
    lazy.Col("started").Add(lazy.Duration(5 * time.Minute)).Alias("deadline"),
)
```

`timestamp + duration`, `timestamp - duration`, `duration + duration`, and
`duration - duration` preserve their respective types. `timestamp - timestamp`
produces a duration. Every operation is checked for overflow and propagates
NULL. Arrow `Duration` arrays in seconds, milliseconds, microseconds, or
nanoseconds are normalized to nanoseconds on source construction.

## Explicit timestamp input units

Use `TimestampSeconds`, `TimestampMillis`, or `TimestampMicros` when a literal
comes from a source whose unit is not nanoseconds:

```go
frame.Filter(lazy.Col("event_time").Eq(lazy.TimestampMillis(apiMillis)))
```

Each constructor is normalized to UTC Unix nanoseconds while its expression is
compiled. Conversion overflow is an error; no timestamp literal wraps or
silently changes unit. `Timestamp(v)` remains the direct spelling for an
already-normalized nanosecond value.

## Checked temporal arithmetic

`Add` and `Sub` retain the temporal result type and report overflow with the
offending row number. `date32` uses days and `date64` uses milliseconds for
their legacy signed-integer deltas; timestamp arithmetic requires an explicit
duration, which is always UTC nanoseconds. Temporal `Mul` and `Div` are
rejected. Operations whose inferred result is a signed or unsigned integer are
checked as well, so they do not wrap. Mixed `uint64` and signed-integer
expressions still follow the engine's existing float64 coercion; floating-point
arithmetic keeps its IEEE-754 behavior.

## Parquet pruning

Row-group statistics now prune direct `DATE`/UTC timestamp comparisons and
calendar extraction comparisons. `Year` uses its monotonic UTC range; the
other components prune when min/max lie in one complete calendar bucket or the
requested component is outside its valid domain. String match predicates prune
an all-NULL row group from null-count metadata. They intentionally do not use
`BYTE_ARRAY` min/max values, because Parquet producers may prefix-truncate
those statistics and using them could wrongly discard matching rows.

Only UTC/no-timezone Arrow timestamps and Parquet timestamps marked
`isAdjustedToUTC=true` are accepted. Raw binary columns, nested Parquet types,
non-UTC timestamps, regular-expression matching, split/list extraction, JSON
decoding/path access, Unicode normalization/collation, timestamp formatting,
week/quarter extraction, and IANA timezone conversion remain unsupported.
