# Bounded regular-expression expressions

GoLume regular expressions use Go's RE2-compatible `regexp` implementation.
Matching time is linear in the size of the input; backreferences and lookaround
are intentionally unavailable.

## API

`RegexMatch(value, pattern)` is a filter predicate. `value` must currently be a
string column and `pattern` must be a string literal.

`RegexExtract(value, pattern, group)` is a string projection. `pattern` must be
a string literal and `group` must be a non-negative int64 literal no greater
than the pattern's capture count. Group zero selects the complete first match.

```go
pattern := lazy.Str(`^([^:]+):([0-9]+)$`)

frame, err := lf.
    Filter(lazy.RegexMatch(lazy.Col("event"), pattern)).
    SelectExprs(
        lazy.RegexExtract(lazy.Col("event"), pattern, lazy.Lit(1)).Alias("name"),
        lazy.RegexExtract(lazy.Col("event"), pattern, lazy.Lit(2)).Alias("id"),
    ).
    CollectFrame(ctx)
```

## Contracts

- Patterns are validated and compiled while the expression program is built.
- Pattern text is limited to 64 KiB and is retained as instruction metadata;
  it is not expanded into a per-row string vector.
- Repeated identical extractions share one projection register through CSE.
- `RegexMatch(NULL, pattern)` is SQL UNKNOWN and composes with `And`, `Or`, and
  `Not` through the existing three-valued predicate program.
- `RegexExtract` returns NULL for NULL input, no match, or an optional capture
  group that did not participate. A valid empty capture remains a valid empty
  string.
- Plain and dictionary input encodings are supported. Dictionary predicates
  evaluate each stable dictionary value once and retain the result in the
  predicate workspace.
