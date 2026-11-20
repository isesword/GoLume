# Ordered two-key native Decimal grouping (N23 candidate)

Native Decimal SUM, explicitly configured AVG, COUNT(*) and COUNT(native
Decimal) can share a grouped query with two direct Int64 keys. Each key may be
nullable. The source must guarantee a complete global ordering prefix on both
keys in GroupBy order. Supply an explicit upstream Sort if this is not already
proven; there is no automatic Sort or resident two-key fallback. Both key
directions and NULL placements are independent; runtime verifies
lexicographic order across batches and partitions, including filtered rows.

Aggregation keeps one current group and emits bounded pages; the second
Int64 column's output vector and validity are included in admission. It does
not spill, although an explicit upstream Sort can. Three or more keys,
other two-key types, window AVG, and grouped ExecuteDecimal materialization
remain unsupported. Single-key query fees and execution modes stay intact.
