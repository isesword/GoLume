# Unordered two-key native Decimal grouping (N24 candidate)

Two direct nullable Int64 keys can be grouped without proven global ordering
only in MemoryOnly mode under a finite actual query budget. The supported
aggregates are native Decimal SUM, explicitly configured AVG, COUNT(*), and
COUNT(native Decimal). Complete ordering proof still selects N23's streaming
path. Without it, the resident path has no output order guarantee; it does not
introduce an automatic sort or spill, other types, more keys or old materialization.

Each component is encoded as a validity byte plus an eight-byte signed integer
bit pattern (little endian). A NULL component leaves zero in the value bytes.
The two-key tuple is precisely 18 bytes; the one-key encoding stays nine.
The existing pre-admitted index copies all key bytes into its owned arena,
verifies exact byte equality after a hash match, and accounts for arena growth.
It retains only admitted state plus bounded output pages, whose two key columns
are also charged. Cancellation, allocation denial and finalization failure
release owned state. Before the first page, resident execution checks final
precision for all groups; it does not return a partial result on late overflow.
