# Native flat Parquet wall-clock TIME (N25 candidate)

Existing UTC-instant projection uses captured IANA or explicit TZif rules:
DateInZone for a local calendar date, TimeOfDayInZone for an ns wall-clock
value, and other existing component/offset/formatting helpers. They do not
resolve a wall clock back to an instant and are not changed here.

The flat Parquet scan now accepts a top-level INT64 TIME(NANOS) without UTC
adjustment, producing native Time64 with exact coefficients and NULLs. A visible
coefficient outside [0, 86400000000000) fails the native read. The check applies
to decoded rows in retained ranges, not to every row group in a file. It does
not change its Arrow import or general Time64 expression semantics. No implicit
timezone, calendar arithmetic or fold/gap policy is introduced.

The admitted data encodings are PLAIN, RLE_DICTIONARY and PLAIN_DICTIONARY;
dictionary pages use PLAIN or PLAIN_DICTIONARY. The scan checks declared footer
encodings and actual projected data/dictionary pages before decoding. It does
not enable native nested TIME, TIME32, TIME(MICROS), UTC-adjusted TIME, new
pruning, or unsupported page encodings. Existing DATE/TIMESTAMP rules apply.

The fixed output lane, nullable mask and concurrently live dense/definition
scratch are included in admission and batch sizing. Actual page preflight
incurs extra reads; no speedup is implied. Existing Arrow reader/page buffer
limits apply, but not all third-party allocations are query-accounted and this
does not certify process RSS.
