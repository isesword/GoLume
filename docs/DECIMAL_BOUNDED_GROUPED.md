# Explicit bounded single-key Decimal SUM/COUNT execution

`engine.ExecuteDecimalBoundedGrouped(ctx, input, options)` is a new explicit engine-only executor. It actually ingests unordered typed rows, aggregates in a finite resident table, flushes sorted partial runs to disk and invokes the N31 multi-pass merger. It is not a change to PreparedQuery, Auto, Lazy, Source or their existing admission guards. There is no transparent general Source adapter.

## Native input and ownership

`DecimalBoundedInput.Next(ctx, *DecimalBoundedInputBatch) (bool, error)` fills an engine-owned destination containing a fixed array of 256 `DecimalBoundedInputRow` values. Each row has a nullable signed Int64 key and a nullable native Decimal128 coefficient, represented by separate validity booleans. Every successful batch, including a zero-row batch, sets `Rows`, `Precision` and `Scale`. No caller-provided unbounded slice is accepted. Rows outside the logical batch and invalid Decimal payloads are ignored. NULL keys are canonicalized before aggregation.

All valid coefficients must fit the declared input precision, which is fixed for the execution; scale is unchanged. The full delivered batch is validated before its rows affect aggregate state. Engine-owned input storage is reused only after its coefficients and key bits have been copied into aggregate states. Input implementations must not retain or mutate the destination after returning, except during a subsequent Next call.

Every nonnil input is immediately taken over by Execute. Its Close is called exactly once on all returns, including invalid configuration, nil/cancelled context and reservation failure. Close errors and cancellation triggered by Close prevent successful result publication. A nil input cannot be closed. Input-private allocations or external resources are not certified by this executor: only engine-owned fixed buffers and allocations are accounted by the supplied manager. Providers requiring whole-query accounting must separately charge their private storage to the appropriate query budget. This is not an RSS claim.

## Explicit configuration and limits

Options require a positive finite memory.Manager and a positive finite QuerySpillStore, plus native precision 1..38 and scale 0..precision. The fixed operation emits SUM(value), COUNT(value) and COUNT(rows) for one nullable Int64 key. SUM is nullable Decimal128(38,inputScale), both counts are nonnull Uint64. AVG, multiple keys, expressions, filters, opaque values and configurable aggregate lists are not part of this entry point.

`TableCapacity` is 1..256. Zero chooses a conservative capacity after reserving fixed executor storage: available memory minus the N31 fixed descriptors, eight maximum accepted typed pages, and two 64-KiB physical-page allowances is divided by record width and clamped to 256. Insufficient headroom rejects with ErrBudgetExceeded. Explicit capacity permits deterministic resource testing; all later reservations remain authoritative. Neither mode guarantees success for arbitrary disk limits, file-owner/metadata peaks or other concurrent allocations. Capacity-zero headroom is only a conservative initial choice; a later real Reserve can still fail. Such failure is terminal with cleanup, not a dynamic Reserve-failure-triggered flush or retry policy. Flushes are triggered only by a full table receiving a new key, or by EOF. The table does not consume all available memory and then assume merge scratch is free.

Hard execution limits are 65,536 input rows, 4,096 successfully delivered batches (including empty batches), and 64 initial flush runs. Each flush has at most 256 distinct keys and exactly one N29 page. Hitting any limit is an explicit error with cleanup, not a silent fallback. The 65,536 limit counts input rows, not distinct groups. At most 64 initial runs times 256 records can represent 16,384 distinct groups; a smaller table capacity lowers that bound further. High-cardinality input can therefore fail at the 64-run limit well before reaching the input-row limit. Empty input produces no groups.

## Actual execution and progress

The resident table is a budgeted fixed-capacity array of N29 partial records, with bounded linear lookup and allocation-free in-place insertion sort. Existing keys update exact signed-256 SUM state and counts. An absent key when the table is full triggers a flush before inserting that key. EOF flushes remaining records. NULL is a real grouping key; positive-row all-NULL groups retain NULL SUM and their counts.

Every flush writes a real N29 checked state file through SpillWithMetadata. No intermediate state is finalized. The table is cleared after successful flush. Initial file owners and fixed run descriptors are retained and accounted. The executor invokes N31 with all actual initial runs, so more than eight flushes require multiple logical-run merge passes. N31 handles page-spanning runs and strictly reduces logical-run count, even for disjoint keys whose page count does not shrink.

`result.Stats` reports actual TableCapacity, InputRows, InputBatches, Flushes, MergePasses and Groups. These are informational fields, not control inputs. One nonempty initial run undergoes one copying merge pass; zero runs yield zero passes.

After all runs have merged, the executor restores each final page in turn and Finalizes every group. A precision overflow in any later page makes Execute fail before returning a consumable result. Intermediate sums larger than Decimal128 or precision 38 may cancel across flushes/passes and succeed. The overflow sentinel is `aggregate.ErrDecimalSumOverflow`. Only after all final groups pass are initial run files deleted. Input.Close and a final context check must also succeed before publication.

## Result, budgets and cleanup

`result.Next(ctx) (*DecimalBoundedOutputBatch, bool, error)` restores at most one final page, copies native final values into the result's fixed 256-row output buffer, then releases the restored page owner. The returned pointer is borrowed until the next Next call, including EOF or error, or Close. Those operations clear the entire previous batch. Callers wanting retained data must copy and account for it themselves. Output metadata remains precision 38/inputScale even for NULL SUM. Output order is NULL-first, then signed Int64 ascending.

A fixed reservation covers the result descriptor and output buffer, engine input buffer and fixed run manifests before allocation, and is conservatively retained through result Close. The table has its own reservation released at execution exit. N29/N31 additionally charge codecs, restored typed pages, merge workspaces, generated file owners and handle metadata. The same finite store accounts for initial files, old/new intermediate generations and final pages at their true overlapping disk peak. Resource tests measure physical bytes rather than treating wire payload length as file size.

Execute owns initial files; N31 only borrows them. The result also owns all remaining N31 output files and retryable cleanup handles. Any execution error closes the input and attempts background cleanup. If deletion fails, Execute returns a nonnil cleanup-only result together with joined error; callers must retry Close and must not consume it. Successful cleanup returns nil result. No existing caller-owned spill files are deleted by this executor.

`Close(ctx)` invalidates output and retries all still-owned files. Failed deletes preserve handles and accounting; a later Close can retry. On complete cleanup leases are released. Next errors also attempt cleanup, preserve a terminal error and prohibit further publication. If the external store has been closed or files corrupted, Next fails rather than pretending successful output. Store and manager must stay available until the result is closed. These owners are single-consumer and not concurrency-safe.

## Scope

This is a real, but deliberately limited, explicit bounded spill execution path. It is not general grouped spill support for old query APIs. No old source file, N29/N30/N31 protocol, query strategy admission or Lazy terminal is modified.
