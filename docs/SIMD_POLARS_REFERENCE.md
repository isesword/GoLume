# Polars SIMD reference for GoLume P0

Pinned revision: `4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0` (Polars `main`,
2026-09-06 lookup). All links below point to this immutable revision.

## What the source shows

- Numeric comparison dispatch and SIMD-specific comparison code are separated
  in [`polars-compute/src/comparisons/simd.rs`](https://github.com/pola-rs/polars/blob/4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0/crates/polars-compute/src/comparisons/simd.rs)
  and the type/array dispatch in [`comparisons/array.rs`](https://github.com/pola-rs/polars/blob/4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0/crates/polars-compute/src/comparisons/array.rs).
- Numeric arithmetic is organized by signed, unsigned, and floating-point
  semantics in [`arithmetic/`](https://github.com/pola-rs/polars/tree/4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0/crates/polars-compute/src/arithmetic),
  rather than by one generic runtime operation switch.
- Filter has separate primitive, boolean, scalar and AVX-512 paths in
  [`filter/`](https://github.com/pola-rs/polars/tree/4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0/crates/polars-compute/src/filter).
  This is a physical data-movement optimization, not evidence that every
  expression is SIMD.
- Bitmap operations are isolated in the Arrow bitmask implementation at
  [`bitmap/bitmask.rs`](https://github.com/pola-rs/polars/blob/4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0/crates/polars-arrow/src/bitmap/bitmask.rs).
- Aggregate implementations are grouped under
  [`compute/aggregate`](https://github.com/pola-rs/polars/tree/4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0/crates/polars-arrow/src/compute/aggregate),
  with memory/layout concerns kept below the expression layer.
- UTF-8 equality and string representation have separate array/scalar/view
  implementations; the UTF-8 array code is at
  [`array/utf8`](https://github.com/pola-rs/polars/tree/4a3aa4c6f8655ae09b4ae53824e4b4060cd8ead0/crates/polars-arrow/src/array/utf8).

## Classification used for GoLume

1. **Explicit SIMD:** files or modules named `simd`, `avx512`, or code using
   architecture-specific vector operations, such as comparison SIMD and the
   AVX-512 filter path.
2. **SIMD-friendly scalar/batch code:** primitive filter, arithmetic and
   aggregate loops whose columnar layout permits compiler vectorization, but
   whose source is not itself proof of emitted vector instructions.
3. **Dependency/library optimization:** UTF-8, bitmap, compression and Arrow
   buffer operations that may use optimized dependencies or platform code.
4. **Batching/parallelism:** array dispatch, chunking, streaming and thread
   scheduling; these improve throughput but are not SIMD coverage.

GoLume's operation matrix must preserve these categories. A Polars module name
or a columnar loop alone cannot approve a GoLume Auto kernel. GoLume still
requires same-build, architecture-specific benchmark evidence and an
end-to-end regression check before changing `validation/simd-auto-gates.json`.
