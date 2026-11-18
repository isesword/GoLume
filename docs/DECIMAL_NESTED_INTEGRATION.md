# Decimal and bounded nested-container integration

This integration is based on published b33cbda and the frozen, locally verified
D1 / N1 through N10 cumulative candidate. It is not a replacement of the remote
repository by an isolated source tree. Existing unrelated files and history are
preserved; raw validation logs are deliberately not committed.

## Included scope
- Explicit Arrow native Decimal128 conversion; Decimal Round (six rounding modes)
  and DivideRound with exact coefficients and existing precision/scale guards.
- Bounded StructGet, ListGet and StructFrom; explicit NativeNested Parquet forest
  reads; recursive structs to the documented depth and scalar profiles.
- List<Struct> read, ownership and Parquet sink paths; exactly the supported
  List<List<Int64/String>> forms, not arbitrary repeated containers.
- StructFrom composition of current supported containers; resident stable Sort
  and TopK payload transport for those bounded nested profiles.
- Resident stable native root Decimal128 Sort keys/payloads, precision 1..38,
  scale 0..precision; exact signed ordering without floating-point conversion.
  Sort+Limit remains full resident Sort then Limit when Decimal prevents TopK.

Defaults remain unchanged; NativeNested is the explicit unified entry. Native
Decimal inside containers, native Decimal TopK/spill/aggregation/Join/windows,
and NativeNested Parquet reads combining root Decimal with containers remain
unsupported in this integration. The latter is separate N11 work and excluded.
FILTER and unrelated unfinished features are not introduced. This is not
completion of roadmap item18 or unrestricted generic nesting.

Feature documents may describe historical local candidate states. This document
is the integration scope/verification summary, not a declaration that all earlier
historical descriptions or failure records have been superseded.

## Local scoped verification
The archived N10 source whitelist contains1477 files. All were hash-checked again
before integration; the225 changed source/document/test files match that freeze.
- Ten packages: scalar and SIMD each19190 tests/subtests passed.
- memory/expr/optimizer/source race9028; focused physical resident/order253.
- Vet passed for the selected ten-package scope.
-45 explicitly selected safe Lazy test files: scalar/SIMD each1876 passed.
- Engine Sort/TopK/Order/Limit selection: scalar/SIMD/race each11 passed.
- Lazy race groups: Decimal90, date51, bounded expressions329, Parquet181,
  recursive24, ListStruct48, new sink26, legacy sink39, NestedList41,
  StructFrom115, resident Sort7, nested TopK16, Decimal resident Sort28.
- External Arrow scalar/SIMD/race each13; downstream recipe each8.
These are scoped results, not a whole-Lazy, full-physical-race or repository pass.

N9 retains the original195-row/64MiB regression. Its genuine ownership/metadata
peak failures were fixed without increasing budgets or reducing data. N10 keeps
its simultaneous-owned-results budget rejection, then verifies sequential result
lifetimes with unchanged data and budget; that rejection was not mislabeled a
leak. Initial compile/setup failures, N7 stale negative, N8/N9/N10 before-reds and
interrupted first runs remain in the archived evidence rather than being erased.

## Limits and historical failures
The protected test and original full-repository gate remain outside permitted
inputs; neither reading nor modifying that file is part of this integration.
Historical combined Lazy600s and full-race/resource timeouts, Join512KiB failure,
List<TIME> resident16MiB/ordered64MiB failures, and FILTER sink-race600.114s37/42
remain failures. Hosted CI, native ARM64 and reliable performance are unverified.

Input/output ownership overlap and live capacity reservations remain budgeted
under the documented profiles. This is not proof that every third-party Arrow,
page/dictionary/codec/writer temporary, constructor normalization, allocator
 overhead, GC or RSS byte is charged to the query MemoryManager. Metadata charges
are engineering envelopes, not minimum-heap proofs. Deep spill metadata paging
is deferred; cancellation is cooperative rather than instruction-preemptible.

N5 sink limits persist: rename can precede directory-sync error; final cancellation
and rename are not atomic; no-overwrite is not atomic concurrent no-replace;
unknown panics rethrow without universal panic-safe cleanup; byte target is soft.
Opaque one-row owned Collect can retain many deep graphs. There is no universal
finite-budget guarantee for arbitrary K, row widths or opaque payloads.
