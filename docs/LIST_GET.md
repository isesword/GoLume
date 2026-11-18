# Native List element access

`expr.ListGet(value, index)` and the `lazy` wrapper select one element per row
from a native List<String>, List<Int64>, flat Struct-item List or checked
two-level List (see LIST_STRUCT.md and NESTED_LIST.md). The latter returns an
owned nullable inner primitive List, allowing another ListGet. The index is an expression,
not a compile-time-only parameter. It must be one of the eight native signed or
unsigned integer types. Bool, floating, temporal, Decimal and string indices
are not implicitly converted.

## Exact lookup semantics

Indices are zero-based. Negative indices and indices at or beyond the list
length produce NULL; negative values do not count backward from the end.
NULL list, NULL index or a selected NULL element also produces NULL. Empty lists
therefore yield NULL for every index. A selected valid empty string remains a
valid empty string. Result nullability is always true, even when the input
fields are required, because out-of-range access is a valid NULL-producing case.

For `[10, NULL, 30]`, indices `0/1/2/3/-1` yield
`10/NULL/30/NULL/NULL`. `MaxUint64` is handled
as a large unsigned index and yields NULL, not a signed conversion overflow.
The two children use ordinary evaluation order, value then index; NULL does not
catch errors in either child expression. Existing conditional guards may limit
which rows demand the children. This is not a generic Try wrapper.

The existing collected-result `Int64ListValue.Element(i)` remains unchanged:
it is a separate borrowed-value accessor whose out-of-bounds operation returns
an error. The new query expression does not change that API's contract.

## Metadata, structure and hidden values

The scalar result preserves the supported element's logical metadata, with
canonical Int64 plain/String view storage. No parent List metadata is mislabeled
as a scalar output. The additional bounded profiles preserve their element
metadata and owned container semantics; this is not arbitrary recursive List,
Map or opaque type admission.

Workspace input may be a valid logical prefix or nonzero-offset view. Validation
covers the logical `rows+1` offset window, its referenced child range and required
bitmap/string-offset bounds rather than insisting the physical vector has no
unused tail. Malformed offsets or short storage remain errors, including when
outputs would be NULL. This does not relax source constructors' own contracts.

Required-element NULL is a storage-contract violation across the entire
referenced list for each active, parent-valid row, even when that row's index is
NULL or out of range. Inactive/parent-NULL rows hide value constraints but not
broken structural storage. Selected NULL payload bytes/integers are not copied.
Query guards do not suppress an error already detected by source construction.

## Ownership and resource boundaries

Results are owned by the Workspace, not aliases into the source child buffer.
They follow the ordinary read-only borrowed-result lifetime through the next
evaluation or Close. Input buffers and validity are not mutated. Existing
retention/copy rules apply when a downstream owner needs a longer lifetime.

Output capacity and string bytes are admitted under the existing Workspace
budget; copies include cancellation checks. This is not a selected-row fast
path: native List dependencies keep selection disabled even for an Int64 result.
Materialized filtering and conditional guarded evaluation remain distinct.

## Verification and exclusions

This N6 candidate extends the frozen N5 integration. Current-candidate validation
is required; historical archive reports are not acceptance of this tree. Protected
full-repository gate restrictions and earlier failures remain in force. No
publication, performance improvement or complete roadmap item18 is implied.
