# N1: bounded record and primitive-list expressions

This candidate selectively restores three expression operations on the b33
published source plus the separately verified/validating Decimal increment.
It does not import the earlier cumulative candidate's general nested containers.
Current test/publication status must be verified against this candidate, not
inherited from those earlier archives.

- `StructGet(value, fieldName)` extracts one literal-named direct child from the
  existing bounded Struct profile. Dots remain literal characters, not paths.
  Parent NULL and child NULL yield NULL. The returned scalar/string/primitive
  list payload is workspace-owned, not a pointer into borrowed input.
- `ListGet(value, index)` accepts only List<String> and List<Int64>, with any
  native integer index. Negative, out-of-range and NULL indices/lists/elements
  return NULL; negative indices do not count from the end. Results own the
  selected scalar or string payload. Child expressions still evaluate normally.
- `StructFrom(named...)` snapshots the ordered input field slice and constructs
  a non-NULL parent in each active row, even if all children are NULL. Child
  NULLs remain independent. Names must be unique and nonempty; bare columns can
  infer their names, whereas computed expressions require an alias.

Supported record children remain the existing eighteen scalar types plus
List<String>/List<Int64>. Recursive Struct, List<Struct>, List<List<...>>,
Decimal/Binary/opaque children and recursive child metadata are rejected.
Top-level Decimal support does not imply Decimal inside a record. This is not
completion of arbitrary generic nesting or roadmap item 18.

Binding, dependency analysis, CSE and optimizer renaming track all children.
StructFrom renaming preserves the names resolved before input-column rewriting.
Private opcodes are appended after existing date/time and Decimal opcodes;
no earlier opcode values are changed. These operations retain dedicated payload
owners rather than borrowing live inputs or reusing cached JSON owners.

Admission precedes temporary plans, effective-parent masks and complete output
replacement. Existing payloads stay charged through the last possible failure;
only a successful atomic commit releases the old owner. Long copies have
cancellation checkpoints. NULL/inactive rows suppress invalid demanded values,
not malformed row-aligned storage. Bool extraction retains its separately
admitted Uint32 output-view cache independently of its Truth/validity payload.

No new spill format, recursive codec, source profile, nested Array/StructList map
or selected-row fast path is introduced. Existing bounded consumers must be
revalidated on the combined source. Tests and API source alone are not proof
that lazy, IPC/Parquet, Sort/Join payload or budget gates pass. Protected full
repository gates and historical failures remain separate.
