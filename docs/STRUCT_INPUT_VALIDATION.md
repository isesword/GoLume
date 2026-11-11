# Struct input validation

`StructVector.Validate` checks the storage of every declared child before it
reads child validity or values. `source/inmemory.New` already uses this entry
point and now rejects malformed nested input before exposing a stream.

The bounded Struct schema is unchanged: scalar and List<String> children only.
Validation now checks:

- Nonnegative matching parent and child-batch row counts.
- Parent and child validity bitmap coverage. A nil bitmap means all valid;
  a present but short bitmap, including an empty one, is invalid for nonzero
  rows. Extra words and unused tail bits remain legal.
- Exactly row-aligned fixed-width values or string offsets for each declared
  child, plus bounded monotonic string offsets.
- Existing canonical List<String> storage validation before NULL lookups.
- Runtime dictionary fallback indices, bitmap, bounded monotonic table offsets,
  and codes in visible non-NULL rows.
- Required scalar/list children only in valid parent rows, and required list
  elements only within valid lists in valid Struct parents.

Parent NULL does not excuse malformed child storage. It does hide child values
and required-child constraints. Likewise, NULL list parents hide the values and
required-element constraints of their child ranges. Hidden dictionary codes are
not interpreted. These rules preserve Arrow's independent parent/child validity.

Non-zero-based string and dictionary offset views remain supported. Zero-row
strings may omit offsets, consistent with `StringViewVector`. Runtime ordinary
string storage still takes precedence over a shadowed dictionary map, and
undeclared runtime columns remain outside this validator's declared-field scope.
Dictionary domain identity and schema metadata equality rules are unchanged.
Validation neither clones nor takes ownership of input data; sources still
require immutable batches for their lifetime.

This is a correctness change, not a new nested type or operator. It does not
turn generic gather/clone helpers into validators for arbitrary malformed input,
change the spill codec's encoding support, modify Sum/Avg, or establish an RSS
memory cap.

Regression coverage includes every supported fixed-width child type, bitmap
word boundaries, malformed offsets, dictionary fallback/hidden codes, required
list elements, InMemory admission, codec credit cleanup, and bounded fuzz cases.
