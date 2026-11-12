package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// joinGatherAdmissionBytes includes every runtime map binding, even columns not
// present in schema. Aliases are charged independently because takeRows creates
// a separate output vector for every binding. The historic full-batch bound is
// retained, but is computed with checked arithmetic and validated traversal.
// A successful preflight allocates no planning data.
func joinGatherAdmissionBytes(schema memory.Schema, batch *memory.Batch, rows []int) (int64, error) {
	payload, owned, runtimeMetadata, err := planJoinGather(batch, rows)
	if err != nil {
		return 0, err
	}
	schemaMetadata, err := joinBatchMetadataBytes(schema.Fields)
	if err != nil {
		return 0, err
	}
	metadata := runtimeMetadata
	if metadata < schemaMetadata {
		metadata = schemaMetadata
	}
	if err := addJoinEstimate(&owned, metadata); err != nil {
		return 0, err
	}
	var lower int64
	if err := addJoinGatherProduct(&lower, uint64(payload), 2); err != nil {
		return 0, err
	}
	if err := addJoinEstimate(&lower, schemaMetadata); err != nil {
		return 0, err
	}
	if lower > owned {
		return lower, nil
	}
	return owned, nil
}

// addJoinGatherProduct checks before conversion or multiplication, including
// callers whose source counts are uint64 rather than representable Go lengths.
func addJoinGatherProduct(total *int64, count, width uint64) error {
	if *total < 0 || (width != 0 && count > uint64(math.MaxInt64)/width) {
		return fmt.Errorf("join gather byte estimate overflows int64")
	}
	return addJoinEstimate(total, int64(count*width))
}

func joinGatherWords(rows int) int {
	words := rows / 64
	if rows%64 != 0 {
		words++
	}
	return words
}

func joinGatherValidity(validity memory.Bitmap, rows int) error {
	if rows < 0 || (validity != nil && len(validity) < joinGatherWords(rows)) {
		return fmt.Errorf("join gather malformed validity or row count")
	}
	return nil
}

func joinGatherFixedPayload(total *int64, length int, validity memory.Bitmap, rows, width int) error {
	if length != rows {
		return fmt.Errorf("join gather vector rows %d do not match batch rows %d", length, rows)
	}
	if err := joinGatherValidity(validity, rows); err != nil {
		return err
	}
	if err := addJoinGatherProduct(total, uint64(length), uint64(width)); err != nil {
		return err
	}
	return addJoinGatherProduct(total, uint64(len(validity)), 8)
}

func joinGatherOffsets(offsets []uint32, bytes int) error {
	if len(offsets) == 0 {
		return fmt.Errorf("join gather missing offsets")
	}
	if uint64(offsets[0]) > uint64(bytes) {
		return fmt.Errorf("join gather malformed offsets")
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] || uint64(offsets[i]) > uint64(bytes) {
			return fmt.Errorf("join gather malformed offsets")
		}
	}
	return nil
}

func joinGatherStringPayload(total *int64, v *memory.StringViewVector, rows int) error {
	if v == nil {
		return fmt.Errorf("join gather nil string vector")
	}
	// StringViewVector.Validate permits a zero-row vector with no offsets.
	// Empty list children can use the same representation. They contribute no
	// offset payload; the gathered output still owns its canonical {0} offset.
	if v.Len() != rows {
		return fmt.Errorf("join gather malformed string rows")
	}
	if len(v.Offsets) != 0 {
		if err := joinGatherOffsets(v.Offsets, len(v.Data)); err != nil {
			return err
		}
	}
	if err := joinGatherValidity(v.Validity, rows); err != nil {
		return err
	}
	if err := addJoinGatherProduct(total, uint64(len(v.Offsets)), 4); err != nil {
		return err
	}
	if err := addJoinEstimate(total, int64(len(v.Data))); err != nil {
		return err
	}
	return addJoinGatherProduct(total, uint64(len(v.Validity)), 8)
}

func joinGatherDictionaryPayload(total *int64, v *memory.DictionaryStringVector, rows int) error {
	return joinGatherDictionaryPayloadMode(total, v, rows, false)
}

// External sort's unadvertised runtime dictionaries can have no table when all
// logical rows are NULL. The bounded copier never dereferences their codes.
// Keep the join wrapper strict; the codec independently validates declared data.
func joinGatherDictionaryPayloadMode(total *int64, v *memory.DictionaryStringVector, rows int, allowEmptyNullDictionary bool) error {
	if v == nil {
		return fmt.Errorf("join gather nil dictionary vector")
	}
	if err := joinGatherFixedPayload(total, len(v.Indices), v.Validity, rows, 4); err != nil {
		return err
	}
	if len(v.Offsets) != 0 || !allowEmptyNullDictionary {
		if err := joinGatherOffsets(v.Offsets, len(v.Data)); err != nil {
			return err
		}
	}
	cardinality := 0
	if len(v.Offsets) != 0 {
		cardinality = len(v.Offsets) - 1
	}
	for i, code := range v.Indices {
		// Empty tables have cardinality zero, so every valid logical row is
		// rejected, even when its code is zero. Hidden NULL codes are irrelevant.
		if (v.Validity == nil || memory.IsSet(v.Validity, i)) && uint64(code) >= uint64(cardinality) {
			return fmt.Errorf("join gather dictionary code out of range")
		}
	}
	if err := addJoinGatherProduct(total, uint64(len(v.Offsets)), 4); err != nil {
		return err
	}
	return addJoinEstimate(total, int64(len(v.Data)))
}

func joinGatherListPayload(total *int64, v *memory.StringListVector, rows int) error {
	if v == nil || v.Values == nil {
		return fmt.Errorf("join gather nil list vector or child")
	}
	if len(v.Offsets) == 0 || len(v.Offsets)-1 != rows || v.Offsets[0] != 0 {
		return fmt.Errorf("join gather malformed list rows")
	}
	if err := joinGatherOffsets(v.Offsets, v.Values.Len()); err != nil {
		return err
	}
	if uint64(v.Offsets[len(v.Offsets)-1]) != uint64(v.Values.Len()) {
		return fmt.Errorf("join gather list terminal offset does not match child rows")
	}
	if err := joinGatherValidity(v.Validity, rows); err != nil {
		return err
	}
	if err := addJoinGatherProduct(total, uint64(len(v.Offsets)), 4); err != nil {
		return err
	}
	if err := addJoinGatherProduct(total, uint64(len(v.Validity)), 8); err != nil {
		return err
	}
	return joinGatherStringPayload(total, v.Values, v.Values.Len())
}

// Actual gathered Struct.Fields support scalar and list children only. The
// complete payload walk can also encounter undeclared nested Structs maps;
// those may extend at most 64 Struct.Values edges below the root Batch.
const maxJoinGatherStructDepth = 64

// The bounded ancestor chain detects cycles without allocating a visited map.
// Sharing between siblings is legal and counted once per binding. The depth
// limit also bounds stack use and ancestor-search work for acyclic input.
type joinGatherAncestor struct {
	batch  *memory.Batch
	parent *joinGatherAncestor
	depth  int
}

func joinGatherPayload(batch *memory.Batch, parent *joinGatherAncestor) (int64, error) {
	return joinGatherPayloadMode(batch, parent, false)
}

func joinGatherPayloadMode(batch *memory.Batch, parent *joinGatherAncestor, allowEmptyNullDictionary bool) (int64, error) {
	if batch == nil || batch.Rows < 0 {
		return 0, fmt.Errorf("join gather nil batch or invalid rows")
	}
	depth := 0
	if parent != nil {
		depth = parent.depth + 1
		if depth > maxJoinGatherStructDepth {
			return 0, fmt.Errorf("bounded join gather struct nesting exceeds %d levels", maxJoinGatherStructDepth)
		}
	}
	for p := parent; p != nil; p = p.parent {
		if p.batch == batch {
			return 0, fmt.Errorf("join gather cyclic struct batch")
		}
	}
	here := joinGatherAncestor{batch: batch, parent: parent, depth: depth}
	var total int64
	for _, v := range batch.Int8s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Int8s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 1); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Int16s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Int16s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 2); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Int32s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Int32s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 4); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Int64s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Int64s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 8); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Uint8s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Uint8s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 1); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Uint16s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Uint16s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 2); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Uint32s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Uint32s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 4); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Uint64s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Uint64s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 8); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Float32s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Float32s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 4); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Float64s {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Float64s vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 8); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Decimals {
		if v == nil {
			return 0, fmt.Errorf("join gather nil Decimals vector")
		}
		if err := joinGatherFixedPayload(&total, len(v.Values), v.Validity, batch.Rows, 16); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Strings {
		if err := joinGatherStringPayload(&total, v, batch.Rows); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.DictStrings {
		if err := joinGatherDictionaryPayloadMode(&total, v, batch.Rows, allowEmptyNullDictionary); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.StringLists {
		if err := joinGatherListPayload(&total, v, batch.Rows); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Int64Lists {
		if err := joinGatherInt64ListPayload(&total, v, batch.Rows); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Structs {
		if v == nil || v.Values == nil || v.Rows != batch.Rows || v.Values.Rows != batch.Rows {
			return 0, fmt.Errorf("join gather malformed struct vector")
		}
		if err := joinGatherValidity(v.Validity, v.Rows); err != nil {
			return 0, err
		}
		if err := addJoinGatherProduct(&total, uint64(len(v.Validity)), 8); err != nil {
			return 0, err
		}
		child, err := joinGatherPayloadMode(v.Values, &here, allowEmptyNullDictionary)
		if err != nil {
			return 0, err
		}
		if err := addJoinEstimate(&total, child); err != nil {
			return 0, err
		}
		for i, field := range v.Fields {
			for j := 0; j < i; j++ {
				if v.Fields[j].Name == field.Name {
					return 0, fmt.Errorf("join gather duplicate struct child %q", field.Name)
				}
			}
			if err := joinGatherCheckChild(v.Values, field); err != nil {
				return 0, err
			}
		}
	}
	// Opaque Take can allocate Arrow slice/concatenation buffers without a
	// proven bound. Reject it, including unadvertised columns, before writing.
	if len(batch.Opaque) != 0 {
		return 0, fmt.Errorf("bounded join gather does not support opaque vectors")
	}
	return total, nil
}

func joinGatherCheckChild(batch *memory.Batch, field memory.Field) error {
	present := false
	switch field.Type {
	case memory.TypeInt8:
		present = batch.Int8s[field.Name] != nil
	case memory.TypeInt16:
		present = batch.Int16s[field.Name] != nil
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		present = batch.Int32s[field.Name] != nil
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		present = batch.Int64s[field.Name] != nil
	case memory.TypeUint8:
		present = batch.Uint8s[field.Name] != nil
	case memory.TypeUint16:
		present = batch.Uint16s[field.Name] != nil
	case memory.TypeUint32, memory.TypeBool:
		present = batch.Uint32s[field.Name] != nil
	case memory.TypeUint64:
		present = batch.Uint64s[field.Name] != nil
	case memory.TypeFloat32:
		present = batch.Float32s[field.Name] != nil
	case memory.TypeFloat64:
		present = batch.Float64s[field.Name] != nil
	case memory.TypeString:
		present = batch.Strings[field.Name] != nil || batch.DictStrings[field.Name] != nil
	case memory.TypeList:
		if len(field.Children) == 1 && field.Children[0].Type == memory.TypeInt64 {
			present = batch.Int64Lists[field.Name] != nil
		} else {
			present = batch.StringLists[field.Name] != nil
		}
	default:
		return fmt.Errorf("unsupported bounded join gather struct child %q type %s", field.Name, field.Type)
	}
	if !present {
		return fmt.Errorf("join gather missing struct child %q", field.Name)
	}
	return nil
}

func joinGatherFixedOwned(total *int64, rows, width int, bitmap bool) error {
	if err := addJoinGatherProduct(total, uint64(rows), uint64(width)); err != nil {
		return err
	}
	if bitmap {
		return addJoinGatherProduct(total, uint64(joinGatherWords(rows)), 8)
	}
	return nil
}

// joinGatherSelection represents either a permutation or a contiguous range.
// Range planning must not allocate the very index array it is admitting.
type joinGatherSelection struct {
	rows         []int
	start, count int
}

func (s joinGatherSelection) len() int {
	if s.rows != nil {
		return len(s.rows)
	}
	return s.count
}

func (s joinGatherSelection) row(i int) int {
	if s.rows != nil {
		return s.rows[i]
	}
	return s.start + i
}

func joinGatherStringSize(v *memory.StringViewVector, rows []int, parent *memory.StructVector) (int, error) {
	return joinGatherStringSizeSelected(v, joinGatherSelection{rows: rows}, parent)
}

func joinGatherStringSizeSelected(v *memory.StringViewVector, rows joinGatherSelection, parent *memory.StructVector) (int, error) {
	var bytes int64
	for i := 0; i < rows.len(); i++ {
		row := rows.row(i)
		// Top-level takeRows copies hidden NULL spans. Struct append does not.
		if parent != nil && (!parent.IsValid(row) || !v.IsValid(row)) {
			continue
		}
		if err := addJoinEstimate(&bytes, int64(v.Offsets[row+1]-v.Offsets[row])); err != nil {
			return 0, err
		}
	}
	if uint64(bytes) > math.MaxUint32 || uint64(bytes) > uint64(math.MaxInt) {
		return 0, fmt.Errorf("join gather string data exceeds offset representation")
	}
	return int(bytes), nil
}

func joinGatherListSize(v *memory.StringListVector, rows []int, parent *memory.StructVector) (int, int, error) {
	return joinGatherListSizeSelected(v, joinGatherSelection{rows: rows}, parent)
}

func joinGatherListSizeSelected(v *memory.StringListVector, rows joinGatherSelection, parent *memory.StructVector) (int, int, error) {
	var elements, bytes int64
	for i := 0; i < rows.len(); i++ {
		row := rows.row(i)
		if (parent != nil && !parent.IsValid(row)) || !v.IsValid(row) {
			continue
		}
		lo, hi := v.Offsets[row], v.Offsets[row+1]
		if err := addJoinEstimate(&elements, int64(hi-lo)); err != nil {
			return 0, 0, err
		}
		for child := uint64(lo); child < uint64(hi); child++ {
			if !v.Values.IsValid(int(child)) {
				continue
			}
			if err := addJoinEstimate(&bytes, int64(v.Values.Offsets[child+1]-v.Values.Offsets[child])); err != nil {
				return 0, 0, err
			}
		}
	}
	if err := joinGatherListSizeLimits(uint64(elements), uint64(bytes)); err != nil {
		return 0, 0, err
	}
	return int(elements), int(bytes), nil
}

// A list's child string vector needs elements+1 offsets. Both that slice length
// and its encoded uint32 offset count must fit; the final byte offset itself may
// equal MaxUint32. Check pure counts before any conversion or allocation.
func joinGatherListSizeLimits(elements, bytes uint64) error {
	if elements >= uint64(math.MaxInt) || elements >= math.MaxUint32 || bytes > uint64(math.MaxInt) || bytes > math.MaxUint32 {
		return fmt.Errorf("join gather list exceeds offset representation")
	}
	return nil
}

func joinGatherStringOwned(total *int64, rows, bytes int, bitmap bool) error {
	if err := addJoinGatherProduct(total, uint64(rows)+1, 4); err != nil {
		return err
	}
	if err := addJoinEstimate(total, int64(bytes)); err != nil {
		return err
	}
	if bitmap {
		return addJoinGatherProduct(total, uint64(joinGatherWords(rows)), 8)
	}
	return nil
}

func joinGatherListOwned(total *int64, v *memory.StringListVector, rows []int, parent *memory.StructVector) error {
	return joinGatherListOwnedSelected(total, v, joinGatherSelection{rows: rows}, parent)
}

func joinGatherListOwnedSelected(total *int64, v *memory.StringListVector, rows joinGatherSelection, parent *memory.StructVector) error {
	elements, bytes, err := joinGatherListSizeSelected(v, rows, parent)
	if err != nil {
		return err
	}
	if err := joinGatherStringOwned(total, rows.len(), 0, true); err != nil {
		return err
	}
	return joinGatherStringOwned(total, elements, bytes, true)
}

func joinGatherStructOwned(v *memory.StructVector, rows []int) (int64, int64, error) {
	return joinGatherStructOwnedSelected(v, joinGatherSelection{rows: rows})
}

func joinGatherStructOwnedSelected(v *memory.StructVector, rows joinGatherSelection) (int64, int64, error) {
	// Even an empty selection owns declared child vectors, offset sentinels,
	// and dictionary tables. Keep its accounting on the same path as other rows.
	var owned int64
	metadata := int64(4096)
	if err := addJoinGatherProduct(&metadata, uint64(len(v.Fields)), uint64(unsafe.Sizeof(memory.Field{}))); err != nil {
		return 0, 0, err
	}
	if err := addJoinGatherProduct(&owned, uint64(joinGatherWords(rows.len())), 8); err != nil {
		return 0, 0, err
	}
	for _, field := range v.Fields {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		width := 0
		switch field.Type {
		case memory.TypeInt8:
			width = 1
		case memory.TypeInt16:
			width = 2
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			width = 4
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			width = 8
		case memory.TypeUint8:
			width = 1
		case memory.TypeUint16:
			width = 2
		case memory.TypeUint32, memory.TypeBool:
			width = 4
		case memory.TypeUint64:
			width = 8
		case memory.TypeFloat32:
			width = 4
		case memory.TypeFloat64:
			width = 8
		case memory.TypeString:
			if str := v.Values.Strings[field.Name]; str != nil {
				bytes, err := joinGatherStringSizeSelected(str, rows, v)
				if err != nil {
					return 0, 0, err
				}
				if err := joinGatherStringOwned(&owned, rows.len(), bytes, true); err != nil {
					return 0, 0, err
				}
			} else {
				dictionary := v.Values.DictStrings[field.Name]
				if err := joinGatherFixedOwned(&owned, rows.len(), 4, true); err != nil {
					return 0, 0, err
				}
				if err := addJoinGatherProduct(&owned, uint64(len(dictionary.Offsets)), 4); err != nil {
					return 0, 0, err
				}
				if err := addJoinEstimate(&owned, int64(len(dictionary.Data))); err != nil {
					return 0, 0, err
				}
			}
		case memory.TypeList:
			if err := addJoinEstimate(&metadata, 512); err != nil {
				return 0, 0, err
			}
			if len(field.Children) == 1 && field.Children[0].Type == memory.TypeInt64 {
				if err := joinGatherInt64ListOwnedSelected(&owned, v.Values.Int64Lists[field.Name], rows, v); err != nil {
					return 0, 0, err
				}
				break
			}
			if err := joinGatherListOwnedSelected(&owned, v.Values.StringLists[field.Name], rows, v); err != nil {
				return 0, 0, err
			}
		default:
			return 0, 0, fmt.Errorf("unsupported bounded join gather struct child")
		}
		if width != 0 {
			if err := joinGatherFixedOwned(&owned, rows.len(), width, true); err != nil {
				return 0, 0, err
			}
		}
	}
	return owned, metadata, nil
}

// planJoinGather returns the historic full payload, exact owned buffer
// capacities, and a runtime-object metadata allowance. No source is modified.
func planJoinGather(batch *memory.Batch, rows []int) (int64, int64, int64, error) {
	if len(rows) == math.MaxInt || uint64(len(rows)) > math.MaxUint32 {
		return 0, 0, 0, fmt.Errorf("join gather row selection exceeds offset representation")
	}
	payload, err := joinGatherPayload(batch, nil)
	if err != nil {
		return 0, 0, 0, err
	}
	for _, row := range rows {
		if row < 0 || row >= batch.Rows {
			return 0, 0, 0, fmt.Errorf("join gather selected row %d out of range", row)
		}
	}
	owned, metadata, err := planJoinGatherSelected(batch, joinGatherSelection{rows: rows})
	return payload, owned, metadata, err
}

// planJoinGatherSelected computes selected capacities after the caller has
// validated the complete source graph and the selection bounds. It performs no
// whole-source dictionary-code scans, so a validated batch may feed many chunks.
func planJoinGatherSelected(batch *memory.Batch, rows joinGatherSelection) (int64, int64, error) {
	var owned int64
	metadata := int64(4096)
	for _, v := range batch.Int8s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 1, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Int16s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 2, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Int32s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 4, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Int64s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 8, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Uint8s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 1, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Uint16s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 2, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Uint32s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 4, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Uint64s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 8, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Float32s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 4, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Float64s {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 8, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Decimals {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 16, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.DictStrings {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		if err := joinGatherFixedOwned(&owned, rows.len(), 4, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Strings {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		bytes, err := joinGatherStringSizeSelected(v, rows, nil)
		if err != nil {
			return 0, 0, err
		}
		if err := joinGatherStringOwned(&owned, rows.len(), bytes, v.Validity != nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.StringLists {
		if err := addJoinEstimate(&metadata, 1024); err != nil {
			return 0, 0, err
		}
		if err := joinGatherListOwnedSelected(&owned, v, rows, nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Int64Lists {
		if err := addJoinEstimate(&metadata, 1024); err != nil {
			return 0, 0, err
		}
		if err := joinGatherInt64ListOwnedSelected(&owned, v, rows, nil); err != nil {
			return 0, 0, err
		}
	}
	for _, v := range batch.Structs {
		if err := addJoinEstimate(&metadata, 512); err != nil {
			return 0, 0, err
		}
		childOwned, childMetadata, err := joinGatherStructOwnedSelected(v, rows)
		if err != nil {
			return 0, 0, err
		}
		if err := addJoinEstimate(&owned, childOwned); err != nil {
			return 0, 0, err
		}
		if err := addJoinEstimate(&metadata, childMetadata); err != nil {
			return 0, 0, err
		}
	}
	return owned, metadata, nil
}

// takeJoinRowsBounded is used only after joinGatherAdmissionBytes is reserved.
// It repeats the allocation-free validation before modifying dst so malformed
// custom Source batches never cause partial writes or Arrow allocations.
func takeJoinRowsBounded(ctx context.Context, dst, src *memory.Batch, rows []int) error {
	if dst == nil || src == nil || dst == src {
		return fmt.Errorf("bounded join gather requires distinct non-nil batches")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, _, _, err := planJoinGather(src, rows); err != nil {
		return err
	}
	dst.ClearColumns()
	n := len(rows)
	for name, v := range src.Int8s {
		out := &memory.Int8Vector{Values: make([]int8, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Int8s[name] = out
	}
	for name, v := range src.Int16s {
		out := &memory.Int16Vector{Values: make([]int16, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Int16s[name] = out
	}
	for name, v := range src.Int32s {
		out := &memory.Int32Vector{Values: make([]int32, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Int32s[name] = out
	}
	for name, v := range src.Int64s {
		out := &memory.Int64Vector{Values: make([]int64, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Int64s[name] = out
	}
	for name, v := range src.Uint8s {
		out := &memory.Uint8Vector{Values: make([]uint8, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Uint8s[name] = out
	}
	for name, v := range src.Uint16s {
		out := &memory.Uint16Vector{Values: make([]uint16, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Uint16s[name] = out
	}
	for name, v := range src.Uint32s {
		out := &memory.Uint32Vector{Values: make([]uint32, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Uint32s[name] = out
	}
	for name, v := range src.Uint64s {
		out := &memory.Uint64Vector{Values: make([]uint64, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Uint64s[name] = out
	}
	for name, v := range src.Float32s {
		out := &memory.Float32Vector{Values: make([]float32, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Float32s[name] = out
	}
	for name, v := range src.Float64s {
		out := &memory.Float64Vector{Values: make([]float64, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Float64s[name] = out
	}
	for name, v := range src.Decimals {
		out := &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: compactValidity(nil, v.Validity, rows), Precision: v.Precision, Scale: v.Scale}
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		dst.Decimals[name] = out
	}
	for name, v := range src.DictStrings {
		out := &memory.DictionaryStringVector{Indices: make([]uint32, n), Validity: compactValidity(nil, v.Validity, rows), Offsets: v.Offsets, Data: v.Data, DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
		for i, row := range rows {
			out.Indices[i] = v.Indices[row]
		}
		dst.DictStrings[name] = out
	}
	for name, v := range src.Strings {
		bytes, err := joinGatherStringSize(v, rows, nil)
		if err != nil {
			return err
		}
		out := &memory.StringViewVector{Offsets: make([]uint32, n+1), Data: make([]byte, 0, bytes), Validity: compactValidity(nil, v.Validity, rows), Encoding: memory.EncodingView}
		for i, row := range rows {
			out.Data = append(out.Data, v.ValueBytes(row)...)
			out.Offsets[i+1] = uint32(len(out.Data))
		}
		dst.Strings[name] = out
	}
	for name, v := range src.StringLists {
		elements, bytes, err := joinGatherListSize(v, rows, nil)
		if err != nil {
			return err
		}
		out := plannedJoinList(n, elements, bytes)
		for _, row := range rows {
			if err := memory.AppendStringListRow(out, v, row); err != nil {
				return err
			}
		}
		dst.StringLists[name] = out
	}
	for name, v := range src.Int64Lists {
		elements, err := joinGatherInt64ListSize(v, rows, nil)
		if err != nil {
			return err
		}
		out := plannedJoinInt64List(n, elements)
		for _, row := range rows {
			if err := memory.AppendInt64ListRow(out, v, row); err != nil {
				return err
			}
		}
		dst.Int64Lists[name] = out
	}
	for name, v := range src.Structs {
		out, err := takeJoinStructRowsBounded(v, rows)
		if err != nil {
			return err
		}
		dst.Structs[name] = out
	}
	dst.Reset(n)
	return nil
}

func takeJoinStructRowsBounded(src *memory.StructVector, rows []int) (*memory.StructVector, error) {
	n := len(rows)
	out := &memory.StructVector{Fields: make([]memory.Field, len(src.Fields)), Values: memory.NewBatch(), Encoding: memory.EncodingView}
	copy(out.Fields, src.Fields)
	out.Validity = make(memory.Bitmap, joinGatherWords(n))
	for _, field := range src.Fields {
		switch field.Type {
		case memory.TypeInt8:
			out.Values.Int8s[field.Name] = &memory.Int8Vector{Values: make([]int8, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeInt16:
			out.Values.Int16s[field.Name] = &memory.Int16Vector{Values: make([]int16, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			out.Values.Int32s[field.Name] = &memory.Int32Vector{Values: make([]int32, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			out.Values.Int64s[field.Name] = &memory.Int64Vector{Values: make([]int64, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeUint8:
			out.Values.Uint8s[field.Name] = &memory.Uint8Vector{Values: make([]uint8, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeUint16:
			out.Values.Uint16s[field.Name] = &memory.Uint16Vector{Values: make([]uint16, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeUint32, memory.TypeBool:
			out.Values.Uint32s[field.Name] = &memory.Uint32Vector{Values: make([]uint32, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeUint64:
			out.Values.Uint64s[field.Name] = &memory.Uint64Vector{Values: make([]uint64, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeFloat32:
			out.Values.Float32s[field.Name] = &memory.Float32Vector{Values: make([]float32, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeFloat64:
			out.Values.Float64s[field.Name] = &memory.Float64Vector{Values: make([]float64, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Encoding: field.Encoding}
		case memory.TypeString:
			if v := src.Values.Strings[field.Name]; v != nil {
				bytes, err := joinGatherStringSize(v, rows, src)
				if err != nil {
					return nil, err
				}
				out.Values.Strings[field.Name] = plannedJoinString(n, bytes)
			} else {
				v := src.Values.DictStrings[field.Name]
				dictionary := &memory.DictionaryStringVector{Indices: make([]uint32, 0, n), Validity: make(memory.Bitmap, joinGatherWords(n)), Offsets: make([]uint32, len(v.Offsets)), Data: make([]byte, len(v.Data)), DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
				copy(dictionary.Offsets, v.Offsets)
				copy(dictionary.Data, v.Data)
				out.Values.DictStrings[field.Name] = dictionary
			}
		case memory.TypeList:
			if len(field.Children) == 1 && field.Children[0].Type == memory.TypeInt64 {
				elements, err := joinGatherInt64ListSize(src.Values.Int64Lists[field.Name], rows, src)
				if err != nil {
					return nil, err
				}
				out.Values.Int64Lists[field.Name] = plannedJoinInt64List(n, elements)
				break
			}
			elements, bytes, err := joinGatherListSize(src.Values.StringLists[field.Name], rows, src)
			if err != nil {
				return nil, err
			}
			out.Values.StringLists[field.Name] = plannedJoinList(n, elements, bytes)
		default:
			return nil, fmt.Errorf("unsupported bounded join gather struct child")
		}
	}
	for _, row := range rows {
		if err := memory.AppendStructRow(out, src, row, src.IsValid(row)); err != nil {
			return nil, err
		}
	}
	return out, nil
}
