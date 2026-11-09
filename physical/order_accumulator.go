package physical

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// externalSortAccumulator is exclusive to the budgeted external sorter. Its
// sparse object graph has the decoder's metadata model: only hinted maps in
// use, one descriptor per field, list child descriptors, and separate shallow
// Struct.Fields arrays and child batches. Schema/name/Children storage is
// borrowed. No resident/Top-K accumulator or opaque column uses this owner.
//
// capacityBytes counts current backing capacities, not logical lengths. Before
// each make, the token covers metadata + current capacities + the entire new
// buffer (including overlap with the old buffer) + held dictionary identities.
// The token retains its peak until close; payloadFloor is only a lower bound.
// Individual geometric buffers have cap < 2*need, and old+new <= 3*need at
// growth. This does NOT imply 3*source-row-size covers late validity, domains,
// or canonicalized nested columns: actual admission is always checked here.
type externalSortAccumulator struct {
	schema        memory.Schema
	batch         *memory.Batch
	reservation   *sortWorkspaceReservation
	metadataBytes int64
	capacityBytes int64
	domainBytes   int64
	finished      bool
	failed        bool
}

func externalSortAccumulatorMetadataBytes(schema memory.Schema) (int64, error) {
	// This also validates duplicates, decimal metadata, and the codec's bounded
	// nesting without allocating on success. Keep the constructor graph in sync
	// with that model rather than calling memory.NewBatch (which creates all maps).
	n, err := memory.ColumnarRunDecodeMetadataBytes(schema)
	if err != nil {
		return 0, err
	}
	owner := (int64(unsafe.Sizeof(externalSortAccumulator{})) + 15) &^ 15
	token := (int64(unsafe.Sizeof(sortWorkspaceReservation{})) + 15) &^ 15
	return sortWorkspaceSize(n, owner, token)
}

func newExternalSortAccumulator(ctx context.Context, schema memory.Schema, workspace *sortWorkspace, payloadFloor int64) (*externalSortAccumulator, error) {
	if ctx == nil || workspace == nil {
		return nil, fmt.Errorf("external sort accumulator requires context and workspace")
	}
	metadata, err := externalSortAccumulatorMetadataBytes(schema)
	if err != nil {
		return nil, err
	}
	total, err := sortWorkspaceSize(metadata, payloadFloor)
	if err != nil {
		return nil, err
	}
	reservation, err := workspace.Reserve(ctx, total)
	if err != nil {
		return nil, err
	}
	// No owner, maps, descriptors, or field arrays exist before admission.
	return &externalSortAccumulator{
		schema: schema, batch: newExternalSortAccumulatorBatch(schema.Fields),
		reservation: reservation.(*sortWorkspaceReservation), metadataBytes: metadata,
	}, nil
}

func externalSortAccumulatorKind(field memory.Field) int {
	switch field.Type {
	case memory.TypeInt8:
		return 0
	case memory.TypeInt16:
		return 1
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		return 2
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return 3
	case memory.TypeUint8:
		return 4
	case memory.TypeUint16:
		return 5
	case memory.TypeUint32, memory.TypeBool:
		return 6
	case memory.TypeUint64:
		return 7
	case memory.TypeFloat32:
		return 8
	case memory.TypeFloat64:
		return 9
	case memory.TypeDecimal:
		return 10
	case memory.TypeString, memory.TypeBinary:
		if field.Encoding == memory.EncodingDictionary {
			return 12
		}
		return 11
	case memory.TypeList:
		return 13
	case memory.TypeStruct:
		return 14
	default:
		panic("unvalidated external sort accumulator schema")
	}
}

func newExternalSortAccumulatorBatch(fields []memory.Field) *memory.Batch {
	var counts [15]int
	for _, field := range fields {
		counts[externalSortAccumulatorKind(field)]++
	}
	b := &memory.Batch{}
	if n := counts[0]; n > 0 {
		b.Int8s = make(map[string]*memory.Int8Vector, n)
	}
	if n := counts[1]; n > 0 {
		b.Int16s = make(map[string]*memory.Int16Vector, n)
	}
	if n := counts[2]; n > 0 {
		b.Int32s = make(map[string]*memory.Int32Vector, n)
	}
	if n := counts[3]; n > 0 {
		b.Int64s = make(map[string]*memory.Int64Vector, n)
	}
	if n := counts[4]; n > 0 {
		b.Uint8s = make(map[string]*memory.Uint8Vector, n)
	}
	if n := counts[5]; n > 0 {
		b.Uint16s = make(map[string]*memory.Uint16Vector, n)
	}
	if n := counts[6]; n > 0 {
		b.Uint32s = make(map[string]*memory.Uint32Vector, n)
	}
	if n := counts[7]; n > 0 {
		b.Uint64s = make(map[string]*memory.Uint64Vector, n)
	}
	if n := counts[8]; n > 0 {
		b.Float32s = make(map[string]*memory.Float32Vector, n)
	}
	if n := counts[9]; n > 0 {
		b.Float64s = make(map[string]*memory.Float64Vector, n)
	}
	if n := counts[10]; n > 0 {
		b.Decimals = make(map[string]*memory.Decimal128Vector, n)
	}
	if n := counts[11]; n > 0 {
		b.Strings = make(map[string]*memory.StringViewVector, n)
	}
	if n := counts[12]; n > 0 {
		b.DictStrings = make(map[string]*memory.DictionaryStringVector, n)
	}
	if n := counts[13]; n > 0 {
		b.StringLists = make(map[string]*memory.StringListVector, n)
	}
	if n := counts[14]; n > 0 {
		b.Structs = make(map[string]*memory.StructVector, n)
	}
	for _, f := range fields {
		switch f.Type {
		case memory.TypeInt8:
			b.Int8s[f.Name] = &memory.Int8Vector{Encoding: f.Encoding}
		case memory.TypeInt16:
			b.Int16s[f.Name] = &memory.Int16Vector{Encoding: f.Encoding}
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			b.Int32s[f.Name] = &memory.Int32Vector{Encoding: f.Encoding}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			b.Int64s[f.Name] = &memory.Int64Vector{Encoding: f.Encoding}
		case memory.TypeUint8:
			b.Uint8s[f.Name] = &memory.Uint8Vector{Encoding: f.Encoding}
		case memory.TypeUint16:
			b.Uint16s[f.Name] = &memory.Uint16Vector{Encoding: f.Encoding}
		case memory.TypeUint32, memory.TypeBool:
			b.Uint32s[f.Name] = &memory.Uint32Vector{Encoding: f.Encoding}
		case memory.TypeUint64:
			b.Uint64s[f.Name] = &memory.Uint64Vector{Encoding: f.Encoding}
		case memory.TypeFloat32:
			b.Float32s[f.Name] = &memory.Float32Vector{Encoding: f.Encoding}
		case memory.TypeFloat64:
			b.Float64s[f.Name] = &memory.Float64Vector{Encoding: f.Encoding}
		case memory.TypeDecimal:
			b.Decimals[f.Name] = &memory.Decimal128Vector{Precision: f.DecimalPrecision, Scale: f.DecimalScale}
		case memory.TypeString, memory.TypeBinary:
			if f.Encoding == memory.EncodingDictionary {
				b.DictStrings[f.Name] = &memory.DictionaryStringVector{Encoding: memory.EncodingDictionary}
			} else {
				b.Strings[f.Name] = &memory.StringViewVector{Encoding: memory.EncodingView}
			}
		case memory.TypeList:
			b.StringLists[f.Name] = &memory.StringListVector{Values: &memory.StringViewVector{Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
		case memory.TypeStruct:
			fields := make([]memory.Field, len(f.Children))
			copy(fields, f.Children) // Own only the outer array, never the names/Children.
			b.Structs[f.Name] = &memory.StructVector{Fields: fields, Values: newExternalSortAccumulatorBatch(f.Children), Encoding: memory.EncodingView}
		}
	}
	return b
}

// externalSortBufferCapacity checks both the Go int and codec uint32 limits.
// It never delegates capacity selection to append/runtime.growslice, including
// a first int8 allocation, where implicit append would round cap(1) up to 8.
func externalSortBufferCapacity(old, need int) (int, error) {
	if old < 0 || need < 0 || uint64(old) > math.MaxUint32 || uint64(need) > math.MaxUint32 {
		return 0, memory.ErrBudgetExceeded
	}
	if need <= old {
		return old, nil
	}
	maxInt := int(^uint(0) >> 1)
	if old > maxInt/2 || uint64(old) > math.MaxUint32/2 {
		// The requested length still fits: omit speculative doubling rather
		// than rejecting an otherwise representable buffer.
		return need, nil
	}
	return max(need, 2*old), nil
}

func externalSortBufferBytes(capacity int, width uintptr) (int64, error) {
	if capacity < 0 || uint64(capacity) > math.MaxUint32 || uint64(width) > math.MaxInt64 || width != 0 && uint64(capacity) > uint64(math.MaxInt64)/uint64(width) {
		return 0, memory.ErrBudgetExceeded
	}
	return int64(capacity) * int64(width), nil
}

func (a *externalSortAccumulator) ensure(ctx context.Context, extra int64) error {
	if ctx == nil {
		return fmt.Errorf("external sort accumulator requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a == nil || a.reservation == nil || a.reservation.done.Load() {
		return errSortWorkspaceClosed
	}
	want, err := sortWorkspaceSize(a.metadataBytes, a.capacityBytes, a.domainBytes, extra)
	if err != nil {
		return err
	}
	if want <= a.reservation.Bytes() {
		return nil
	}
	return a.reservation.grow(ctx, want-a.reservation.Bytes())
}

func externalSortGrowBuffer[T any](ctx context.Context, a *externalSortAccumulator, old []T, need int) ([]T, error) {
	capacity, err := externalSortBufferCapacity(cap(old), need)
	if err != nil {
		return old, err
	}
	if capacity == cap(old) {
		return old, nil
	}
	var element T
	bytes, err := externalSortBufferBytes(capacity, unsafe.Sizeof(element))
	if err != nil {
		return old, err
	}
	oldBytes, err := externalSortBufferBytes(cap(old), unsafe.Sizeof(element))
	if err != nil {
		return old, err
	}
	// Charge coexistence, not just (new-old), before make. On a refusal neither
	// the buffer nor its accounting changes. Prior successful column growth is
	// still reachable and charged until the caller closes the failed owner.
	if err = a.ensure(ctx, bytes); err != nil {
		return old, err
	}
	out := make([]T, len(old), capacity)
	copy(out, old)
	a.capacityBytes += bytes - oldBytes
	return out, nil
}

func externalSortAppendValue[T any](ctx context.Context, a *externalSortAccumulator, dst *[]T, value T) error {
	if uint64(len(*dst)) >= math.MaxUint32 || len(*dst) == int(^uint(0)>>1) {
		return memory.ErrBudgetExceeded
	}
	grown, err := externalSortGrowBuffer(ctx, a, *dst, len(*dst)+1)
	if err != nil {
		return err
	}
	n := len(grown)
	*dst = grown[:n+1]
	(*dst)[n] = value
	return nil
}

func externalSortValidity(validity memory.Bitmap, row int) (bool, error) {
	if row < 0 || validity != nil && row/64 >= len(validity) {
		return false, fmt.Errorf("external sort validity row out of range")
	}
	return validity == nil || memory.IsSet(validity, row), nil
}

func (a *externalSortAccumulator) appendValidity(ctx context.Context, dst *memory.Bitmap, valid, explicit bool, row int) error {
	if row < 0 || uint64(row) >= math.MaxUint32 {
		return memory.ErrBudgetExceeded
	}
	if *dst == nil && valid && !explicit {
		return nil
	}
	// Avoid WordsFor(row+1)'s potentially overflowing rows+63 arithmetic.
	need := row/64 + 1
	first := *dst == nil
	oldLen := len(*dst)
	grown, err := externalSortGrowBuffer(ctx, a, []uint64(*dst), need)
	if err != nil {
		return err
	}
	*dst = grown[:need]
	clear((*dst)[oldLen:])
	if first {
		// The absent bitmap represented all previous rows as valid.
		for i := range *dst {
			(*dst)[i] = ^uint64(0)
		}
		(*dst)[need-1] &= memory.TailMask(row + 1)
	}
	mask := uint64(1) << uint(row&63)
	if valid {
		(*dst)[row/64] |= mask
	} else {
		(*dst)[row/64] &^= mask
	}
	return nil
}

func externalSortAppendScalar[T any](ctx context.Context, a *externalSortAccumulator, dst *[]T, validity *memory.Bitmap, src []T, srcValidity memory.Bitmap, row int, parentValid, canonical bool) error {
	if row < 0 || row >= len(src) {
		return fmt.Errorf("external sort scalar row out of range")
	}
	valid, err := externalSortValidity(srcValidity, row)
	if err != nil {
		return err
	}
	valid = valid && parentValid
	value := src[row]
	if canonical && !valid {
		var zero T
		value = zero
	}
	dstRow := len(*dst)
	// Validity is admitted before values are logically appended. Both buffers
	// remain owned if a later field refuses; failed owners are not reusable.
	if err := a.appendValidity(ctx, validity, valid, !canonical && srcValidity != nil, dstRow); err != nil {
		return err
	}
	return externalSortAppendValue(ctx, a, dst, value)
}

var errExternalSortAccumulatorFailed = errors.New("external sort accumulator failed; close is required")

func (a *externalSortAccumulator) appendRow(ctx context.Context, src *memory.Batch, row int) (err error) {
	if a == nil || a.batch == nil || a.reservation == nil {
		return errSortWorkspaceClosed
	}
	if a.failed {
		return errExternalSortAccumulatorFailed
	}
	defer func() {
		if err != nil {
			a.failed = true
		}
	}()

	if a.finished {
		return fmt.Errorf("external sort accumulator already finished")
	}
	if src == nil || row < 0 || row >= src.Rows {
		return fmt.Errorf("external sort source row out of range")
	}
	if err := a.ensure(ctx, 0); err != nil {
		return err
	}
	if uint64(a.batch.Rows) >= math.MaxUint32 || a.batch.Rows == int(^uint(0)>>1) {
		return memory.ErrBudgetExceeded
	}
	if err := a.appendFields(ctx, a.batch, src, a.schema.Fields, row, true, false); err != nil {
		return err
	}
	a.batch.Rows++
	return nil
}

func (a *externalSortAccumulator) appendFields(ctx context.Context, dst, src *memory.Batch, fields []memory.Field, row int, parentValid, canonical bool) error {
	for _, f := range fields {
		switch f.Type {
		case memory.TypeInt8:
			v, out := src.Int8s[f.Name], dst.Int8s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeInt16:
			v, out := src.Int16s[f.Name], dst.Int16s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			v, out := src.Int32s[f.Name], dst.Int32s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			v, out := src.Int64s[f.Name], dst.Int64s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeUint8:
			v, out := src.Uint8s[f.Name], dst.Uint8s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeUint16:
			v, out := src.Uint16s[f.Name], dst.Uint16s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeUint32, memory.TypeBool:
			v, out := src.Uint32s[f.Name], dst.Uint32s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeUint64:
			v, out := src.Uint64s[f.Name], dst.Uint64s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeFloat32:
			v, out := src.Float32s[f.Name], dst.Float32s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeFloat64:
			v, out := src.Float64s[f.Name], dst.Float64s[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeDecimal:
			v, out := src.Decimals[f.Name], dst.Decimals[f.Name]
			if v == nil || out == nil {
				return fmt.Errorf("column %q missing", f.Name)
			}
			if v.Precision != f.DecimalPrecision || v.Scale != f.DecimalScale {
				return fmt.Errorf("decimal column %q metadata mismatch", f.Name)
			}
			if err := externalSortAppendScalar(ctx, a, &out.Values, &out.Validity, v.Values, v.Validity, row, parentValid, canonical); err != nil {
				return err
			}
		case memory.TypeString, memory.TypeBinary:
			if f.Encoding == memory.EncodingDictionary {
				if err := a.appendDictionary(ctx, dst.DictStrings[f.Name], src.DictStrings[f.Name], row); err != nil {
					return err
				}
			} else {
				if err := a.appendString(ctx, dst.Strings[f.Name], src.Strings[f.Name], row, parentValid, canonical); err != nil {
					return err
				}
			}
		case memory.TypeList:
			if err := a.appendList(ctx, dst.StringLists[f.Name], src.StringLists[f.Name], row, parentValid); err != nil {
				return err
			}
		case memory.TypeStruct:
			v, out := src.Structs[f.Name], dst.Structs[f.Name]
			if v == nil || out == nil || v.Values == nil || row >= v.Rows || row >= v.Values.Rows {
				return fmt.Errorf("struct column %q missing or malformed", f.Name)
			}
			valid, err := externalSortValidity(v.Validity, row)
			if err != nil {
				return err
			}
			valid = parentValid && valid
			if err := a.appendValidity(ctx, &out.Validity, valid, false, out.Rows); err != nil {
				return err
			}
			if err := a.appendFields(ctx, out.Values, v.Values, f.Children, row, valid, true); err != nil {
				return err
			}
			out.Rows++
			out.Values.Rows = out.Rows
		default:
			return fmt.Errorf("unsupported external sort field %q", f.Name)
		}
	}
	return nil
}

func (a *externalSortAccumulator) initOffsets(ctx context.Context, offsets *[]uint32) error {
	if len(*offsets) != 0 {
		return nil
	}
	return externalSortAppendValue(ctx, a, offsets, uint32(0))
}

func (a *externalSortAccumulator) appendBytes(ctx context.Context, dst *[]byte, value []byte) error {
	if uint64(len(*dst))+uint64(len(value)) > math.MaxUint32 || len(value) > int(^uint(0)>>1)-len(*dst) {
		return memory.ErrBudgetExceeded
	}
	need := len(*dst) + len(value)
	grown, err := externalSortGrowBuffer(ctx, a, *dst, need)
	if err != nil {
		return err
	}
	oldLen := len(grown)
	*dst = grown[:need]
	copy((*dst)[oldLen:], value)
	return nil
}

func (a *externalSortAccumulator) appendString(ctx context.Context, dst, src *memory.StringViewVector, row int, parentValid, canonical bool) error {
	if dst == nil || src == nil || row < 0 || row >= src.Len() {
		return fmt.Errorf("external sort string row missing or out of range")
	}
	valid, err := externalSortValidity(src.Validity, row)
	if err != nil {
		return err
	}
	valid = valid && parentValid
	var value []byte
	// Top-level string/binary preserves the hidden byte span of NULLs. Nested
	// lists and structs use the existing canonical empty span for NULLs.
	if !canonical || valid {
		start, end := src.Offsets[row], src.Offsets[row+1]
		if start > end || uint64(end) > uint64(len(src.Data)) {
			return fmt.Errorf("external sort string offsets out of range")
		}
		value = src.Data[start:end]
	}
	if err := a.initOffsets(ctx, &dst.Offsets); err != nil {
		return err
	}
	if err := a.appendValidity(ctx, &dst.Validity, valid, !canonical && src.Validity != nil, dst.Len()); err != nil {
		return err
	}
	if err := a.appendBytes(ctx, &dst.Data, value); err != nil {
		return err
	}
	return externalSortAppendValue(ctx, a, &dst.Offsets, uint32(len(dst.Data)))
}

func (a *externalSortAccumulator) appendList(ctx context.Context, dst, src *memory.StringListVector, row int, parentValid bool) error {
	if dst == nil || src == nil || src.Values == nil || row < 0 || row >= src.Len() {
		return fmt.Errorf("external sort list row missing or out of range")
	}
	valid, err := externalSortValidity(src.Validity, row)
	if err != nil {
		return err
	}
	valid = valid && parentValid
	if err := a.initOffsets(ctx, &dst.Offsets); err != nil {
		return err
	}
	if err := a.initOffsets(ctx, &dst.Values.Offsets); err != nil {
		return err
	}
	if err := a.appendValidity(ctx, &dst.Validity, valid, false, dst.Len()); err != nil {
		return err
	}
	if valid {
		start, end := src.Offsets[row], src.Offsets[row+1]
		if start > end || uint64(end) > uint64(src.Values.Len()) {
			return fmt.Errorf("external sort list offsets out of range")
		}
		if uint64(dst.Values.Len())+uint64(end-start)+1 > math.MaxUint32 {
			return memory.ErrBudgetExceeded
		}
		for child := int(start); child < int(end); child++ {
			if err := a.appendString(ctx, dst.Values, src.Values, child, true, true); err != nil {
				return err
			}
		}
	}
	return externalSortAppendValue(ctx, a, &dst.Offsets, uint32(dst.Values.Len()))
}

func (a *externalSortAccumulator) appendDictionary(ctx context.Context, dst, src *memory.DictionaryStringVector, row int) error {
	if dst == nil || src == nil || row < 0 || row >= len(src.Indices) {
		return fmt.Errorf("external sort dictionary row missing or out of range")
	}
	valid, err := externalSortValidity(src.Validity, row)
	if err != nil {
		return err
	}
	var code uint32
	var value []byte
	if valid {
		code = src.Indices[row]
		if uint64(code)+1 >= uint64(len(src.Offsets)) {
			return fmt.Errorf("external sort dictionary code out of range")
		}
		start, end := src.Offsets[code], src.Offsets[code+1]
		if start > end || uint64(end) > uint64(len(src.Data)) {
			return fmt.Errorf("external sort dictionary offsets out of range")
		}
		value = src.Data[start:end]
	}
	if len(dst.Offsets) == 0 {
		// Copy the full first table's effective window, even when the selected row
		// is NULL. Prefix/suffix bytes are not part of the dictionary. Preserve
		// duplicate codes in that table, but never duplicate newly merged values.
		var base, end uint32
		if len(src.Offsets) != 0 {
			base, end = src.Offsets[0], src.Offsets[len(src.Offsets)-1]
			if base > end || uint64(end) > uint64(len(src.Data)) {
				return fmt.Errorf("external sort dictionary window out of range")
			}
			previous := base
			for _, offset := range src.Offsets {
				if offset < previous || offset > end {
					return fmt.Errorf("external sort dictionary offsets not ordered")
				}
				previous = offset
			}
		}
		if uint64(len(src.Offsets)) > math.MaxUint32 {
			return memory.ErrBudgetExceeded
		}
		// Pay for this retained reference BEFORE borrowing it, independently of
		// payloadFloor. Subsequent growth accounts the domain along with buffers.
		if err := a.ensure(ctx, int64(len(src.DictionaryDomain))); err != nil {
			return err
		}
		dst.DictionaryDomain = src.DictionaryDomain
		a.domainBytes += int64(len(src.DictionaryDomain))
		grown, err := externalSortGrowBuffer(ctx, a, dst.Offsets, max(1, len(src.Offsets)))
		if err != nil {
			return err
		}
		dst.Offsets = grown[:max(1, len(src.Offsets))]
		for i, offset := range src.Offsets {
			dst.Offsets[i] = offset - base
		}
		if err := a.appendBytes(ctx, &dst.Data, src.Data[base:end]); err != nil {
			return err
		}
	} else if valid {
		// Identity is not evidence that the same code denotes the same bytes.
		// Check actual values and remap across reordered or dishonest domains.
		if uint64(code)+1 >= uint64(len(dst.Offsets)) || !bytes.Equal(dst.DictionaryValue(code), value) {
			found := false
			for i := 0; i+1 < len(dst.Offsets); i++ {
				if bytes.Equal(dst.DictionaryValue(uint32(i)), value) {
					code, found = uint32(i), true
					break
				}
			}
			if !found {
				if !sortDictionaryCanAppend(uint64(len(dst.Offsets)), uint64(len(dst.Data)), uint64(len(value))) {
					return memory.ErrBudgetExceeded
				}
				code = uint32(len(dst.Offsets) - 1)
				if err := a.appendBytes(ctx, &dst.Data, value); err != nil {
					return err
				}
				// The old identity must not survive a table mutation, even if a later
				// allocation refuses and the caller inspects the failed owner's graph.
				a.domainBytes -= int64(len(dst.DictionaryDomain))
				dst.DictionaryDomain = ""
				if err := externalSortAppendValue(ctx, a, &dst.Offsets, uint32(len(dst.Data))); err != nil {
					return err
				}
			}
		}
	}
	if err := a.appendValidity(ctx, &dst.Validity, valid, src.Validity != nil, len(dst.Indices)); err != nil {
		return err
	}
	return externalSortAppendValue(ctx, a, &dst.Indices, code)
}

// sha256.New's digest, its working words, sum, length word, and hex buffer
// are bounded; 1024 conservatively covers these temporaries (including the
// digest copy made by Sum and its padding block) even if the Go
// compiler spills them to heap. The retained 64-byte domain is charged apart.
const externalSortDictionaryFinishScratch int64 = 1024

func (a *externalSortAccumulator) finish(ctx context.Context) (err error) {
	if a == nil || a.batch == nil || a.reservation == nil {
		return errSortWorkspaceClosed
	}
	if a.failed {
		return errExternalSortAccumulatorFailed
	}
	defer func() {
		if err != nil {
			a.failed = true
		}
	}()
	if err := a.ensure(ctx, 0); err != nil {
		return err
	}
	if a.finished {
		return nil
	}
	for _, v := range a.batch.DictStrings {
		if err := a.initOffsets(ctx, &v.Offsets); err != nil {
			return err
		}
		if v.DictionaryDomain != "" {
			continue
		}
		if err := a.ensure(ctx, 64+externalSortDictionaryFinishScratch); err != nil {
			return err
		}
		h := sha256.New()
		_, _ = h.Write([]byte("GoLume sorted dictionary v1"))
		var word [8]byte
		binary.LittleEndian.PutUint64(word[:], uint64(len(v.Offsets)))
		_, _ = h.Write(word[:])
		for _, offset := range v.Offsets {
			binary.LittleEndian.PutUint32(word[:4], offset)
			_, _ = h.Write(word[:4])
		}
		_, _ = h.Write(v.Data)
		var sum [sha256.Size]byte
		digest := h.Sum(sum[:0])
		var encoded [sha256.Size * 2]byte
		hex.Encode(encoded[:], digest)
		v.DictionaryDomain = string(encoded[:])
		a.domainBytes += int64(len(v.DictionaryDomain))
	}
	if err := a.finishOffsets(ctx, a.batch); err != nil {
		return err
	}
	total, err := sortWorkspaceSize(a.metadataBytes, a.capacityBytes, a.domainBytes)
	if err != nil {
		return err
	}
	if total > a.reservation.Bytes() {
		return memory.ErrBudgetExceeded
	}
	a.finished = true
	return nil
}

func (a *externalSortAccumulator) finishOffsets(ctx context.Context, b *memory.Batch) error {
	for _, v := range b.Strings {
		if err := a.initOffsets(ctx, &v.Offsets); err != nil {
			return err
		}
	}
	for _, v := range b.StringLists {
		if err := a.initOffsets(ctx, &v.Offsets); err != nil {
			return err
		}
		if err := a.initOffsets(ctx, &v.Values.Offsets); err != nil {
			return err
		}
	}
	for _, v := range b.Structs {
		if err := a.finishOffsets(ctx, v.Values); err != nil {
			return err
		}
	}
	return nil
}

func clearExternalSortAccumulatorVectors[T any](vectors map[string]*T) {
	for _, v := range vectors {
		if v != nil {
			var zero T
			*v = zero
		}
	}
	clear(vectors)
}

func destroyExternalSortAccumulatorBatch(b *memory.Batch) {
	if b == nil {
		return
	}
	for _, v := range b.Structs {
		if v != nil {
			destroyExternalSortAccumulatorBatch(v.Values)
			clear(v.Fields) // Shallow: source schema names and Children remain intact.
			*v = memory.StructVector{}
		}
	}
	for _, v := range b.StringLists {
		if v != nil {
			if v.Values != nil {
				*v.Values = memory.StringViewVector{}
			}
			*v = memory.StringListVector{}
		}
	}
	clearExternalSortAccumulatorVectors(b.Int8s)
	clearExternalSortAccumulatorVectors(b.Int16s)
	clearExternalSortAccumulatorVectors(b.Int32s)
	clearExternalSortAccumulatorVectors(b.Int64s)
	clearExternalSortAccumulatorVectors(b.Uint8s)
	clearExternalSortAccumulatorVectors(b.Uint16s)
	clearExternalSortAccumulatorVectors(b.Uint32s)
	clearExternalSortAccumulatorVectors(b.Uint64s)
	clearExternalSortAccumulatorVectors(b.Float32s)
	clearExternalSortAccumulatorVectors(b.Float64s)
	clearExternalSortAccumulatorVectors(b.Decimals)
	clearExternalSortAccumulatorVectors(b.Strings)
	clearExternalSortAccumulatorVectors(b.DictStrings)
	clear(b.StringLists)
	clear(b.Structs)
	*b = memory.Batch{}
}

func (a *externalSortAccumulator) close() {
	if a == nil {
		return
	}
	// First sever every still reachable owned descriptor/map/outer Fields and
	// borrowed schema/domain reference; only then return the allocation token.
	destroyExternalSortAccumulatorBatch(a.batch)
	reservation := a.reservation
	*a = externalSortAccumulator{}
	if reservation != nil {
		reservation.Release()
	}
}
