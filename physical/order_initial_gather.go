package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"sort"
	"unsafe"
)

// externalSortInitialGather owns the complete gathered runtime graph, including
// columns absent from the public schema. Each runtime alias is copied separately.
// Source names, child schema trees, top-level dictionary tables and dictionary
// domains are borrowed only until close; the source must stay immutable and live.
// Extra Opaque columns used to reach generic takeRows. They are now deliberately
// rejected: Arrow Take has no bounded allocation plan for this owner.
type externalSortInitialGather struct {
	batch            *memory.Batch
	reservation      memory.Reservation
	indices          []int
	comparator       batchComparator
	displacedOrdinal *memory.Uint64Vector
}

type externalSortInitialGatherPlan struct {
	gatherBytes, encodeBytes, rangeBytes int64
	capacityBytes, metadataBytes         int64
	start, end                           int
	source                               *memory.Batch
	keyCount                             int
}

// validateExternalSortInitialGather must precede any recursive range estimator.
// It bounds depth, rejects cycles/nil children/bad offsets and unsupported opaque
// arrays, and validates all runtime maps, including unlisted Struct children.
// All-NULL dictionaries with no table remain valid runtime extras. Declaring
// such a dictionary in the encoded schema still reaches the codec's rejection.
func validateExternalSortInitialGather(batch *memory.Batch) error {
	_, err := joinGatherPayloadMode(batch, nil, true)
	return err
}

func externalSortInitialGatherRange(batch *memory.Batch, start, end, keyCount int, ordinalBase uint64) error {
	if batch == nil || start < 0 || end < start || end > batch.Rows || keyCount < 0 {
		return fmt.Errorf("external sort initial gather invalid range or key count")
	}
	rows := end - start
	if rows == math.MaxInt || uint64(rows) >= math.MaxUint32 {
		return fmt.Errorf("external sort initial gather range exceeds offset representation")
	}
	if rows != 0 && uint64(end-1) > math.MaxUint64-ordinalBase {
		return fmt.Errorf("external sort row ordinal overflows uint64")
	}
	return nil
}

func planExternalSortInitialGather(schema memory.Schema, batch *memory.Batch, start, end, keyCount int, ordinalBase uint64) (externalSortInitialGatherPlan, error) {
	if err := externalSortInitialGatherRange(batch, start, end, keyCount, ordinalBase); err != nil {
		return externalSortInitialGatherPlan{}, err
	}
	if err := validateExternalSortInitialGather(batch); err != nil {
		return externalSortInitialGatherPlan{}, err
	}
	return planExternalSortInitialGatherValidated(schema, batch, start, end, keyCount, ordinalBase)
}

// The source graph must already have passed validateExternalSortInitialGather
// and must not change. Reusing that validation avoids rescanning every dictionary
// code in the complete input for every small initial run.
func planExternalSortInitialGatherValidated(schema memory.Schema, batch *memory.Batch, start, end, keyCount int, ordinalBase uint64) (externalSortInitialGatherPlan, error) {
	var p externalSortInitialGatherPlan
	if err := externalSortInitialGatherRange(batch, start, end, keyCount, ordinalBase); err != nil {
		return p, err
	}
	selection := joinGatherSelection{start: start, count: end - start}
	owned, _, err := planJoinGatherSelected(batch, selection)
	if err != nil {
		return p, err
	}
	metadata, err := externalSortInitialGatherMetadata(batch, selection.len())
	if err != nil {
		return p, err
	}
	bindings, err := sortBindingBytes(keyCount)
	if err != nil {
		return p, err
	}
	// Exact value/bitmap/offset/data capacities, plus both row-index and stable
	// ordinal arrays. The displaced runtime ordinal descriptor remains counted.
	var rowArrays int64
	if err := addJoinGatherProduct(&rowArrays, uint64(selection.len()), uint64(unsafe.Sizeof(int(0)))+8); err != nil {
		return p, err
	}
	actual, err := sortWorkspaceSize(owned, metadata, rowArrays, bindings)
	if err != nil {
		return p, err
	}
	rangeBytes, err := memory.EstimateBatchRangeBytes(batch, start, selection.len())
	if err != nil {
		return p, err
	}
	// Preserve the external sort's original lower bound; never inherit join's
	// 2*full-source floor, which would make tiny chunks depend on the whole input.
	legacy, err := externalSortInitialGatherLegacyBytes(rangeBytes, selection.len(), bindings)
	if err != nil {
		return p, err
	}
	encoded, err := memory.EstimateColumnarRunRangeBytes(schema, batch, start, selection.len())
	if err != nil {
		return p, err
	}
	synthesized, err := externalSortInitialGatherBitmapBytes(batch, selection)
	if err != nil {
		return p, err
	}
	var ordinals int64
	if err := addJoinGatherProduct(&ordinals, uint64(selection.len()), 8); err != nil {
		return p, err
	}
	encoded, err = sortWorkspaceSize(encoded, synthesized, ordinals, 4)
	if err != nil {
		return p, err
	}
	p = externalSortInitialGatherPlan{gatherBytes: max(actual, legacy), encodeBytes: encoded, rangeBytes: rangeBytes,
		capacityBytes: owned + rowArrays, metadataBytes: metadata + bindings, start: start, end: end, source: batch, keyCount: keyCount}
	return p, nil
}

func externalSortInitialGatherLegacyBytes(payload int64, rows int, bindings int64) (int64, error) {
	if payload < 0 || rows < 0 || bindings < 0 {
		return 0, memory.ErrBudgetExceeded
	}
	var total int64
	if err := addJoinGatherProduct(&total, uint64(payload), 2); err != nil {
		return 0, err
	}
	if err := addJoinGatherProduct(&total, uint64(rows), 16); err != nil {
		return 0, err
	}
	return sortWorkspaceSize(total, 1024, bindings)
}

// Match the decoder/accumulator's conservative metadata accounting contract:
// 128 per nonempty hinted map, 512 per binding, separately rounded descriptors,
// and each Batch's header/temporary allowance. This is not a Go heap/RSS claim.
const externalSortInitialGatherBatchBytes = 2*int64(unsafe.Sizeof(memory.Batch{})) + 256

func externalSortInitialGatherRounded(n int64) (int64, error) {
	n, err := sortWorkspaceSize(n, 15)
	return n &^ 15, err
}

func externalSortInitialGatherCounts(b *memory.Batch) [15]int {
	return [15]int{len(b.Int8s), len(b.Int16s), len(b.Int32s), len(b.Int64s), len(b.Uint8s), len(b.Uint16s), len(b.Uint32s), len(b.Uint64s), len(b.Float32s), len(b.Float64s), len(b.Decimals), len(b.Strings), len(b.DictStrings), len(b.StringLists), len(b.Structs)}
}

func externalSortInitialGatherChildCounts(v *memory.StructVector, rows int) [15]int {
	var counts [15]int
	if rows == 0 {
		return counts
	}
	for _, field := range v.Fields {
		kind := externalSortAccumulatorKind(field)
		if field.Type == memory.TypeString {
			kind = 11
			if v.Values.Strings[field.Name] == nil {
				kind = 12
			}
		}
		counts[kind]++
	}
	return counts
}

func externalSortInitialGatherMapMetadata(counts [15]int) (int64, error) {
	sizes := [15]uintptr{unsafe.Sizeof(memory.Int8Vector{}), unsafe.Sizeof(memory.Int16Vector{}), unsafe.Sizeof(memory.Int32Vector{}), unsafe.Sizeof(memory.Int64Vector{}), unsafe.Sizeof(memory.Uint8Vector{}), unsafe.Sizeof(memory.Uint16Vector{}), unsafe.Sizeof(memory.Uint32Vector{}), unsafe.Sizeof(memory.Uint64Vector{}), unsafe.Sizeof(memory.Float32Vector{}), unsafe.Sizeof(memory.Float64Vector{}), unsafe.Sizeof(memory.Decimal128Vector{}), unsafe.Sizeof(memory.StringViewVector{}), unsafe.Sizeof(memory.DictionaryStringVector{}), unsafe.Sizeof(memory.StringListVector{}), unsafe.Sizeof(memory.StructVector{})}
	total := externalSortInitialGatherBatchBytes
	for kind, count := range counts {
		if count == 0 {
			continue
		}
		descriptor, err := externalSortInitialGatherRounded(int64(sizes[kind]))
		if err != nil {
			return 0, err
		}
		if kind == 13 {
			child, err := externalSortInitialGatherRounded(int64(unsafe.Sizeof(memory.StringViewVector{})))
			if err != nil {
				return 0, err
			}
			descriptor += child
		}
		if err := addJoinEstimate(&total, 128); err != nil {
			return 0, err
		}
		if err := addJoinGatherProduct(&total, uint64(count), uint64(512+descriptor)); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func externalSortInitialGatherMetadata(batch *memory.Batch, rows int) (int64, error) {
	counts := externalSortInitialGatherCounts(batch)
	if counts[7] == math.MaxInt {
		return 0, memory.ErrBudgetExceeded
	}
	counts[7]++ // Ordinal descriptor and binding, even if it replaces an extra.
	total, err := externalSortInitialGatherMapMetadata(counts)
	if err != nil {
		return 0, err
	}
	for _, v := range batch.Structs {
		child, err := externalSortInitialGatherMapMetadata(externalSortInitialGatherChildCounts(v, rows))
		if err != nil {
			return 0, err
		}
		var fields int64
		if err := addJoinGatherProduct(&fields, uint64(len(v.Fields)), uint64(unsafe.Sizeof(memory.Field{}))); err != nil {
			return 0, err
		}
		fields, err = externalSortInitialGatherRounded(fields)
		if err != nil {
			return 0, err
		}
		total, err = sortWorkspaceSize(total, child, fields)
		if err != nil {
			return 0, err
		}
	}
	owner, err := externalSortInitialGatherRounded(int64(unsafe.Sizeof(externalSortInitialGather{})))
	if err != nil {
		return 0, err
	}
	token, err := externalSortInitialGatherRounded(int64(unsafe.Sizeof(sortWorkspaceReservation{})))
	if err != nil {
		return 0, err
	}
	return sortWorkspaceSize(total, owner, token)
}

// Range estimation preserves every runtime payload binding, including extras.
// Add only a safe bound for the full all-valid bitmaps the bounded nested copier
// synthesizes. Never subtract runtime payload from a schema-only estimate.
func externalSortInitialGatherBitmapBytes(batch *memory.Batch, rows joinGatherSelection) (int64, error) {
	var total int64
	add := func(validity memory.Bitmap, n int) error {
		if validity != nil {
			return nil
		}
		return addJoinGatherProduct(&total, uint64(joinGatherWords(n)), 8)
	}
	list := func(v *memory.StringListVector, parent *memory.StructVector) error {
		elements, _, err := joinGatherListSizeSelected(v, rows, parent)
		if err != nil {
			return err
		}
		if err := add(v.Validity, rows.len()); err != nil {
			return err
		}
		return add(v.Values.Validity, elements)
	}
	for _, v := range batch.StringLists {
		if err := list(v, nil); err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Structs {
		if err := add(v.Validity, rows.len()); err != nil {
			return 0, err
		}
		for _, f := range v.Fields {
			var validity memory.Bitmap
			switch f.Type {
			case memory.TypeInt8:
				validity = v.Values.Int8s[f.Name].Validity
			case memory.TypeInt16:
				validity = v.Values.Int16s[f.Name].Validity
			case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
				validity = v.Values.Int32s[f.Name].Validity
			case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
				validity = v.Values.Int64s[f.Name].Validity
			case memory.TypeUint8:
				validity = v.Values.Uint8s[f.Name].Validity
			case memory.TypeUint16:
				validity = v.Values.Uint16s[f.Name].Validity
			case memory.TypeUint32, memory.TypeBool:
				validity = v.Values.Uint32s[f.Name].Validity
			case memory.TypeUint64:
				validity = v.Values.Uint64s[f.Name].Validity
			case memory.TypeFloat32:
				validity = v.Values.Float32s[f.Name].Validity
			case memory.TypeFloat64:
				validity = v.Values.Float64s[f.Name].Validity
			case memory.TypeString:
				if str := v.Values.Strings[f.Name]; str != nil {
					validity = str.Validity
				} else {
					validity = v.Values.DictStrings[f.Name].Validity
				}
			case memory.TypeList:
				if err := list(v.Values.StringLists[f.Name], v); err != nil {
					return 0, err
				}
				continue
			default:
				return 0, fmt.Errorf("unsupported external sort initial struct child")
			}
			if err := add(validity, rows.len()); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

// plan must describe this unchanged, validated source and the same schema/keys.
// All allocations after admission are attached to this owner before any fallible
// fill step. An error destroys the partial graph before returning its token.
func newExternalSortInitialGather(ctx context.Context, manager memory.Manager, schema memory.Schema, src *memory.Batch, keys []SortKey, ordinal string, ordinalBase uint64, plan externalSortInitialGatherPlan) (_ *externalSortInitialGather, err error) {
	if ctx == nil || manager == nil {
		return nil, fmt.Errorf("external sort initial gather requires context and manager")
	}
	if err := externalSortInitialGatherRange(src, plan.start, plan.end, len(keys), ordinalBase); err != nil {
		return nil, err
	}
	if plan.source != src || plan.keyCount != len(keys) || plan.gatherBytes <= 0 {
		return nil, fmt.Errorf("external sort initial gather requires a matching validated plan")
	}
	if _, exists := schema.Field(ordinal); exists {
		return nil, fmt.Errorf("external sort ordinal %q collides with input schema", ordinal)
	}
	reservation, err := manager.Reserve(ctx, plan.gatherBytes)
	if err != nil {
		return nil, err
	}
	owner := &externalSortInitialGather{reservation: reservation}
	defer func() {
		if err != nil {
			owner.close()
		}
	}()
	rows := plan.end - plan.start
	owner.indices = make([]int, rows)
	for i := range owner.indices {
		owner.indices[i] = plan.start + i
	}
	// Do not use newBatchComparator: its partial binding array is local on error.
	if src.Rows >= 2 {
		owner.comparator.keys = make([]boundSortKey, len(keys))
		for i, key := range keys {
			owner.comparator.keys[i], err = bindSortKey(src, schema, key)
			if err != nil {
				return nil, err
			}
		}
	}
	sort.Stable(owner)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	counts := externalSortInitialGatherCounts(src)
	counts[7]++
	owner.batch = newExternalSortInitialGatherBatch(counts)
	if err = takeExternalSortInitialRows(ctx, owner.batch, src, owner.indices); err != nil {
		return nil, err
	}
	owner.displacedOrdinal = owner.batch.Uint64s[ordinal]
	stable := &memory.Uint64Vector{}
	owner.batch.Uint64s[ordinal] = stable
	stable.Values = make([]uint64, rows)
	for i, row := range owner.indices {
		stable.Values[i] = ordinalBase + uint64(row)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return owner, nil
}

// Len implements sort.Interface.
func (a *externalSortInitialGather) Len() int { return len(a.indices) }

// Swap implements sort.Interface.
func (a *externalSortInitialGather) Swap(i, j int) {
	a.indices[i], a.indices[j] = a.indices[j], a.indices[i]
}

// Less implements sort.Interface.
func (a *externalSortInitialGather) Less(i, j int) bool {
	return a.comparator.less(a.indices[i], a.indices[j])
}

func (a *externalSortInitialGather) close() {
	if a == nil {
		return
	}
	destroyExternalSortAccumulatorBatch(a.batch)
	if a.displacedOrdinal != nil {
		*a.displacedOrdinal = memory.Uint64Vector{}
	}
	clear(a.indices)
	clear(a.comparator.keys)
	reservation := a.reservation
	*a = externalSortInitialGather{}
	if reservation != nil {
		reservation.Release()
	}
}

func newExternalSortInitialGatherBatch(counts [15]int) *memory.Batch {
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
	return b
}

func takeExternalSortInitialRows(ctx context.Context, dst, src *memory.Batch, rows []int) error {
	n := len(rows)
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Int8s {
		out := &memory.Int8Vector{Values: make([]int8, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Int8s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Int16s {
		out := &memory.Int16Vector{Values: make([]int16, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Int16s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Int32s {
		out := &memory.Int32Vector{Values: make([]int32, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Int32s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Int64s {
		out := &memory.Int64Vector{Values: make([]int64, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Int64s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Uint8s {
		out := &memory.Uint8Vector{Values: make([]uint8, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Uint8s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Uint16s {
		out := &memory.Uint16Vector{Values: make([]uint16, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Uint16s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Uint32s {
		out := &memory.Uint32Vector{Values: make([]uint32, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Uint32s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Uint64s {
		out := &memory.Uint64Vector{Values: make([]uint64, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Uint64s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Float32s {
		out := &memory.Float32Vector{Values: make([]float32, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Float32s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Float64s {
		out := &memory.Float64Vector{Values: make([]float64, n), Validity: compactValidity(nil, v.Validity, rows), Encoding: v.Encoding}
		dst.Float64s[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Decimals {
		out := &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: compactValidity(nil, v.Validity, rows), Precision: v.Precision, Scale: v.Scale}
		dst.Decimals[name] = out
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.DictStrings {
		out := &memory.DictionaryStringVector{Indices: make([]uint32, n), Validity: compactValidity(nil, v.Validity, rows), Offsets: v.Offsets, Data: v.Data, DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
		dst.DictStrings[name] = out
		for i, row := range rows {
			out.Indices[i] = v.Indices[row]
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Strings {
		bytes, err := joinGatherStringSize(v, rows, nil)
		if err != nil {
			return err
		}
		out := &memory.StringViewVector{Offsets: make([]uint32, n+1), Data: make([]byte, 0, bytes), Validity: compactValidity(nil, v.Validity, rows), Encoding: memory.EncodingView}
		dst.Strings[name] = out
		for i, row := range rows {
			out.Data = append(out.Data, v.ValueBytes(row)...)
			out.Offsets[i+1] = uint32(len(out.Data))
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.StringLists {
		elements, bytes, err := joinGatherListSize(v, rows, nil)
		if err != nil {
			return err
		}
		out := plannedJoinList(n, elements, bytes)
		dst.StringLists[name] = out
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := memory.AppendStringListRow(out, v, row); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, v := range src.Structs {
		out := &memory.StructVector{}
		dst.Structs[name] = out
		if err := fillExternalSortInitialStruct(ctx, out, v, rows); err != nil {
			return err
		}
	}
	dst.Reset(n)
	return nil
}

func fillExternalSortInitialStruct(ctx context.Context, out, src *memory.StructVector, rows []int) error {
	n := len(rows)
	*out = memory.StructVector{Fields: make([]memory.Field, len(src.Fields)), Values: newExternalSortInitialGatherBatch(externalSortInitialGatherChildCounts(src, n)), Encoding: memory.EncodingView}
	copy(out.Fields, src.Fields)
	if n == 0 {
		return nil
	}
	out.Validity = make(memory.Bitmap, joinGatherWords(n))
	for _, field := range src.Fields {
		if err := ctx.Err(); err != nil {
			return err
		}
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
					return err
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
			elements, bytes, err := joinGatherListSize(src.Values.StringLists[field.Name], rows, src)
			if err != nil {
				return err
			}
			out.Values.StringLists[field.Name] = plannedJoinList(n, elements, bytes)
		default:
			return fmt.Errorf("unsupported bounded join gather struct child")
		}
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := memory.AppendStructRow(out, src, row, src.IsValid(row)); err != nil {
			return err
		}
	}
	return nil
}
