package physical

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"unsafe"

	"gocolumn/memory"
)

// residentSortAccumulator owns selected typed values, not source dictionary
// tables. Its table index and every capacity grow under the same pre-admitted
// owner as the data. No Stats, domain identity, or spare input capacity is an
// allocation plan. Opaque Arrow rows have a separately charged sidecar and are
// emitted individually instead of concatenating arbitrary Arrow types.
type residentSortAccumulator struct {
	*externalSortAccumulator
	workspace *sortWorkspace
	metadata  memory.Reservation
	opaque    *residentOpaqueRows
	view      *memory.Batch
	viewLease memory.Reservation
	borrowed  *memory.Batch
}

type residentDictionaryIndex struct {
	vector  *memory.DictionaryStringVector
	hashes  []uint64
	codes   []uint32
	entries int
}

func newResidentSortAccumulator(ctx context.Context, schema memory.Schema, workspace *sortWorkspace, first *memory.Batch) (_ *residentSortAccumulator, err error) {
	if first == nil || workspace == nil {
		return nil, fmt.Errorf("resident accumulator requires input and workspace")
	}
	// Plan from runtime storage as well as logical fields. Struct string children
	// can legitimately use DictStrings while their public schema remains View.
	metadata, dictionaries, err := residentAccumulatorMetadata(schema.Fields, first, 0)
	if err != nil {
		return nil, err
	}
	metadata, err = sortWorkspaceSize(metadata, 1024, int64(dictionaries)*int64(unsafe.Sizeof(residentDictionaryIndex{})))
	if err != nil {
		return nil, err
	}
	lease, err := workspace.Reserve(ctx, metadata)
	if err != nil {
		return nil, err
	}
	a := &residentSortAccumulator{workspace: workspace, externalSortAccumulator: &externalSortAccumulator{reservation: lease.(*sortWorkspaceReservation), metadataBytes: metadata}}
	defer func() {
		if err != nil {
			a.close()
		}
	}()
	fields := residentAccumulatorFields(schema.Fields, first, 0)
	a.schema = memory.Schema{Fields: fields}
	a.batch = newExternalSortAccumulatorBatch(fields)
	a.residentDictionaries = make([]residentDictionaryIndex, 0, dictionaries)
	a.registerResidentDictionaries(a.batch)
	a.opaque, err = newResidentOpaqueRows(ctx, schema, first, workspace)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func residentDictionaryHash(value []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range value {
		h ^= uint64(b)
		h *= 1099511628211
	}
	// Zero denotes an empty slot. Equal hashes are always checked by bytes.
	if h == 0 {
		h = 1
	}
	return h
}

func (a *externalSortAccumulator) appendResidentDictionary(ctx context.Context, dst, src *memory.DictionaryStringVector, row int, parentValid bool) error {
	if dst == nil || src == nil || row < 0 || row >= len(src.Indices) {
		return fmt.Errorf("resident dictionary row missing or out of range")
	}
	valid, err := externalSortValidity(src.Validity, row)
	if err != nil {
		return err
	}
	valid = valid && parentValid
	if err = a.initOffsets(ctx, &dst.Offsets); err != nil {
		return err
	}
	code := uint32(0)
	if valid {
		sourceCode := src.Indices[row]
		if uint64(sourceCode)+1 >= uint64(len(src.Offsets)) {
			return fmt.Errorf("resident dictionary code out of range")
		}
		lo, hi := src.Offsets[sourceCode], src.Offsets[sourceCode+1]
		if lo > hi || uint64(hi) > uint64(len(src.Data)) {
			return fmt.Errorf("resident dictionary offsets out of range")
		}
		value := src.Data[lo:hi]
		var index *residentDictionaryIndex
		for i := range a.residentDictionaries {
			if a.residentDictionaries[i].vector == dst {
				index = &a.residentDictionaries[i]
				break
			}
		}
		if index == nil {
			return fmt.Errorf("resident dictionary index missing")
		}
		hash := residentDictionaryHash(value)
		found := false
		if len(index.hashes) > 0 {
			slot := int(hash & uint64(len(index.hashes)-1))
			for index.hashes[slot] != 0 {
				if index.hashes[slot] == hash && bytes.Equal(dst.DictionaryValue(index.codes[slot]), value) {
					code = index.codes[slot]
					found = true
					break
				}
				slot = (slot + 1) & (len(index.hashes) - 1)
			}
		}
		if !found {
			if !sortDictionaryCanAppend(uint64(len(dst.Offsets)), uint64(len(dst.Data)), uint64(len(value))) {
				return memory.ErrBudgetExceeded
			}
			if err = a.growResidentDictionaryIndex(ctx, index); err != nil {
				return err
			}
			code = uint32(len(dst.Offsets) - 1)
			if err = a.appendBytes(ctx, &dst.Data, value); err != nil {
				return err
			}
			if err = externalSortAppendValue(ctx, a, &dst.Offsets, uint32(len(dst.Data))); err != nil {
				return err
			}
			slot := int(hash & uint64(len(index.hashes)-1))
			for index.hashes[slot] != 0 {
				slot = (slot + 1) & (len(index.hashes) - 1)
			}
			index.hashes[slot], index.codes[slot] = hash, code
			index.entries++
		}
	}
	if err = a.appendValidity(ctx, &dst.Validity, valid, src.Validity != nil, len(dst.Indices)); err != nil {
		return err
	}
	return externalSortAppendValue(ctx, a, &dst.Indices, code)
}

func (a *externalSortAccumulator) growResidentDictionaryIndex(ctx context.Context, index *residentDictionaryIndex) error {
	if index.entries < len(index.hashes)/2 {
		return nil
	}
	n := max(8, len(index.hashes)*2)
	if n < len(index.hashes) || uint64(n) > math.MaxUint32 {
		return memory.ErrBudgetExceeded
	}
	size, err := externalSortBufferBytes(n, 12)
	if err != nil {
		return err
	}
	if err = a.ensure(ctx, size); err != nil {
		return err
	}
	hashes, codes := make([]uint64, n), make([]uint32, n)
	for i, h := range index.hashes {
		if h == 0 {
			continue
		}
		slot := int(h & uint64(n-1))
		for hashes[slot] != 0 {
			slot = (slot + 1) & (n - 1)
		}
		hashes[slot], codes[slot] = h, index.codes[i]
	}
	a.capacityBytes += size - int64(len(index.hashes))*12
	index.hashes, index.codes = hashes, codes
	return nil
}

func (a *residentSortAccumulator) appendRow(ctx context.Context, src *memory.Batch, row int) error {
	if err := a.externalSortAccumulator.appendRow(ctx, src, row); err != nil {
		return err
	}
	if a.opaque != nil {
		if err := a.opaque.appendRow(ctx, src, row); err != nil {
			a.failed = true
			return err
		}
	}
	return nil
}

func (a *residentSortAccumulator) appendFrom(ctx context.Context, src *residentSortAccumulator, row int) error {
	if err := a.externalSortAccumulator.appendRow(ctx, src.batch, row); err != nil {
		return err
	}
	if a.opaque != nil {
		if err := a.opaque.appendFrom(ctx, src.opaque, row); err != nil {
			a.failed = true
			return err
		}
	}
	return nil
}

func (a *residentSortAccumulator) hasOpaque() bool { return a != nil && a.opaque != nil }

func (a *residentSortAccumulator) bindOutput(ctx context.Context, dst *memory.Batch, row int) error {
	a.clearOutput()
	if dst == nil {
		return fmt.Errorf("resident sort destination is nil")
	}
	count := a.batch.Rows
	offset := 0
	if row >= 0 {
		offset, count = row, 1
	}
	bytes, err := memory.BorrowedRangeMetadataBytes(a.batch, offset, count)
	if err != nil {
		return err
	}
	// BorrowBatchRange creates all batch maps and fresh descriptors in addition
	// to its precisely sized rebased offsets and misaligned validity buffers.
	headers, err := topKSnapshotHeaders(a.batch)
	if err != nil {
		return err
	}
	total, err := sortWorkspaceSize(bytes, headers)
	if err != nil {
		return err
	}
	a.viewLease, err = a.workspace.Reserve(ctx, total)
	if err != nil {
		return err
	}
	a.view, err = memory.BorrowBatchRange(a.batch, offset, count)
	if err != nil {
		a.clearOutput()
		return err
	}
	if a.opaque != nil {
		if row < 0 {
			a.clearOutput()
			return fmt.Errorf("opaque resident output requires one row")
		}
		if err = a.opaque.bindRow(a.view, row); err != nil {
			a.clearOutput()
			return err
		}
	}
	dst.ClearColumns()
	bindAll(dst, a.view)
	dst.Reset(count)
	a.borrowed = dst
	return nil
}

func (a *residentSortAccumulator) clearOutput() {
	if a == nil {
		return
	}
	if a.borrowed != nil {
		a.borrowed.ClearColumns()
		a.borrowed = nil
	}
	if a.view != nil {
		destroyResidentBorrowedBatch(a.view)
		a.view = nil
	}
	if a.viewLease != nil {
		a.viewLease.Release()
		a.viewLease = nil
	}
}

func (a *residentSortAccumulator) close() {
	if a == nil {
		return
	}
	a.clearOutput()
	if a.opaque != nil {
		a.opaque.close()
		a.opaque = nil
	}
	if a.externalSortAccumulator != nil {
		a.externalSortAccumulator.close()
		a.externalSortAccumulator = nil
	}
	lease := a.metadata
	*a = residentSortAccumulator{}
	if lease != nil {
		lease.Release()
	}
}

// Borrowed structs share immutable Fields with their owner, so do not clear
// those arrays when invalidating output descriptors. Drop Arrow references
// before zeroing the independent copied wrappers.
func destroyResidentBorrowedBatch(b *memory.Batch) {
	if b == nil {
		return
	}
	for _, v := range b.Structs {
		if v != nil {
			destroyResidentBorrowedBatch(v.Values)
			*v = memory.StructVector{}
		}
	}
	clear(b.Structs)
	b.ReleaseOwned()
	clearExternalSortAccumulatorVectors(b.Opaque)
	destroyExternalSortAccumulatorBatch(b)
}

// newResidentSortedAccumulator gathers a permutation of an already owned typed
// batch. Its capacity plan is computed before allocating; dictionary tables are
// transferred under overlapping reservations rather than reinterned row by row.
func newResidentSortedAccumulator(ctx context.Context, src *residentSortAccumulator, rows []int) (_ *residentSortAccumulator, err error) {
	if src == nil || src.hasOpaque() {
		return nil, fmt.Errorf("typed resident gather requires a typed owner")
	}
	selection := joinGatherSelection{rows: rows}
	for _, row := range rows {
		if row < 0 || row >= src.batch.Rows {
			return nil, fmt.Errorf("resident gather row out of range")
		}
	}
	payload, _, err := planJoinGatherSelected(src.batch, selection)
	if err != nil {
		return nil, err
	}
	metadata, err := externalSortInitialGatherMetadata(src.batch, len(rows))
	if err != nil {
		return nil, err
	}
	metadata, err = sortWorkspaceSize(metadata, int64(unsafe.Sizeof(residentSortAccumulator{})), int64(unsafe.Sizeof(externalSortAccumulator{})), residentSchemaArrayBytes(src.schema.Fields))
	if err != nil {
		return nil, err
	}
	// Top-level gather borrows the tables. Admit their actual retained capacities
	// before the source owner can end; no source dictionary or identity escapes.
	var tables int64
	for _, v := range src.batch.DictStrings {
		tables, err = sortWorkspaceSize(tables, int64(cap(v.Data)), int64(cap(v.Offsets))*4, int64(len(v.DictionaryDomain)))
		if err != nil {
			return nil, err
		}
	}
	for _, v := range src.batch.Structs {
		for _, d := range v.Values.DictStrings {
			tables, err = sortWorkspaceSize(tables, int64(len(d.DictionaryDomain)))
			if err != nil {
				return nil, err
			}
		}
	}
	total, err := sortWorkspaceSize(payload, metadata, tables)
	if err != nil {
		return nil, err
	}
	reservation, err := src.workspace.Reserve(ctx, total)
	if err != nil {
		return nil, err
	}
	out := &residentSortAccumulator{workspace: src.workspace, externalSortAccumulator: &externalSortAccumulator{reservation: reservation.(*sortWorkspaceReservation), metadataBytes: metadata, capacityBytes: payload + tables, finished: true}}
	defer func() {
		if err != nil {
			out.close()
		}
	}()
	out.schema = memory.Schema{Fields: append([]memory.Field(nil), src.schema.Fields...)}
	out.batch = newExternalSortInitialGatherBatch(externalSortInitialGatherCounts(src.batch))
	if err = takeExternalSortInitialRows(ctx, out.batch, src.batch, rows); err != nil {
		return nil, err
	}
	return out, nil
}

func residentAccumulatorMetadata(fields []memory.Field, src *memory.Batch, depth int) (int64, int, error) {
	if src == nil || depth > 1 {
		return 0, 0, fmt.Errorf("resident struct nesting is unsupported")
	}
	counts := [15]int{}
	var extra int64
	dictionaries := 0
	for i, f := range fields {
		for j := 0; j < i; j++ {
			if fields[j].Name == f.Name {
				return 0, 0, fmt.Errorf("duplicate resident field %q", f.Name)
			}
		}
		if depth == 0 && residentUsesOpaque(f, src) {
			continue
		}
		switch f.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration, memory.TypeDecimal, memory.TypeString, memory.TypeBinary, memory.TypeList, memory.TypeStruct:
		default:
			return 0, 0, fmt.Errorf("unsupported resident field %q", f.Name)
		}
		if (f.Type == memory.TypeString || f.Type == memory.TypeBinary) && src.Strings[f.Name] == nil && src.DictStrings[f.Name] != nil {
			f.Encoding = memory.EncodingDictionary
		}
		counts[externalSortAccumulatorKind(f)]++
		if f.Encoding == memory.EncodingDictionary && (f.Type == memory.TypeString || f.Type == memory.TypeBinary) {
			dictionaries++
		}
		var n int64
		var err error
		switch f.Type {
		case memory.TypeStruct:
			v := src.Structs[f.Name]
			if v == nil || v.Values == nil {
				return 0, 0, fmt.Errorf("resident struct %q missing", f.Name)
			}
			var childDictionaries int
			n, childDictionaries, err = residentAccumulatorMetadata(v.Fields, v.Values, depth+1)
			dictionaries += childDictionaries
			if err == nil {
				n, err = sortWorkspaceSize(n, int64(len(v.Fields))*int64(unsafe.Sizeof(memory.Field{})))
			}
		case memory.TypeList:
			err = f.ValidateListString()
			n = int64(unsafe.Sizeof(memory.StringViewVector{}))
		}
		if err != nil {
			return 0, 0, err
		}
		extra, err = sortWorkspaceSize(extra, n)
		if err != nil {
			return 0, 0, err
		}
	}
	metadata, err := externalSortInitialGatherMapMetadata(counts)
	if err != nil {
		return 0, 0, err
	}
	metadata, err = sortWorkspaceSize(metadata, extra, int64(len(fields))*int64(unsafe.Sizeof(memory.Field{})))
	return metadata, dictionaries, err
}

func residentAccumulatorFields(fields []memory.Field, src *memory.Batch, depth int) []memory.Field {
	out := make([]memory.Field, 0, len(fields))
	for _, f := range fields {
		if depth == 0 && residentUsesOpaque(f, src) {
			continue
		}
		if (f.Type == memory.TypeString || f.Type == memory.TypeBinary) && src.Strings[f.Name] == nil && src.DictStrings[f.Name] != nil {
			f.Encoding = memory.EncodingDictionary
		}
		if f.Type == memory.TypeStruct {
			v := src.Structs[f.Name]
			f.Children = residentAccumulatorFields(v.Fields, v.Values, depth+1)
		}
		out = append(out, f)
	}
	return out
}

func (a *externalSortAccumulator) registerResidentDictionaries(b *memory.Batch) {
	for _, v := range b.DictStrings {
		a.residentDictionaries = append(a.residentDictionaries, residentDictionaryIndex{vector: v})
	}
	for _, v := range b.Structs {
		a.registerResidentDictionaries(v.Values)
	}
}

func residentSchemaArrayBytes(fields []memory.Field) int64 {
	n := int64(len(fields)) * int64(unsafe.Sizeof(memory.Field{}))
	for _, f := range fields {
		if f.Type == memory.TypeStruct {
			n += residentSchemaArrayBytes(f.Children)
		}
	}
	return n
}
