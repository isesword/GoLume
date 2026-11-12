package physical

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
)

// compactWorkspace accounts backing buffers, following BatchBytes' convention
// of excluding Go map/vector headers. Immutable dictionary payloads and struct
// schemas remain borrowed from the input, never released by this workspace.
// Any growth replaces the complete output, keeping the old token live until
// allocation and compaction succeed. Reuse keeps its original capacity charge.
type compactWorkspace struct {
	batch   *memory.Batch
	lease   memory.Reservation
	manager memory.Manager
	bounded bool
}

func (w *compactWorkspace) compact(ctx context.Context, src *memory.Batch, rows []int, selection memory.Bitmap, kernels kernel.Set) (*memory.Batch, error) {
	bytes, fits, err := compactLayout(src, w.batch, rows, false, w.bounded)
	if err != nil {
		return nil, err
	}
	if fits {
		if err := compactSelected(ctx, w.batch, src, rows, selection, kernels); err != nil {
			return nil, err
		}
		return w.batch, nil
	}
	lease, err := w.manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, fmt.Errorf("reserve filter compaction: %w", err)
	}
	next := memory.NewBatch()
	if _, _, err := compactLayout(src, next, rows, true, w.bounded); err != nil {
		next.ClearColumns()
		lease.Release()
		return nil, err
	}
	if err := compactSelected(ctx, next, src, rows, selection, kernels); err != nil {
		next.ClearColumns()
		lease.Release()
		return nil, err
	}
	previous, previousLease := w.batch, w.lease
	w.batch, w.lease = next, lease
	if previous != nil {
		previous.ClearColumns()
	}
	if previousLease != nil {
		previousLease.Release()
	}
	return w.batch, nil
}

func (w *compactWorkspace) release() {
	if w.batch != nil {
		w.batch.ClearColumns()
	}
	if w.lease != nil {
		w.lease.Release()
	}
	w.batch, w.lease, w.manager = nil, nil, nil
}

func compactOpaqueError() error {
	return &memory.ResourceError{Scope: "filter", Operator: "Filter", Class: memory.MemoryWorkspace, Hint: "project out opaque columns, use native typed vectors, or explicitly use an unbounded direct stream", Cause: fmt.Errorf("discontiguous opaque row compaction has no bounded allocation contract: %w", memory.ErrBudgetExceeded)}
}

func ensureBytes(v []byte, n int) []byte {
	if cap(v) < n {
		return make([]byte, n)
	}
	return v[:n]
}

func compactStringSize(v *memory.StringViewVector, rows []int, parents *compactParent) (int, error) {
	if v == nil {
		return 0, fmt.Errorf("nil compact string")
	}
	var bytes uint64
	for _, row := range rows {
		if row < 0 || row+1 >= len(v.Offsets) || v.Validity != nil && row>>6 >= len(v.Validity) {
			return 0, fmt.Errorf("compact string row out of range")
		}
		lo, hi := v.Offsets[row], v.Offsets[row+1]
		if lo > hi || uint64(hi) > uint64(len(v.Data)) {
			return 0, fmt.Errorf("invalid compact string offsets")
		}
		if rowValid(v.Validity, row) && parents.valid(row) {
			bytes += uint64(hi - lo)
		}
		if bytes > math.MaxUint32 || bytes > uint64(int(^uint(0)>>1)) {
			return 0, fmt.Errorf("compact string payload exceeds offset range")
		}
	}
	return int(bytes), nil
}
func compactListSize(v *memory.StringListVector, rows []int, parents *compactParent) (int, int, error) {
	if v == nil || v.Values == nil {
		return 0, 0, fmt.Errorf("nil compact list")
	}
	var children, bytes uint64
	for _, row := range rows {
		if row < 0 || row+1 >= len(v.Offsets) || v.Validity != nil && row>>6 >= len(v.Validity) {
			return 0, 0, fmt.Errorf("compact list row out of range")
		}
		lo, hi := int(v.Offsets[row]), int(v.Offsets[row+1])
		if lo > hi || hi >= len(v.Values.Offsets) {
			return 0, 0, fmt.Errorf("invalid compact list offsets")
		}
		if !rowValid(v.Validity, row) || !parents.valid(row) {
			continue
		}
		children += uint64(hi - lo)
		for child := lo; child < hi; child++ {
			if v.Values.Validity != nil && child>>6 >= len(v.Values.Validity) {
				return 0, 0, fmt.Errorf("short compact list child validity")
			}
			start, end := v.Values.Offsets[child], v.Values.Offsets[child+1]
			if start > end || uint64(end) > uint64(len(v.Values.Data)) {
				return 0, 0, fmt.Errorf("invalid compact list string offsets")
			}
			if rowValid(v.Values.Validity, child) {
				bytes += uint64(end - start)
			}
		}
		if children > math.MaxUint32 || bytes > math.MaxUint32 || children > uint64(int(^uint(0)>>1)-1) || bytes > uint64(int(^uint(0)>>1)) {
			return 0, 0, fmt.Errorf("compact list exceeds offset range")
		}
	}
	return int(children), int(bytes), nil
}

// compactLayout is an allocation-free preflight unless allocate is true.
// It validates shape and totals the exact buffers of a fresh output. Allocate
// must only be called after that complete total has been reserved.
func compactLayout(src, dst *memory.Batch, rows []int, allocate, bounded bool) (int64, bool, error) {
	return compactLayoutParents(src, dst, rows, allocate, bounded, nil)
}

func compactLayoutParents(src, dst *memory.Batch, rows []int, allocate, bounded bool, parents *compactParent) (int64, bool, error) {
	n := len(rows)
	if src == nil {
		return 0, false, fmt.Errorf("nil compact input")
	}
	for _, row := range rows {
		if row < 0 || row >= src.Rows {
			return 0, false, fmt.Errorf("compact row out of range")
		}
	}
	fits := dst != nil
	if dst == nil {
		dst = &memory.Batch{}
	}
	if len(src.Int8s) != len(dst.Int8s) {
		fits = false
	}
	if len(src.Int16s) != len(dst.Int16s) {
		fits = false
	}
	if len(src.Int32s) != len(dst.Int32s) {
		fits = false
	}
	if len(src.Int64s) != len(dst.Int64s) {
		fits = false
	}
	if len(src.Uint8s) != len(dst.Uint8s) {
		fits = false
	}
	if len(src.Uint16s) != len(dst.Uint16s) {
		fits = false
	}
	if len(src.Uint32s) != len(dst.Uint32s) {
		fits = false
	}
	if len(src.Uint64s) != len(dst.Uint64s) {
		fits = false
	}
	if len(src.Float32s) != len(dst.Float32s) {
		fits = false
	}
	if len(src.Decimals) != len(dst.Decimals) {
		fits = false
	}
	if len(src.Float64s) != len(dst.Float64s) {
		fits = false
	}
	if len(src.Strings) != len(dst.Strings) {
		fits = false
	}
	if len(src.DictStrings) != len(dst.DictStrings) {
		fits = false
	}
	if len(src.Int64Lists) != len(dst.Int64Lists) {
		fits = false
	}
	if len(src.StringLists) != len(dst.StringLists) {
		fits = false
	}
	if len(src.Structs) != len(dst.Structs) {
		fits = false
	}
	if len(src.Opaque) != len(dst.Opaque) {
		fits = false
	}
	var total int64
	add := func(n int64) error {
		if n < 0 || total > math.MaxInt64-n {
			return fmt.Errorf("compact size overflow")
		}
		total += n
		return nil
	}
	inheritedNulls := false
	if parents != nil {
		for _, row := range rows {
			if !parents.valid(row) {
				inheritedNulls = true
				break
			}
		}
	}
	validity := func(src memory.Bitmap, dst memory.Bitmap, count int, inherit bool) (memory.Bitmap, error) {
		words := 0
		if src != nil || inherit && inheritedNulls {
			words = memory.WordsFor(count)
		}
		if cap(dst) < words || cap(dst) > 0 && cap(dst) < memory.WordsFor(count) {
			fits = false
		}
		if err := add(int64(words) * 8); err != nil {
			return nil, err
		}
		if allocate && words > 0 {
			return make(memory.Bitmap, words), nil
		}
		return dst, nil
	}
	for name, v := range src.Int8s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact int8 %q", name)
		}
		out := dst.Int8s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Int8Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 1); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]int8, n)
			out.Validity = bits
			dst.Int8s[name] = out
		}
	}
	for name, v := range src.Int16s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact int16 %q", name)
		}
		out := dst.Int16s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Int16Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 2); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]int16, n)
			out.Validity = bits
			dst.Int16s[name] = out
		}
	}
	for name, v := range src.Int32s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact int32 %q", name)
		}
		out := dst.Int32s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Int32Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 4); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]int32, n)
			out.Validity = bits
			dst.Int32s[name] = out
		}
	}
	for name, v := range src.Int64s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact int64 %q", name)
		}
		out := dst.Int64s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Int64Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 8); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]int64, n)
			out.Validity = bits
			dst.Int64s[name] = out
		}
	}
	for name, v := range src.Uint8s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact uint8 %q", name)
		}
		out := dst.Uint8s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Uint8Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 1); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]uint8, n)
			out.Validity = bits
			dst.Uint8s[name] = out
		}
	}
	for name, v := range src.Uint16s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact uint16 %q", name)
		}
		out := dst.Uint16s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Uint16Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 2); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]uint16, n)
			out.Validity = bits
			dst.Uint16s[name] = out
		}
	}
	for name, v := range src.Uint32s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact uint32 %q", name)
		}
		out := dst.Uint32s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Uint32Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 4); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]uint32, n)
			out.Validity = bits
			dst.Uint32s[name] = out
		}
	}
	for name, v := range src.Uint64s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact uint64 %q", name)
		}
		out := dst.Uint64s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Uint64Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 8); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]uint64, n)
			out.Validity = bits
			dst.Uint64s[name] = out
		}
	}
	for name, v := range src.Float32s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact float32 %q", name)
		}
		out := dst.Float32s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Float32Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 4); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]float32, n)
			out.Validity = bits
			dst.Float32s[name] = out
		}
	}
	for name, v := range src.Decimals {
		if v == nil || src.Rows < 0 || len(v.Values) < src.Rows {
			return 0, false, fmt.Errorf("short compact decimal %q", name)
		}
		view := *v
		view.Values = view.Values[:src.Rows]
		if err := view.Validate(src.Rows); err != nil {
			return 0, false, fmt.Errorf("compact decimal %q: %w", name, err)
		}
		out := dst.Decimals[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Decimal128Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if int64(n) > (math.MaxInt64-total)/16 {
			return 0, false, fmt.Errorf("compact decimal size overflow")
		}
		if err := add(int64(n) * 16); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]memory.Decimal128, n)
			out.Validity = bits
			out.Precision, out.Scale = v.Precision, v.Scale
			dst.Decimals[name] = out
		}
	}
	for name, v := range src.Float64s {
		if v == nil || len(v.Values) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact float64 %q", name)
		}
		out := dst.Float64s[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.Float64Vector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Values) < n {
			fits = false
		}
		if err := add(int64(n) * 8); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Values = make([]float64, n)
			out.Validity = bits
			dst.Float64s[name] = out
		}
	}
	for name, v := range src.DictStrings {
		if v == nil || len(v.Indices) < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact dictionary %q", name)
		}
		out := dst.DictStrings[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.DictionaryStringVector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Indices) < n {
			fits = false
		}
		if err := add(int64(n) * 4); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Indices = make([]uint32, n)
			out.Validity = bits
			dst.DictStrings[name] = out
		}
	}
	for name, v := range src.Strings {
		size, err := compactStringSize(v, rows, parents)
		if err != nil {
			return 0, false, err
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact string validity")
		}
		out := dst.Strings[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.StringViewVector{}
			}
		}
		var old memory.Bitmap
		if out != nil {
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		if out == nil || cap(out.Offsets) < n+1 || cap(out.Data) < size {
			fits = false
		}
		if err := add(int64(n+1)*4 + int64(size)); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Offsets = make([]uint32, n+1)
			out.Data = make([]byte, size)
			out.Validity = bits
			dst.Strings[name] = out
		}
	}
	for name, v := range src.StringLists {
		children, size, err := compactListSize(v, rows, parents)
		if err != nil {
			return 0, false, err
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) || v.Values.Validity != nil && len(v.Values.Validity) < memory.WordsFor(len(v.Values.Offsets)-1) {
			return 0, false, fmt.Errorf("short compact list validity")
		}
		out := dst.StringLists[name]
		if out == nil || out.Values == nil {
			fits = false
			if allocate {
				out = &memory.StringListVector{Values: &memory.StringViewVector{}}
			}
		}
		var old, childOld memory.Bitmap
		if out != nil && out.Values != nil {
			old = out.Validity
			childOld = out.Values.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		childBits, err := validity(v.Values.Validity, childOld, children, false)
		if err != nil {
			return 0, false, err
		}
		if out == nil || out.Values == nil || cap(out.Offsets) < n+1 || cap(out.Values.Offsets) < children+1 || cap(out.Values.Data) < size {
			fits = false
		}
		if err := add(int64(n+1)*4 + int64(children+1)*4 + int64(size)); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Offsets = make([]uint32, n+1)
			out.Values.Offsets = make([]uint32, children+1)
			out.Values.Data = make([]byte, size)
			out.Validity = bits
			out.Values.Validity = childBits
			dst.StringLists[name] = out
		}
	}
	for name, v := range src.Int64Lists {
		children, err := compactInt64ListSize(v, rows, parents)
		if err != nil {
			return 0, false, err
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) || v.Values.Validity != nil && len(v.Values.Validity) < memory.WordsFor(len(v.Values.Values)) {
			return 0, false, fmt.Errorf("short compact list validity")
		}
		out := dst.Int64Lists[name]
		if out == nil || out.Values == nil {
			fits = false
			if allocate {
				out = &memory.Int64ListVector{Values: &memory.Int64Vector{}}
			}
		}
		var old, childOld memory.Bitmap
		if out != nil && out.Values != nil {
			old = out.Validity
			childOld = out.Values.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		childBits, err := validity(v.Values.Validity, childOld, children, false)
		if err != nil {
			return 0, false, err
		}
		if out == nil || out.Values == nil || cap(out.Offsets) < n+1 || cap(out.Values.Values) < children {
			fits = false
		}
		if err := add(int64(n+1)*4 + int64(children)*8); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Offsets = make([]uint32, n+1)
			out.Values.Values = make([]int64, children)
			out.Validity = bits
			out.Values.Validity = childBits
			dst.Int64Lists[name] = out
		}
	}
	for name, v := range src.Structs {
		if v == nil || v.Values == nil || v.Rows < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
			return 0, false, fmt.Errorf("short compact struct %q", name)
		}
		out := dst.Structs[name]
		if out == nil {
			fits = false
			if allocate {
				out = &memory.StructVector{Values: memory.NewBatch()}
			}
		}
		var child *memory.Batch
		var old memory.Bitmap
		if out != nil {
			child = out.Values
			old = out.Validity
		}
		bits, err := validity(v.Validity, old, n, true)
		if err != nil {
			return 0, false, err
		}
		childSource := *v.Values
		childSource.Rows = src.Rows
		childParents := &compactParent{validity: v.Validity, parent: parents}
		bytes, childFits, err := compactLayoutParents(&childSource, child, rows, allocate, bounded, childParents)
		if err != nil {
			return 0, false, err
		}
		if !childFits {
			fits = false
		}
		if err := add(bytes); err != nil {
			return 0, false, err
		}
		if allocate {
			out.Validity = bits
			dst.Structs[name] = out
		}
	}
	if len(src.Opaque) > 0 {
		contiguous := contiguousRows(rows)
		if bounded && !contiguous {
			return 0, false, compactOpaqueError()
		}
		for name, v := range src.Opaque {
			if v == nil || v.Rows < src.Rows || v.Validity != nil && len(v.Validity) < memory.WordsFor(src.Rows) {
				return 0, false, fmt.Errorf("short compact opaque %q", name)
			}
			arr, ok := v.Value.(arrow.Array)
			if !ok {
				if bounded {
					return 0, false, compactOpaqueError()
				}
				return 0, false, fmt.Errorf("opaque column %q is not an Arrow array", name)
			}
			if arr.Len() < src.Rows {
				return 0, false, fmt.Errorf("short compact Arrow array %q", name)
			}
			if !contiguous {
				fits = false
				continue
			}
			out := dst.Opaque[name]
			if out == nil {
				fits = false
				if allocate {
					out = &memory.OpaqueVector{}
				}
			}
			var old memory.Bitmap
			if out != nil {
				old = out.Validity
			}
			bits, err := validity(v.Validity, old, n, true)
			if err != nil {
				return 0, false, err
			}
			if allocate {
				out.Validity = bits
				dst.Opaque[name] = out
			}
		}
	}
	return total, fits, nil
}

// Drop input payload aliases before advancing the source. Retained owned
// buffers stay charged; a previous dictionary allocation must not be pinned
// after its source owner releases its own reservation.
func clearCompactBorrows(batch *memory.Batch) {
	if batch == nil {
		return
	}
	for _, v := range batch.DictStrings {
		v.Offsets = nil
		v.Data = nil
		v.DictionaryDomain = ""
	}
	for _, v := range batch.Opaque {
		if v.Owned {
			if value, ok := v.Value.(interface{ Release() }); ok {
				value.Release()
			}
		}
		v.Value = nil
		v.Owned = false
		v.Rows = 0
	}
	for _, v := range batch.Structs {
		v.Fields = nil
		clearCompactBorrows(v.Values)
	}
}
