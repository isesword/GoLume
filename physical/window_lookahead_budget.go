package physical

import (
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
)

// futureCloneAdmissionBytes includes descriptor/map storage as well as payload.
// CloneStructRange grows child arrays by append: a length-only estimate misses
// spare capacity and the old+new buffers live during growth. Four payload copies
// plus per-vector small-allocation slack conservatively cover that overlap. The
// complete admission is held until the retained clone is discarded; it is not a
// claim of exact live bytes. Metadata traversal allocates no tracking structures.
func futureCloneAdmissionBytes(batch *memory.Batch) (int64, error) {
	metadata, err := futureCloneMetadata(batch, 0)
	if err != nil {
		return 0, err
	}
	payload, err := memory.EstimateBatchRangeBytes(batch, 0, batch.Rows)
	if err != nil {
		return 0, err
	}
	return futureBudgetAdd(metadata, payload, 4)
}

func futureBudgetAdd(total, count, scale int64) (int64, error) {
	if total < 0 || count < 0 || scale < 0 || (scale != 0 && count > (math.MaxInt64-total)/scale) {
		return 0, fmt.Errorf("window lookahead clone admission overflows int64")
	}
	return total + count*scale, nil
}

func futureCloneMetadata(batch *memory.Batch, depth int) (int64, error) {
	// Native struct schemas currently have shallower supported depth. This also
	// rejects malformed cyclic batches before the recursive payload estimator.
	if batch == nil || depth > 64 {
		return 0, fmt.Errorf("window lookahead clone has nil or excessively nested batch")
	}
	// Batch, fifteen map headers, retained queue node and allocator slack.
	total := int64(4096)
	counts := [...]int{len(batch.Int8s), len(batch.Int16s), len(batch.Int32s), len(batch.Int64s), len(batch.Uint8s), len(batch.Uint16s), len(batch.Uint32s), len(batch.Uint64s), len(batch.Float32s), len(batch.Float64s), len(batch.Strings), len(batch.DictStrings), len(batch.StringLists), len(batch.Structs), len(batch.Opaque)}
	var err error
	for _, count := range counts {
		// Covers vector descriptor, map slots with growth overlap, and small backing
		// allocations/size-class rounding for payload and validity slices.
		total, err = futureBudgetAdd(total, int64(count), 512)
		if err != nil {
			return 0, err
		}
	}
	for _, list := range batch.StringLists {
		if list == nil || list.Values == nil {
			return 0, fmt.Errorf("window lookahead clone has nil list child")
		}
		total, err = futureBudgetAdd(total, 1, 512)
		if err != nil {
			return 0, err
		}
	}
	for _, record := range batch.Structs {
		if record == nil || record.Values == nil {
			return 0, fmt.Errorf("window lookahead clone has nil struct child")
		}
		// Fields is cloned separately from payload. Child Field.Children slices are
		// borrowed by the shallow Field copy, not duplicated here.
		total, err = futureBudgetAdd(total, int64(len(record.Fields)), 2*int64(unsafe.Sizeof(memory.Field{})))
		if err != nil {
			return 0, err
		}
		child, childErr := futureCloneMetadata(record.Values, depth+1)
		if childErr != nil {
			return 0, childErr
		}
		total, err = futureBudgetAdd(total, child, 1)
		if err != nil {
			return 0, err
		}
	}
	for _, opaque := range batch.Opaque {
		if opaque == nil {
			return 0, fmt.Errorf("window lookahead clone has nil opaque vector")
		}
		value, ok := opaque.Value.(arrow.Array)
		if !ok {
			return 0, fmt.Errorf("window lookahead opaque value lacks Arrow sizing contract")
		}
		extra, childErr := futureArrowCloneMetadata(value.Data(), depth+1)
		if childErr != nil {
			return 0, childErr
		}
		total, err = futureBudgetAdd(total, extra, 1)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func futureArrowCloneMetadata(data arrow.ArrayData, depth int) (int64, error) {
	if residentOpaqueIsNil(data) || depth > 64 {
		return 0, fmt.Errorf("window lookahead Arrow clone has nil or excessively nested data")
	}
	// NewSlice retains buffers but constructs ArrayData/typed array wrappers and
	// child/buffer pointer slices. Account recursive wrappers, not only buffers.
	total := int64(1024)
	var err error
	total, err = futureBudgetAdd(total, int64(len(data.Buffers())), 64)
	if err != nil {
		return 0, err
	}
	total, err = futureBudgetAdd(total, int64(len(data.Children())), 64)
	if err != nil {
		return 0, err
	}
	for _, child := range data.Children() {
		extra, e := futureArrowCloneMetadata(child, depth+1)
		if e != nil {
			return 0, e
		}
		total, err = futureBudgetAdd(total, extra, 1)
		if err != nil {
			return 0, err
		}
	}
	if dict := data.Dictionary(); !residentOpaqueIsNil(dict) {
		extra, e := futureArrowCloneMetadata(dict, depth+1)
		if e != nil {
			return 0, e
		}
		total, err = futureBudgetAdd(total, extra, 1)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// futurePreparationAdmissionBytes reserves copied schemas/descriptors, the
// hidden-name map and generated names before prepareFutureWindow allocates them.
// Directional fills also use this lease for their rolling-state descriptors
// and native output vector/map metadata. Existing offset-only past windows
// return without allocation or extra admission.
func futurePreparationAdmissionBytes(owner *WindowSource, input source.Source) (int64, error) {
	hasFuture := false
	for _, e := range owner.expressions {
		if needsFutureWindow(e) || isFillWindow(e.Function) {
			hasFuture = true
			break
		}
	}
	if !hasFuture {
		return 0, nil
	}
	total := int64(4096)
	var err error
	for _, count := range [...]int{len(input.Schema().Fields), len(owner.expressions), len(owner.outputFields)} {
		total, err = futureBudgetAdd(total, int64(count), 1024)
		if err != nil {
			return 0, err
		}
	}
	nested, err := futureSchemaMetadata(input.Schema().Fields, 0)
	if err != nil {
		return 0, err
	}
	return futureBudgetAdd(total, nested, 1)
}

// NewSchema recursively copies Children slices, even though the initial field
// slice append is shallow. Include those copies before source preparation.
func futureSchemaMetadata(fields []memory.Field, depth int) (int64, error) {
	if depth > 64 {
		return 0, fmt.Errorf("window lookahead schema nesting exceeds limit")
	}
	total, err := futureBudgetAdd(0, int64(len(fields)), 256)
	if err != nil {
		return 0, err
	}
	for _, field := range fields {
		if len(field.Children) == 0 {
			continue
		}
		child, childErr := futureSchemaMetadata(field.Children, depth+1)
		if childErr != nil {
			return 0, childErr
		}
		total, err = futureBudgetAdd(total, child, 1)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
