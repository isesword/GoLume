package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

// planStructListInitialGather uses the same initial-run owner and comparator.
// New nested roots have independent leases; flat payload keeps its old copier.
func planStructListInitialGather(ctx context.Context, schema memory.Schema, b *memory.Batch, start, end, keyCount int, ordinal uint64) (externalSortInitialGatherPlan, error) {
	var p externalSortInitialGatherPlan
	if err := externalSortInitialGatherRange(b, start, end, keyCount, ordinal); err != nil {
		return p, err
	}
	if err := validateSortInitialInput(ctx, schema, b); err != nil {
		return p, err
	}
	var owners int64
	for name, v := range b.Structs {
		f, ok := schema.Field(name)
		if !ok {
			return p, fmt.Errorf("sort Struct payload absent from schema")
		}
		var n int64
		var err error
		if memory.BatchHasArrays(v.Values) {
			n, err = memory.EstimateNestedListsStructTakeRangeBytes(ctx, v, f, start, end-start)
		} else if memory.BatchHasStructLists(v.Values) {
			n, err = memory.EstimateStructListsStructTakeRangeBytes(ctx, v, f, start, end-start)
		} else {
			n, err = memory.EstimateRecursiveStructTakeRangeBytes(ctx, v, f, start, end-start)
		}
		if err != nil {
			return p, err
		}
		owners, err = sortWorkspaceSize(owners, n)
		if err != nil {
			return p, err
		}
	}
	for name, v := range b.StructLists {
		f, ok := schema.Field(name)
		if !ok {
			return p, fmt.Errorf("sort StructList payload absent from schema")
		}
		n, err := memory.EstimateStructListTakeRangeBytes(ctx, v, f, memory.StructListParentDomain{}, start, end-start)
		if err != nil {
			return p, err
		}
		owners, err = sortWorkspaceSize(owners, n)
		if err != nil {
			return p, err
		}
	}
	for name, v := range b.Arrays {
		f, ok := schema.Field(name)
		if !ok {
			return p, fmt.Errorf("sort array absent from schema")
		}
		n, err := memory.EstimateNestedListTakeRangeBytes(ctx, v, f, memory.NestedListParentDomain{}, start, end-start)
		if err != nil {
			return p, err
		}
		owners, err = sortWorkspaceSize(owners, n)
		if err != nil {
			return p, err
		}
	}
	data, err := memory.EstimateBatchRangeBytesWithCheck(b, start, end-start, ctx.Err)
	if err != nil {
		return p, err
	}
	metadata, err := externalSortInitialGatherMapMetadata(externalSortInitialGatherCounts(b))
	if err != nil {
		return p, err
	}
	// Cover transient bindings, extra map, owner arrays and copied ordinal vector.
	var roots int64
	if err := addJoinGatherProduct(&roots, uint64(len(b.Structs))+uint64(len(b.StructLists))+uint64(len(b.Arrays)), 2048); err != nil {
		return p, err
	}
	extra, err := sortWorkspaceSize(8192, roots)
	if err != nil {
		return p, err
	}
	metadata, err = sortWorkspaceSize(metadata, extra)
	if err != nil {
		return p, err
	}
	binding, err := sortBindingBytes(keyCount)
	if err != nil {
		return p, err
	}
	rows := end - start
	var arrays int64
	if err = addJoinGatherProduct(&arrays, uint64(rows), 16); err != nil {
		return p, err
	}
	// Existing flat copying retains validity representation. Double data is a
	// conservative floor, not the new owners' actual bytes or a process bound.
	flat, err := sortWorkspaceSize(data, data, metadata, binding, arrays)
	if err != nil {
		return p, err
	}
	gather, err := sortWorkspaceSize(flat, owners)
	if err != nil {
		return p, err
	}
	encoded, err := memory.EstimateColumnarRunRangeBytesWithCheck(schema, b, start, rows, ctx.Err)
	if err != nil {
		return p, err
	}
	encoded, err = sortWorkspaceSize(encoded, owners, arrays, metadata)
	if err != nil {
		return p, err
	}
	return externalSortInitialGatherPlan{gatherBytes: gather, recursiveBytes: owners, encodeBytes: encoded, rangeBytes: data, capacityBytes: flat, metadataBytes: metadata, start: start, end: end, source: b, keyCount: keyCount, structLists: true}, nil
}
func (a *externalSortInitialGather) copyStructListRoots(ctx context.Context, m memory.Manager, schema memory.Schema, src *memory.Batch) error {
	a.batch.StructLists = make(map[string]*memory.StructListVector, len(src.StructLists))
	if len(src.Arrays) > 0 {
		a.batch.Arrays = make(map[string]*memory.ArrayNode, len(src.Arrays))
	}
	for name, v := range src.Arrays {
		f, ok := schema.Field(name)
		if !ok {
			return fmt.Errorf("missing array field")
		}
		o, err := memory.TakeNestedListRowsOwned(ctx, m, v, f, memory.NestedListParentDomain{}, a.indices)
		if err != nil {
			return err
		}
		a.arrayOwners = append(a.arrayOwners, o)
		a.batch.Arrays[name] = o.Vector
	}

	for name, v := range src.Structs {
		f, ok := schema.Field(name)
		if !ok {
			return fmt.Errorf("missing Struct payload field")
		}
		var o *memory.OwnedRecursiveStruct
		var err error
		if memory.BatchHasArrays(v.Values) {
			o, err = memory.TakeNestedListsStructRowsOwned(ctx, m, v, f, a.indices)
		} else if memory.BatchHasStructLists(v.Values) {
			o, err = memory.TakeStructListsStructRowsOwned(ctx, m, v, f, a.indices)
		} else {
			o, err = memory.TakeRecursiveStructRowsOwned(ctx, m, v, f, a.indices)
		}
		if err != nil {
			return err
		}
		a.recursiveOwners = append(a.recursiveOwners, o)
		a.batch.Structs[name] = o.Vector
	}
	for name, v := range src.StructLists {
		f, ok := schema.Field(name)
		if !ok {
			return fmt.Errorf("missing StructList payload field")
		}
		o, err := memory.TakeStructListRowsOwned(ctx, m, v, f, memory.StructListParentDomain{}, a.indices)
		if err != nil {
			return err
		}
		a.structListOwners = append(a.structListOwners, o)
		a.batch.StructLists[name] = o.Vector
	}
	return nil
}

func validateSortInitialInput(ctx context.Context, schema memory.Schema, b *memory.Batch) error {
	// Validate every input batch before any range sizing or run is created.
	if err := validateResidentTypedDecimalBoundary(ctx, schema, b); err != nil {
		return err
	}
	if residentHasNativeDecimal(schema.Fields) {
		if err := validateResidentDecimalBatch(ctx, schema, b); err != nil {
			return err
		}
	}

	if !schema.HasStructLists() && !schema.HasNestedLists() {
		return validateExternalSortInitialGather(b)
	}
	if schema.HasNestedLists() {
		if err := memory.ValidateNestedListsBatch(b, schema, ctx.Err); err != nil {
			return err
		}
	} else if err := memory.ValidateStructListsBatch(b, schema, ctx.Err); err != nil {
		return err
	}
	// All flat runtime maps are copied, including undeclared extras. Retain
	// the old storage/opaque/dictionary checks for that entire borrowed view.
	flat := *b
	flat.Structs = nil
	flat.StructLists = nil
	flat.Arrays = nil
	return validateExternalSortInitialGather(&flat)
}
