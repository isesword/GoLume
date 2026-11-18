package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"unsafe"
)

// Extended payloads use the resident growing owner, not the spill stream. Keep
// the legacy gather and its calibrated budgets for previously shallow schemas.
func residentExtendedSchema(fields []memory.Field) bool {
	for _, f := range fields {
		if memory.IsStructList(f) || memory.IsNestedList(f) {
			return true
		}
		if f.Type == memory.TypeStruct {
			for _, c := range f.Children {
				if c.Type == memory.TypeStruct {
					return true
				}
			}
			if residentExtendedSchema(f.Children) {
				return true
			}
		}
	}
	return false
}

// 2048 per sparse Batch and 1024 per field cover map entries/descriptors,
// schema-array copies and primitive-list descriptors. Repeated axes have their
// own descriptors and item batch, charged separately. Names remain borrowed.
func residentExtendedMetadata(ctx context.Context, fields []memory.Field, b *memory.Batch, depth int) (int64, int, error) {
	if b == nil || depth > memory.MaxStructDepth {
		return 0, 0, fmt.Errorf("invalid resident container graph")
	}
	total, dictionaries := int64(2048), 0
	for _, f := range fields {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if depth == 0 && residentUsesOpaque(f, b) {
			continue
		}
		n := int64(1024) + 2*int64(unsafe.Sizeof(memory.Field{}))
		var err error
		switch {
		case f.Type == memory.TypeStruct:
			v := b.Structs[f.Name]
			if v == nil {
				return 0, 0, fmt.Errorf("missing resident Struct")
			}
			child, d, e := residentExtendedMetadata(ctx, v.Fields, v.Values, depth+1)
			if e != nil {
				return 0, 0, e
			}
			dictionaries += d
			n, err = sortWorkspaceSize(n, child)
		case memory.IsStructList(f):
			v := b.StructLists[f.Name]
			if v == nil || v.Values == nil {
				return 0, 0, fmt.Errorf("missing resident StructList")
			}
			child, d, e := residentExtendedMetadata(ctx, v.Values.Fields, v.Values.Values, depth+1)
			if e != nil {
				return 0, 0, e
			}
			dictionaries += d
			n, err = sortWorkspaceSize(n, child, 1024)
		case memory.IsNestedList(f):
			n, err = sortWorkspaceSize(n, 3*1024)
		case f.Type == memory.TypeString || f.Type == memory.TypeBinary:
			if b.Strings[f.Name] == nil && b.DictStrings[f.Name] != nil {
				dictionaries++
			}
		}
		if err != nil {
			return 0, 0, err
		}
		total, err = sortWorkspaceSize(total, n)
		if err != nil {
			return 0, 0, err
		}
	}
	return total, dictionaries, nil
}

func residentExtendedHeaders(ctx context.Context, b *memory.Batch, depth int) (int64, error) {
	if b == nil || depth > memory.MaxStructDepth {
		return 0, fmt.Errorf("invalid resident view graph")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// Existing scalar/dictionary/opaque descriptors plus recursively counted
	// Structs. Add repeated-root descriptors without changing TopK's scope.
	total, err := topKSnapshotHeaders(b)
	if err != nil {
		return 0, err
	}
	var walk func(*memory.Batch, int) (int64, error)
	walk = func(b *memory.Batch, d int) (int64, error) {
		if b == nil || d > memory.MaxStructDepth {
			return 0, fmt.Errorf("invalid resident view graph")
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := sortWorkspaceSize(int64(len(b.Arrays))*4096, int64(len(b.StructLists))*2048)
		if err != nil {
			return 0, err
		}
		for _, v := range b.StructLists {
			child, e := residentExtendedHeaders(ctx, v.Values.Values, d+1)
			if e != nil {
				return 0, e
			}
			n, err = sortWorkspaceSize(n, child)
			if err != nil {
				return 0, err
			}
		}
		for _, v := range b.Structs {
			child, e := walk(v.Values, d+1)
			if e != nil {
				return 0, e
			}
			n, err = sortWorkspaceSize(n, child)
			if err != nil {
				return 0, err
			}
		}
		return n, nil
	}
	extra, err := walk(b, depth)
	if err != nil {
		return 0, err
	}
	return sortWorkspaceSize(total, extra)
}

func newResidentExtendedSorted(ctx context.Context, src *residentSortAccumulator, rows []int) (_ *residentSortAccumulator, err error) {
	out, err := newResidentSortAccumulator(ctx, src.schema, src.workspace, src.batch)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			out.close()
		}
	}()
	for _, row := range rows {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if row < 0 || row >= src.batch.Rows {
			return nil, fmt.Errorf("resident permutation index out of range")
		}
		if err = out.appendFrom(ctx, src, row); err != nil {
			return nil, err
		}
	}
	if err = out.finish(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// Filter only the existing opaque sidecar's logical fields, never typed roots.
// The temporary schema is admitted before allocation; nested validation still
// checks every declared and undeclared container, including hidden offsets.
func validateResidentExtendedInput(ctx context.Context, schema memory.Schema, b *memory.Batch, w *sortWorkspace) error {
	n, err := sortWorkspaceSize(1024, int64(len(schema.Fields))*int64(unsafe.Sizeof(memory.Field{})))
	if err != nil {
		return err
	}
	lease, err := w.Reserve(ctx, n)
	if err != nil {
		return err
	}
	defer lease.Release()
	fields := make([]memory.Field, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		if err := ctx.Err(); err != nil {
			return err
		}
		if residentUsesOpaque(f, b) {
			continue
		}
		fields = append(fields, f)
	}
	return memory.ValidateNestedListsBatch(b, memory.Schema{Fields: fields}, ctx.Err)
}
