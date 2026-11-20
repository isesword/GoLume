package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

// Only resident Sort admits the new top-level three-axis payload. Existing
// opaque and shallow schema contracts remain owned by their original guards.
func validateResidentRepeatedListSchema(schema memory.Schema, resident bool) error {
	for _, f := range schema.Fields {
		if err := memory.ValidateRepeatedListDepth([]memory.Field{f}, memory.MaxNestedListDepth); err != nil {
			if !resident || !residentThreeAxisField(f) {
				return err
			}
			if err := f.ValidateNestedList(); err != nil {
				return err
			}
		}
	}
	return nil
}

func residentThreeAxisField(f memory.Field) bool {
	for axis := 0; axis < memory.MaxRepeatedListDepth; axis++ {
		if f.Type != memory.TypeList || len(f.Children) != 1 {
			return false
		}
		f = f.Children[0]
	}
	return f.Type == memory.TypeInt64 || f.Type == memory.TypeString
}

func residentHasThreeAxisPayload(schema memory.Schema) bool {
	for _, f := range schema.Fields {
		if residentThreeAxisField(f) && f.ValidateNestedList() == nil {
			return true
		}
	}
	return false
}

func residentRepeatedNodeCount(f memory.Field) (int64, error) {
	if err := f.ValidateNestedList(); err != nil {
		return 0, err
	}
	nodes := int64(1)
	for f.Type == memory.TypeList {
		nodes++
		f = f.Children[0]
	}
	return nodes, nil
}

// New typed payloads do not enter the opaque one-row-output owner path. Check
// delivered storage as well as schema, including zero-row batches and siblings.
func validateResidentThreeAxisStorage(ctx context.Context, schema memory.Schema, b *memory.Batch) error {
	if !residentHasThreeAxisPayload(schema) {
		return nil
	}
	var walk func(*memory.Batch, int) error
	walk = func(b *memory.Batch, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if b == nil || depth > memory.MaxStructDepth+1 {
			return fmt.Errorf("invalid resident three-axis storage graph")
		}
		if len(b.Opaque) != 0 {
			return fmt.Errorf("resident three-axis List payload cannot be combined with opaque storage")
		}
		for _, v := range b.Structs {
			if v == nil {
				return fmt.Errorf("nil resident Struct")
			}
			if err := walk(v.Values, depth+1); err != nil {
				return err
			}
		}
		for _, v := range b.StructLists {
			if v == nil || v.Values == nil {
				return fmt.Errorf("nil resident StructList")
			}
			if err := walk(v.Values.Values, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(b, 0)
}
