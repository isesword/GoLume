package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

// Native local TIME lists have Int64 storage but retain their logical schema.
// Keep this admission local to resident Sort, not generic list or spill codecs.
func validateResidentListTimeSchema(schema memory.Schema, resident bool) error {
	has := false
	var walk func(memory.Field, int) error
	walk = func(f memory.Field, depth int) error {
		// Arrow-owned payloads follow the existing opaque contract.
		if f.ArrowType != "" {
			return nil
		}
		if depth > memory.MaxStructDepth+memory.MaxNestedListDepth+1 {
			return nil
		}
		if f.Type == memory.TypeList && len(f.Children) == 1 && f.Children[0].Type == memory.TypeTime64 {
			if !resident {
				return fmt.Errorf("native List<Time64> is supported only by resident Sort, not TopK or spill")
			}
			if depth != 0 || !memory.IsListTime64(f) {
				return fmt.Errorf("resident Sort requires canonical top-level List<Time64>")
			}
			has = true
			return nil
		}
		for _, c := range f.Children {
			if err := walk(c, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	for _, f := range schema.Fields {
		if err := walk(f, 0); err != nil {
			return err
		}
	}
	if has && residentExtendedSchema(schema.Fields) {
		return fmt.Errorf("resident List<Time64> with extended nested payloads is unsupported")
	}
	return nil
}

func validateResidentListTimeBatch(ctx context.Context, schema memory.Schema, b *memory.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, f := range schema.Fields {
		if !memory.IsListTime64(f) {
			continue
		}
		if b == nil || b.Rows < 0 {
			return fmt.Errorf("invalid resident List<Time64> batch")
		}
		v := b.Int64Lists[f.Name]
		if v == nil || v.Values == nil || (v.Encoding != memory.EncodingView && v.Encoding != memory.EncodingPlain) || v.Values.Encoding != memory.EncodingPlain {
			return fmt.Errorf("resident List<Time64> %q storage or encoding mismatch", f.Name)
		}
		if b.StringLists[f.Name] != nil || b.Arrays[f.Name] != nil || b.StructLists[f.Name] != nil || b.Opaque[f.Name] != nil {
			return fmt.Errorf("resident List<Time64> %q has conflicting storage", f.Name)
		}
		if err := v.Validate(b.Rows); err != nil {
			return fmt.Errorf("resident List<Time64> %q: %w", f.Name, err)
		}
		for row := 0; row < b.Rows; row++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !v.IsValid(row) {
				if !f.Nullable {
					return fmt.Errorf("required List<Time64> %q contains NULL", f.Name)
				}
				continue
			}
			for i := v.Offsets[row]; i < v.Offsets[row+1]; i++ {
				if i&255 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if !v.Values.IsValid(int(i)) {
					if !f.Children[0].Nullable {
						return fmt.Errorf("required Time64 element in %q contains NULL", f.Name)
					}
					continue
				}
				if value := v.Values.Values[i]; value < 0 || value >= 86400_000_000_000 {
					return fmt.Errorf("resident Time64 element in %q outside local nanosecond day", f.Name)
				}
			}
		}
	}
	return ctx.Err()
}
