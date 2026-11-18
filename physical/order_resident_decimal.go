package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

func residentHasNativeDecimal(fields []memory.Field) bool {
	for _, f := range fields {
		if f.Type == memory.TypeDecimal && f.DecimalPrecision != 0 {
			return true
		}
	}
	return false
}

// Schema Children can describe an opaque Arrow payload, not a typed binding.
// Construction validates only newly admitted root native Decimal metadata.
func validateResidentDecimalSchema(schema memory.Schema) error {
	for _, f := range schema.Fields {
		if f.Type == memory.TypeDecimal && f.DecimalPrecision != 0 {
			if err := f.ValidateDecimal128(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Only actual typed container roots participate in native nested admission.
// Opaque Struct metadata may be arbitrarily deep and retain Decimal children.
func validateResidentTypedDecimalBoundary(ctx context.Context, schema memory.Schema, b *memory.Batch) error {
	for _, f := range schema.Fields {
		typed := f.Type == memory.TypeStruct && b.Structs[f.Name] != nil
		if f.Type == memory.TypeList {
			typed = b.Arrays[f.Name] != nil || b.StructLists[f.Name] != nil || b.Int64Lists[f.Name] != nil || b.StringLists[f.Name] != nil
		}
		if !typed {
			continue
		}
		if err := rejectResidentNestedDecimal(ctx, f, 0); err != nil {
			return err
		}
	}
	return nil
}
func rejectResidentNestedDecimal(ctx context.Context, f memory.Field, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > memory.MaxStructDepth+memory.MaxNestedListDepth+1 {
		return fmt.Errorf("typed resident container exceeds supported depth")
	}
	if depth > 0 && f.Type == memory.TypeDecimal {
		return fmt.Errorf("native nested Decimal resident sort is unsupported")
	}
	for _, c := range f.Children {
		if err := rejectResidentNestedDecimal(ctx, c, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateResidentSortKeys(schema memory.Schema, keys []SortKey) error {
	for _, key := range keys {
		f, ok := schema.Field(key.Column)
		if !ok {
			return fmt.Errorf("sort column %q not found", key.Column)
		}
		if f.Type == memory.TypeDecimal {
			if err := f.ValidateDecimal128(); err != nil {
				return err
			}
			continue
		}
		if err := validateSortKeys(schema, []SortKey{key}); err != nil {
			return err
		}
	}
	return nil
}

// Validate all batches, even zero/one-row batches where no comparator is built.
// NULL coefficient bytes have no numeric meaning; their noncanonical bits are
// ignored for numeric validation, as the native Decimal contract permits.
func validateResidentDecimalBatch(ctx context.Context, schema memory.Schema, b *memory.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || b.Rows < 0 {
		return fmt.Errorf("invalid Decimal resident batch")
	}
	for _, f := range schema.Fields {
		if err := ctx.Err(); err != nil {
			return err
		}
		if f.Type != memory.TypeDecimal || f.DecimalPrecision == 0 {
			continue
		}
		if err := f.ValidateDecimal128(); err != nil {
			return err
		}
		v := b.Decimals[f.Name]
		if v == nil || len(v.Values) != b.Rows {
			return fmt.Errorf("Decimal column %q has missing or mismatched rows", f.Name)
		}
		if v.Precision != f.DecimalPrecision || v.Scale != f.DecimalScale {
			return fmt.Errorf("Decimal column %q metadata mismatch", f.Name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(b.Rows) {
			return fmt.Errorf("Decimal column %q short validity", f.Name)
		}
		for row, value := range v.Values {
			if row&63 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			valid := v.IsValid(row)
			if !f.Nullable && !valid {
				return fmt.Errorf("non-nullable decimal field %q contains NULL", f.Name)
			}
			if valid && !memory.Decimal128FitsPrecision(value, f.DecimalPrecision) {
				return fmt.Errorf("Decimal column %q row %d exceeds precision", f.Name, row)
			}
		}
	}
	return ctx.Err()
}

func bindResidentDecimalKey(v *memory.Decimal128Vector) boundSortKey {
	return boundSortKey{validity: v.Validity, compare: func(a, b int) int {
		x, y := v.Values[a], v.Values[b]
		if x.HighBits() < y.HighBits() {
			return -1
		}
		if x.HighBits() > y.HighBits() {
			return 1
		}
		if x.LowBits() < y.LowBits() {
			return -1
		}
		if x.LowBits() > y.LowBits() {
			return 1
		}
		return 0
	}}
}
