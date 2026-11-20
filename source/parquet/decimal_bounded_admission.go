package parquet

import (
	"fmt"
	"gocolumn/memory"
)

// ValidateDecimalBoundedTerminalInput admits only an original, unprojected flat
// source for the narrow Decimal bounded Lazy terminal. It checks private base
// metadata without copying schema/partition slices or opening a reader. The
// original source is capped at 64 row groups and eight plain Int64/Decimal fields;
// all caller-created pushdown clones and nested-source variants are rejected.
// This is not an RSS or Arrow metadata memory guarantee.
func (s *Source) ValidateDecimalBoundedTerminalInput() error {
	if s == nil || s.meta == nil || s.nativeForest || s.fixed != nil || len(s.structNodes) != 0 || len(s.nestedLists) != 0 {
		return fmt.Errorf("decimal bounded terminal requires original flat Parquet source")
	}
	if len(s.allParts) > 64 || len(s.parts) != len(s.allParts) {
		return fmt.Errorf("decimal bounded terminal original Parquet row groups exceed 64 or are pruned")
	}
	if s.pagePredicate != nil || len(s.report.Projection) != 0 || s.report.Predicate != "" {
		return fmt.Errorf("decimal bounded terminal rejects pre-pushdown Parquet clones")
	}
	for i, part := range s.allParts {
		if part != s.parts[i] || part.ID != i || part.Rows < 0 {
			return fmt.Errorf("decimal bounded terminal requires original Parquet partitions")
		}
	}
	count := len(s.baseSchema.Fields)
	if count < 1 || count > 8 || len(s.schema.Fields) != count || len(s.allBindings) != count || len(s.bindings) != count {
		return fmt.Errorf("decimal bounded terminal original Parquet fields must be 1..8 and unprojected")
	}
	for i, binding := range s.allBindings {
		if binding != s.bindings[i] || binding.Index != i || len(binding.Name) == 0 || len(binding.Name) > 128 {
			return fmt.Errorf("decimal bounded terminal original Parquet bindings mismatch")
		}
		if binding.Type != memory.TypeInt64 && binding.Type != memory.TypeDecimal {
			return fmt.Errorf("decimal bounded terminal original Parquet fields require Int64/native Decimal")
		}
		expected := fieldForBinding(binding)
		for _, field := range [2]memory.Field{s.baseSchema.Fields[i], s.schema.Fields[i]} {
			if field.Name != expected.Name || field.Type != expected.Type || field.Nullable != expected.Nullable || field.Encoding != memory.EncodingPlain || field.ArrowType != "" || len(field.Children) != 0 || field.DictionaryDomain != "" || field.DecimalPrecision != expected.DecimalPrecision || field.DecimalScale != expected.DecimalScale {
				return fmt.Errorf("decimal bounded terminal original Parquet field metadata mismatch")
			}
			if field.Type == memory.TypeDecimal {
				if err := field.ValidateDecimal128(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
