package lazy

import "gocolumn/expr"

// StructFrom constructs a non-NULL bounded record from ordered named children.
// Existing bounded Struct/List container profiles compose within their depth limits.
// Decimal, Binary and unsupported child profiles remain rejected at binding.
func StructFrom(fields ...expr.NamedExpr) expr.StructFromExpr { return expr.StructFrom(fields...) }

// StructGet extracts one literal-named direct child from a bounded Struct.
// A dot in fieldName is part of the field name, not a recursive path.
func StructGet(value expr.Expr, fieldName string) expr.StructGetExpr {
	return expr.StructGet(value, fieldName)
}

// ListGet extracts a zero-based native List<String> or List<Int64> element.
// NULL, negative and out-of-range indices return NULL; negative indices are not tail-relative.
func ListGet(value, index expr.Expr) expr.ListGetExpr { return expr.ListGet(value, index) }
