package expr

import (
	"fmt"
	"gocolumn/memory"
)

// StructGetExpr extracts one literal-named direct field from a bounded Struct.
// It never interprets dots as paths or performs recursive traversal.
type StructGetExpr struct {
	Value     Expr
	FieldName string
}

func (StructGetExpr) exprNode() {}

// String returns the expression with its literal child field name.
func (e StructGetExpr) String() string { return fmt.Sprintf("STRUCT_GET(%s,%q)", e.Value, e.FieldName) }

// StructGet returns an owned direct-child value; a NULL parent produces NULL.
func StructGet(value Expr, fieldName string) StructGetExpr {
	return StructGetExpr{Value: value, FieldName: fieldName}
}

// Alias names the extracted output.
func (e StructGetExpr) Alias(name string) NamedExpr { return Named(e, name) }

type structGetPlan struct {
	parent memory.Field
	child  memory.Field
	index  int
}

func bindStructGet(bound *BoundExpr, n StructGetExpr) error {
	parent := bound.Children[0].Field
	if parent.Type != memory.TypeStruct {
		return fmt.Errorf("STRUCT_GET requires native Struct")
	}
	if err := validateExpressionContainers(parent); err != nil {
		return fmt.Errorf("STRUCT_GET: %w", err)
	}
	if n.FieldName == "" {
		return fmt.Errorf("STRUCT_GET requires a nonempty literal field name")
	}
	for _, child := range parent.Children {
		if child.Name != n.FieldName {
			continue
		}
		bound.Field = cloneBoundField(child)
		bound.Field.Nullable = parent.Nullable || child.Nullable
		bound.Field.Name = ""
		bound.Field.DictionaryDomain = ""
		switch child.Type {
		case memory.TypeString, memory.TypeList, memory.TypeStruct:
			bound.Field.Encoding = memory.EncodingView
			if child.Type == memory.TypeList {
				bound.Field.Children[0].DictionaryDomain = ""
				bound.Field.Children[0].Encoding = memory.EncodingPlain
				if bound.Field.Children[0].Type == memory.TypeString {
					bound.Field.Children[0].Encoding = memory.EncodingView
				}
			}
		default:
			bound.Field.Encoding = memory.EncodingPlain
		}
		return nil
	}
	return fmt.Errorf("STRUCT_GET field %q not found", n.FieldName)
}

// validateStructGetInput keeps N1 to the preexisting bounded Struct contract.
// Scalar-shaped children cannot conceal recursive metadata; primitive lists
// retain the same nonrecursive metadata and encoding rules as ListGet.
func validateStructGetInput(parent memory.Field) error {
	if err := parent.ValidateStruct(); err != nil {
		return err
	}
	for _, child := range parent.Children {
		if child.Type != memory.TypeList {
			if len(child.Children) != 0 {
				return fmt.Errorf("bounded Struct child %q has recursive metadata", child.Name)
			}
			continue
		}
		item := child.Children[0]
		if len(item.Children) != 0 || child.DictionaryDomain != "" || item.DictionaryDomain != "" ||
			(child.Encoding != memory.EncodingPlain && child.Encoding != memory.EncodingView) ||
			(item.Encoding != memory.EncodingPlain && item.Encoding != memory.EncodingView) {
			return fmt.Errorf("bounded Struct child %q requires primitive non-dictionary list metadata", child.Name)
		}
	}
	return nil
}
