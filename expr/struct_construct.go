package expr

import (
	"fmt"
	"gocolumn/memory"
	"strings"
)

// StructFromExpr constructs a bounded record from ordered named expressions.
// Use StructFrom to snapshot the caller's field slice.
type StructFromExpr struct{ Fields []NamedExpr }

func (StructFromExpr) exprNode() {}

// StructFrom constructs an owned non-NULL record in each active row. Child
// NULLs remain child NULLs, even when all children are NULL. At least one unique
// named field is required. Bare columns may keep their names; computed fields
// require aliases. Existing scalar and checked container profiles are supported;
// the constructed parent must remain within their depth limits. Decimal children
// and additional container shapes are not admitted.
func StructFrom(fields ...NamedExpr) StructFromExpr {
	return StructFromExpr{Fields: append([]NamedExpr(nil), fields...)}
}

// String preserves field names and expression order for display.
func (e StructFromExpr) String() string {
	var b strings.Builder
	b.WriteString("STRUCT_FROM(")
	for i, field := range e.Fields {
		if i > 0 {
			b.WriteByte(',')
		}
		name, _ := field.Name()
		fmt.Fprintf(&b, "%q:%s", name, field.Expr)
	}
	b.WriteByte(')')
	return b.String()
}

// Alias names a constructed record output.
func (e StructFromExpr) Alias(name string) NamedExpr { return Named(e, name) }

func bindStructFrom(bound *BoundExpr, node StructFromExpr) error {
	fields := make([]memory.Field, len(node.Fields))
	for i, named := range node.Fields {
		name, err := named.Name()
		if err != nil {
			return err
		}
		field := cloneBoundField(bound.Children[i].Field)
		if structConstructExtendedField(field) {
			field.Name = name
			if err := field.ValidateArrayContainers(); err != nil {
				return err
			}
			field.Encoding = memory.EncodingView
			fields[i] = field
			continue
		}
		if field.Type == memory.TypeList {
			if err := field.ValidateList(); err != nil {
				return err
			}
			if len(field.Children[0].Children) != 0 || field.Children[0].Encoding == memory.EncodingDictionary || field.Children[0].DictionaryDomain != "" {
				return fmt.Errorf("STRUCT_FROM does not support recursive or dictionary list elements")
			}
			field.Children[0].Encoding = memory.EncodingPlain
			if field.Children[0].Type == memory.TypeString {
				field.Children[0].Encoding = memory.EncodingView
			}
		} else if len(field.Children) != 0 {
			return fmt.Errorf("STRUCT_FROM does not support recursive child metadata")
		}
		if field.Encoding == memory.EncodingDictionary && field.Type != memory.TypeString {
			return fmt.Errorf("STRUCT_FROM dictionary input must be String")
		}
		field.Name = name
		field.Encoding = memory.EncodingPlain
		field.DictionaryDomain = ""
		if field.Type == memory.TypeString || field.Type == memory.TypeList {
			field.Encoding = memory.EncodingView
		}
		fields[i] = field
	}
	parent := memory.StructField("", false, fields...)
	validate := parent.ValidateStruct
	if structConstructExtendedFields(fields) {
		checked := parent
		checked.Name = "expression"
		validate = checked.ValidateArrayContainers
	}
	if err := validate(); err != nil {
		return err
	}
	bound.Field = parent
	return nil
}
