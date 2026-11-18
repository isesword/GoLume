package memory

import "fmt"

// MaxNestedListDepth bounds the new checked array profile to two repeated axes.
// It is an engineering limit independent of MaxStructDepth.
const MaxNestedListDepth = 2

// NestedListField snapshots exactly List<List<Int64>> or List<List<String>>.
// inner must be a primitive List field named item. Existing admission is unchanged.
func NestedListField(name string, nullable bool, inner Field) (Field, error) {
	f := Field{Name: name, Type: TypeList, Nullable: nullable, Encoding: EncodingView, Children: []Field{inner}}
	if err := f.ValidateNestedList(); err != nil {
		return Field{}, err
	}
	return cloneField(f), nil
}

// ValidateNestedList checks the bounded two-List shape before any recursive clone.
func (f Field) ValidateNestedList() error { return checkNestedListField(f, nil) }
func checkNestedListField(f Field, check func() error) error {
	for depth := 0; depth < MaxNestedListDepth; depth++ {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if f.Name == "" || (depth > 0 && f.Name != "item") || f.Type != TypeList || len(f.Children) != 1 || !structListEncoding(f.Encoding) || f.DictionaryDomain != "" || f.DecimalPrecision != 0 || f.DecimalScale != 0 {
			return fmt.Errorf("invalid checked nested List field")
		}
		f = f.Children[0]
	}
	if f.Name != "item" || (f.Type != TypeInt64 && f.Type != TypeString) || len(f.Children) != 0 || !structListEncoding(f.Encoding) || f.DictionaryDomain != "" || f.DecimalPrecision != 0 || f.DecimalScale != 0 {
		return fmt.Errorf("nested List terminal must be native Int64 or String")
	}
	return recursiveCheck(check)
}

// NewSchemaWithNestedListsChecked validates and snapshots bounded nested Lists
// and existing checked shapes. It does not admit this profile to Batch consumers.
// Up to eight outer Structs plus two Lists form a maximum ten-container path.
func NewSchemaWithNestedListsChecked(fields ...Field) (Schema, error) {
	var walk func(Field, int) error
	walk = func(f Field, depth int) error {
		if f.Name == "" {
			return fmt.Errorf("unnamed nested field")
		}
		if f.Type == TypeList && len(f.Children) == 1 && f.Children[0].Type == TypeList {
			return checkNestedListField(f, nil)
		}
		if f.Type != TypeStruct {
			if depth > 0 && f.Type != TypeList {
				if err := (Field{Type: TypeStruct, Children: []Field{f}}).ValidateStruct(); err != nil {
					return err
				}
			}
			return checkStructListTree(f, depth, nil)
		}
		if depth >= MaxStructDepth || len(f.Children) == 0 {
			return fmt.Errorf("Struct depth exceeds limit or empty Struct")
		}
		if !structListEncoding(f.Encoding) || f.DictionaryDomain != "" {
			return fmt.Errorf("invalid Struct metadata")
		}
		for i, c := range f.Children {
			for j := 0; j < i; j++ {
				if c.Name == f.Children[j].Name {
					return fmt.Errorf("duplicate Struct child %q", c.Name)
				}
			}
			if err := walk(c, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	for i, f := range fields {
		for j := 0; j < i; j++ {
			if f.Name == fields[j].Name {
				return Schema{}, fmt.Errorf("duplicate root field %q", f.Name)
			}
		}
		if err := walk(f, 0); err != nil {
			return Schema{}, err
		}
	}
	return NewSchema(fields...), nil
}

// ArrayNode is a checked typed array node, not a new Batch storage map.
// List nodes use Offsets/Validity/Child; terminal nodes use exactly Int64 or Strings.
// Each List edge changes row coordinates. Inputs are borrowed and must stay stable.
type ArrayNode struct {
	Kind     DataType
	Offsets  []uint32
	Validity Bitmap
	Child    *ArrayNode
	Int64    *Int64Vector
	Strings  *StringViewVector
	Encoding Encoding
}

// Len returns the logical row count for this node, or zero for missing storage.
func (v *ArrayNode) Len() int {
	if v == nil {
		return 0
	}
	switch v.Kind {
	case TypeList:
		if len(v.Offsets) > 0 {
			return len(v.Offsets) - 1
		}
	case TypeInt64:
		if v.Int64 != nil {
			return len(v.Int64.Values)
		}
	case TypeString:
		if v.Strings != nil {
			return v.Strings.Len()
		}
	}
	return 0
}

// IsValid safely reports row validity; malformed storage must still be validated.
func (v *ArrayNode) IsValid(row int) bool {
	if v == nil || row < 0 || row >= v.Len() {
		return false
	}
	bits := v.Validity
	if v.Kind == TypeInt64 {
		bits = v.Int64.Validity
	}
	if v.Kind == TypeString {
		bits = v.Strings.Validity
	}
	return bits == nil || (row/64 < len(bits) && IsSet(bits, row))
}

// ValueRange returns a List child interval, or false for NULL/invalid rows.
func (v *ArrayNode) ValueRange(row int) (int, int, bool) {
	if v == nil || v.Kind != TypeList || !v.IsValid(row) || v.Child == nil {
		return 0, 0, false
	}
	lo, hi := v.Offsets[row], v.Offsets[row+1]
	if lo > hi || uint64(hi) > uint64(v.Child.Len()) {
		return 0, 0, false
	}
	return int(lo), int(hi), true
}

// NestedListParentDomain borrows up to eight outer Struct bitmaps in R0 coordinates.
// A zero domain means no outer ancestors, and nil ancestor bitmaps mean valid.
type NestedListParentDomain struct {
	validity    [MaxStructDepth]Bitmap
	count, rows int
}

// NewNestedListParentDomain checks ancestor masks without copying their payloads.
func NewNestedListParentDomain(rows int, ancestors ...Bitmap) (NestedListParentDomain, error) {
	var d NestedListParentDomain
	if !nestedListCountOK(rows) || len(ancestors) > MaxStructDepth {
		return d, fmt.Errorf("invalid nested List parent domain")
	}
	for i, b := range ancestors {
		if !structValidityFits(b, rows) {
			return d, fmt.Errorf("short nested List ancestor bitmap")
		}
		d.validity[i] = b
	}
	d.count = len(ancestors)
	d.rows = rows
	return d, nil
}
func (d NestedListParentDomain) visible(row int) bool {
	for i := 0; i < d.count; i++ {
		if d.validity[i] != nil && !IsSet(d.validity[i], row) {
			return false
		}
	}
	return true
}

// ValidateWithContext checks every storage span, including hidden data, and
// required values only in the visible parent-to-inner-to-terminal domain.
func (v *ArrayNode) ValidateWithContext(f Field, rows int, d NestedListParentDomain, check func() error) error {
	if err := v.validateNestedStructure(f, rows, d, check); err != nil {
		return err
	}
	for r := 0; r < rows; r++ {
		if err := v.validateNestedParent(f, d, r, check); err != nil {
			return err
		}
	}
	return nil
}
func (v *ArrayNode) validateNestedStructure(f Field, rows int, d NestedListParentDomain, check func() error) error {
	if err := checkNestedListField(f, check); err != nil {
		return err
	}
	if !nestedListCountOK(rows) || (d.count > 0 && d.rows != rows) {
		return fmt.Errorf("nested List parent row mismatch")
	}
	node := v
	var seen [3]*ArrayNode
	for axis := 0; axis < 3; axis++ {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if !nestedListCountOK(rows) || node == nil || !structListEncoding(node.Encoding) || node.Kind != f.Type {
			return fmt.Errorf("nested array node kind/storage mismatch")
		}
		for j := 0; j < axis; j++ {
			if node == seen[j] {
				return fmt.Errorf("nested array cycle")
			}
		}
		seen[axis] = node
		if axis < 2 {
			if node.Int64 != nil || node.Strings != nil || node.Child == nil || len(node.Offsets) == 0 || len(node.Offsets)-1 != rows || node.Offsets[0] != 0 || !structValidityFits(node.Validity, rows) {
				return fmt.Errorf("invalid nested List storage")
			}
			for i := 1; i < len(node.Offsets); i++ {
				if err := recursiveCheck(check); err != nil {
					return err
				}
				if node.Offsets[i] < node.Offsets[i-1] {
					return fmt.Errorf("nested List offsets decrease")
				}
			}
			n := node.Child.Len()
			if uint64(node.Offsets[rows]) != uint64(n) {
				return fmt.Errorf("nested List terminal offset mismatch")
			}
			rows = n
			node = node.Child
			f = f.Children[0]
			continue
		}
		if node.Child != nil || len(node.Offsets) != 0 || node.Validity != nil {
			return fmt.Errorf("extraneous terminal node storage")
		}
		if node.Kind == TypeInt64 {
			if node.Int64 == nil || node.Strings != nil || len(node.Int64.Values) != rows || !structListEncoding(node.Int64.Encoding) || !structValidityFits(node.Int64.Validity, rows) {
				return fmt.Errorf("invalid nested Int64 storage")
			}
		} else {
			s := node.Strings
			if s == nil || node.Int64 != nil || !structListEncoding(s.Encoding) || len(s.Offsets) == 0 || len(s.Offsets)-1 != rows || s.Offsets[0] != 0 || !structValidityFits(s.Validity, rows) || uint64(s.Offsets[rows]) != uint64(len(s.Data)) {
				return fmt.Errorf("invalid nested String storage")
			}
			for i := 1; i < len(s.Offsets); i++ {
				if err := recursiveCheck(check); err != nil {
					return err
				}
				if s.Offsets[i] < s.Offsets[i-1] {
					return fmt.Errorf("nested String offsets decrease")
				}
			}
		}
	}
	return nil
}
func (v *ArrayNode) validateNestedParent(f Field, d NestedListParentDomain, r int, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if !d.visible(r) {
		return nil
	}
	if !v.IsValid(r) {
		if !f.Nullable {
			return fmt.Errorf("required outer List is NULL")
		}
		return nil
	}
	inner, leaf := v.Child, v.Child.Child
	innerField := f.Children[0]
	leafField := innerField.Children[0]
	for i := int(v.Offsets[r]); i < int(v.Offsets[r+1]); i++ {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if !inner.IsValid(i) {
			if !innerField.Nullable {
				return fmt.Errorf("required inner List is NULL")
			}
			continue
		}
		for j := int(inner.Offsets[i]); j < int(inner.Offsets[i+1]); j++ {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if !leaf.IsValid(j) && !leafField.Nullable {
				return fmt.Errorf("required terminal value is NULL")
			}
		}
	}
	return nil
}
