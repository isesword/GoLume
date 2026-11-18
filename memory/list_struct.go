package memory

import "fmt"

// ListStructField constructs the checked standalone List<Struct> profile. The
// element is a nonempty flat record of numeric, Bool, Date32, Timestamp or String
// fields. This does not enlarge ValidateList or general Batch admission.
func ListStructField(name string, nullable, elementNullable bool, children ...Field) (Field, error) {
	f := Field{Name: name, Type: TypeList, Nullable: nullable, Encoding: EncodingView, Children: []Field{{Name: "item", Type: TypeStruct, Nullable: elementNullable, Encoding: EncodingView, Children: children}}}
	if err := f.ValidateListStruct(); err != nil {
		return Field{}, err
	}
	return cloneField(f), nil
}

// ValidateListStruct checks the standalone bounded flat-element profile without
// cloning or traversing unsupported descendants.
func (f Field) ValidateListStruct() error { return checkListStructField(f, nil) }
func checkListStructField(f Field, check func() error) error {
	if f.Name == "" || f.Type != TypeList || len(f.Children) != 1 {
		return fmt.Errorf("List<Struct> requires a named List and one item")
	}
	item := f.Children[0]
	if item.Name != "item" || item.Type != TypeStruct || len(item.Children) == 0 {
		return fmt.Errorf("List<Struct> requires a nonempty Struct item")
	}
	if !structListEncoding(f.Encoding) || !structListEncoding(item.Encoding) || f.DictionaryDomain != "" || item.DictionaryDomain != "" {
		return fmt.Errorf("List<Struct> does not support dictionary metadata")
	}
	for i, c := range item.Children {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if c.Name == "" || len(c.Children) != 0 {
			return fmt.Errorf("List<Struct> element children must be named flat fields")
		}
		for j := 0; j < i; j++ {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if item.Children[j].Name == c.Name {
				return fmt.Errorf("duplicate element field %q", c.Name)
			}
		}
		if !structListEncoding(c.Encoding) || c.DictionaryDomain != "" {
			return fmt.Errorf("List<Struct> child %q requires native non-dictionary storage", c.Name)
		}
		switch c.Type {
		case TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeFloat32, TypeFloat64, TypeBool, TypeDate32, TypeTimestamp, TypeString:
		default:
			return fmt.Errorf("List<Struct> child %q has unsupported type %s", c.Name, c.Type)
		}
	}
	return nil
}

// NewSchemaWithStructListsChecked snapshots the explicitly extended schema
// profile after depth and shape validation. It describes standalone List<Struct>
// storage; it does not authorize consumers of ordinary Batch to accept it.
func NewSchemaWithStructListsChecked(fields ...Field) (Schema, error) {
	for _, f := range fields {
		if err := checkStructListTree(f, 0, nil); err != nil {
			return Schema{}, err
		}
	}
	return NewSchema(fields...), nil
}
func checkStructListTree(f Field, depth int, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if f.Type == TypeList && len(f.Children) == 1 && f.Children[0].Type == TypeStruct {
		if depth >= MaxStructDepth {
			return fmt.Errorf("Struct depth exceeds %d", MaxStructDepth)
		}
		return checkListStructField(f, check)
	}
	if f.Type != TypeStruct {
		return checkRecursiveField(f, depth, check)
	}
	if depth >= MaxStructDepth || len(f.Children) == 0 {
		return fmt.Errorf("Struct depth exceeds limit or empty Struct")
	}
	for i, c := range f.Children {
		if c.Name == "" {
			return fmt.Errorf("unnamed Struct child")
		}
		for j := 0; j < i; j++ {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if f.Children[j].Name == c.Name {
				return fmt.Errorf("duplicate Struct child %q", c.Name)
			}
		}
		if c.Type != TypeStruct && c.Type != TypeList {
			if err := (Field{Type: TypeStruct, Children: []Field{c}}).ValidateStruct(); err != nil {
				return err
			}
		}
		if err := checkStructListTree(c, depth+1, check); err != nil {
			return err
		}
	}
	return nil
}

// StructListParentDomain is a borrowed, read-only parent-row validity domain.
// Its zero value means no outer Struct ancestors. Nil ancestor bitmaps count as
// required (always present) Struct levels. It never indexes ancestors by element.
type StructListParentDomain struct {
	validity    [MaxStructDepth - 1]Bitmap
	count, rows int
}

// NewStructListParentDomain checks and snapshots ancestor bitmap references. The
// caller must keep their data immutable through validation or copying.
func NewStructListParentDomain(rows int, ancestors ...Bitmap) (StructListParentDomain, error) {
	var d StructListParentDomain
	if rows < 0 || len(ancestors) > MaxStructDepth-1 {
		return d, fmt.Errorf("invalid List<Struct> parent domain depth or rows")
	}
	for i, v := range ancestors {
		if !structValidityFits(v, rows) {
			return d, fmt.Errorf("ancestor validity is too short")
		}
		d.validity[i] = v
	}
	d.count = len(ancestors)
	d.rows = rows
	return d, nil
}
func (d StructListParentDomain) visible(row int) bool {
	for i := 0; i < d.count; i++ {
		if d.validity[i] != nil && !IsSet(d.validity[i], row) {
			return false
		}
	}
	return true
}

// StructListVector stores parent offsets into a separate element-row Struct.
// NULL parents may have hidden spans; owned copies canonicalize those to empty.
// This standalone vector is deliberately not registered in Batch yet.
type StructListVector struct {
	Offsets  []uint32
	Validity Bitmap
	Values   *StructVector
	Encoding Encoding
}

// Len returns the number of parent rows.
func (v *StructListVector) Len() int {
	if v == nil || len(v.Offsets) == 0 {
		return 0
	}
	return len(v.Offsets) - 1
}

// IsValid reports parent validity. Call validation before indexing untrusted data.
func (v *StructListVector) IsValid(row int) bool {
	return v != nil && row >= 0 && row < v.Len() && (v.Validity == nil || (row/64 < len(v.Validity) && IsSet(v.Validity, row)))
}

// ValueRange returns the element interval and parent validity.
func (v *StructListVector) ValueRange(row int) (int, int, bool) {
	if !v.IsValid(row) || v.Values == nil {
		return 0, 0, false
	}
	lo, hi := v.Offsets[row], v.Offsets[row+1]
	if lo > hi || uint64(hi) > uint64(v.Values.Rows) {
		return 0, 0, false
	}
	return int(lo), int(hi), true
}

// ValidateWithContext checks all storage, including hidden spans, and required
// values only in the full parent-row ancestor domain. check may be nil.
func (v *StructListVector) ValidateWithContext(f Field, rows int, d StructListParentDomain, check func() error) error {
	if err := v.validateStructure(f, rows, d, check); err != nil {
		return err
	}
	for r := 0; r < rows; r++ {
		if err := v.validateParent(f, d, r, check); err != nil {
			return err
		}
	}
	return nil
}
func (v *StructListVector) validateStructure(f Field, rows int, d StructListParentDomain, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if err := checkListStructField(f, check); err != nil {
		return err
	}
	if rows < 0 || v == nil || !structListEncoding(v.Encoding) || len(v.Offsets) == 0 || len(v.Offsets)-1 != rows || v.Offsets[0] != 0 || !structValidityFits(v.Validity, rows) {
		return fmt.Errorf("invalid List<Struct> parent storage")
	}
	if d.count > 0 && d.rows != rows {
		return fmt.Errorf("parent domain row count mismatch")
	}
	for i := 1; i < len(v.Offsets); i++ {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if v.Offsets[i] < v.Offsets[i-1] {
			return fmt.Errorf("List<Struct> offsets decrease")
		}
	}
	e := v.Values
	if e == nil || !structListEncoding(e.Encoding) || e.Values == nil || e.Rows < 0 || uint64(v.Offsets[rows]) != uint64(e.Rows) || e.Values.Rows != e.Rows || !structValidityFits(e.Validity, e.Rows) {
		return fmt.Errorf("List<Struct> element rows or validity mismatch")
	}
	children := f.Children[0].Children
	b := e.Values
	physicalFields := len(b.Int8s) + len(b.Int16s) + len(b.Int32s) + len(b.Int64s) + len(b.Uint8s) + len(b.Uint16s) + len(b.Uint32s) + len(b.Uint64s) + len(b.Float32s) + len(b.Float64s) + len(b.Strings)
	if physicalFields != len(children) {
		return fmt.Errorf("List<Struct> undeclared or missing element storage")
	}
	if len(e.Fields) != len(children) {
		return fmt.Errorf("List<Struct> element schema mismatch")
	}
	for i, c := range children {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		equal, err := recursiveFieldEqual(c, e.Fields[i], 0, check)
		if err != nil {
			return err
		}
		if !equal {
			return fmt.Errorf("List<Struct> child metadata mismatch")
		}
		if c.Type == TypeString && e.Values.Strings[c.Name] == nil {
			return fmt.Errorf("List<Struct> String requires view storage")
		}
		if err := validateStructChildStorageContext(e.Values, c, e.Rows, check); err != nil {
			return err
		}
	}
	if len(e.Values.Arrays)+len(e.Values.StructLists)+len(e.Values.Structs)+len(e.Values.StringLists)+len(e.Values.Int64Lists)+len(e.Values.DictStrings)+len(e.Values.Decimals)+len(e.Values.Opaque) != 0 {
		return fmt.Errorf("unsupported List<Struct> element storage")
	}
	return nil
}
func (v *StructListVector) validateParent(f Field, d StructListParentDomain, r int, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if !d.visible(r) {
		return nil
	}
	if !v.IsValid(r) {
		if !f.Nullable {
			return fmt.Errorf("required List<Struct> is NULL")
		}
		return nil
	}
	item := f.Children[0]
	for e := int(v.Offsets[r]); e < int(v.Offsets[r+1]); e++ {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if !v.Values.IsValid(e) {
			if !item.Nullable {
				return fmt.Errorf("required Struct item is NULL")
			}
			continue
		}
		for _, c := range item.Children {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if !c.Nullable && !batchFieldValid(v.Values.Values, c, e) {
				return fmt.Errorf("required item child %q is NULL", c.Name)
			}
		}
	}
	return nil
}

func structListEncoding(e Encoding) bool { return e == EncodingPlain || e == EncodingView }
