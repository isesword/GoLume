package memory

import (
	"context"
	"fmt"
)

// IsNestedList identifies a two-List shape; it does not replace checked admission.
func IsNestedList(f Field) bool {
	return f.Type == TypeList && len(f.Children) == 1 && f.Children[0].Type == TypeList
}

// HasNestedLists reports whether the bounded schema contains shared array nodes.
func (s Schema) HasNestedLists() bool {
	for _, f := range s.Fields {
		if fieldHasNestedLists(f, 0) {
			return true
		}
	}
	return false
}
func fieldHasNestedLists(f Field, depth int) bool {
	if IsNestedList(f) {
		return true
	}
	if f.Type != TypeStruct || depth >= MaxStructDepth {
		return false
	}
	for _, c := range f.Children {
		if fieldHasNestedLists(c, depth+1) {
			return true
		}
	}
	return false
}

// ValidateNestedListsBatch validates the explicit container profile, including
// undeclared nested storage and all ancestor domains. Inputs must stay immutable.
func ValidateNestedListsBatch(b *Batch, s Schema, check func() error) error {
	return validateNestedListsBatch(b, s, check, false)
}

func validateNestedListsBatch(b *Batch, s Schema, check func() error, allowRootTimeList bool) error {
	for i, f := range s.Fields {
		for j := 0; j < i; j++ {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if f.Name == s.Fields[j].Name {
				return fmt.Errorf("duplicate root field %q", f.Name)
			}
		}
		if !allowRootTimeList || !IsListTime64(f) {
			if err := checkArrayContainerField(f, 0, check); err != nil {
				return err
			}
		}
	}
	if err := checkArrayBatchGraph(b, s.Fields, 0, check); err != nil {
		return err
	}
	var chain [MaxStructDepth]*StructVector
	for _, f := range s.Fields {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		switch {
		case f.Type == TypeStruct:
			if err := validateContainerVector(b.Structs[f.Name], f, b.Rows, check, &chain, 0, true); err != nil {
				return err
			}
		case IsNestedList(f):
			if err := b.Arrays[f.Name].ValidateWithContext(f, b.Rows, NestedListParentDomain{}, check); err != nil {
				return err
			}
		case IsStructList(f):
			if err := b.StructLists[f.Name].ValidateWithContext(f, b.Rows, StructListParentDomain{}, check); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkArrayContainerField(f Field, depth int, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if f.Name == "" {
		return fmt.Errorf("unnamed container field")
	}
	if IsNestedList(f) {
		return checkNestedListField(f, check)
	}
	if f.Type != TypeStruct {
		if depth > 0 && f.Type != TypeList {
			if err := (Field{Type: TypeStruct, Children: []Field{f}}).ValidateStruct(); err != nil {
				return err
			}
		}
		return checkStructListTree(f, depth, check)
	}
	if depth >= MaxStructDepth || len(f.Children) == 0 || !structListEncoding(f.Encoding) || f.DictionaryDomain != "" {
		return fmt.Errorf("invalid or excessively deep Struct %q", f.Name)
	}
	for i, c := range f.Children {
		for j := 0; j < i; j++ {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if c.Name == f.Children[j].Name {
				return fmt.Errorf("duplicate Struct child %q", c.Name)
			}
		}
		if err := checkArrayContainerField(c, depth+1, check); err != nil {
			return err
		}
	}
	return nil
}
func checkArrayBatchGraph(b *Batch, fields []Field, depth int, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if b == nil || b.Rows < 0 || depth > MaxStructDepth {
		return fmt.Errorf("invalid array batch graph")
	}
	find := func(name string, shape func(Field) bool) (Field, error) {
		for _, f := range fields {
			if err := recursiveCheck(check); err != nil {
				return Field{}, err
			}
			if f.Name == name && shape(f) {
				return f, nil
			}
		}
		return Field{}, fmt.Errorf("undeclared container %q", name)
	}
	for name, v := range b.Arrays {
		if _, err := find(name, IsNestedList); err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("nil array %q", name)
		}
	}
	for name, v := range b.StructLists {
		if _, err := find(name, IsStructList); err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("nil StructList %q", name)
		}
	}
	for name, v := range b.Structs {
		f, err := find(name, func(f Field) bool { return f.Type == TypeStruct })
		if err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("nil Struct %q", name)
		}
		if err := checkArrayBatchGraph(v.Values, f.Children, depth+1, check); err != nil {
			return err
		}
	}
	return nil
}

// CloneNestedListsStructRangeOwned owns a Struct containing shared array nodes.
// The complete input graph is validated before any ownership admission.
func CloneNestedListsStructRangeOwned(ctx context.Context, m Manager, src *StructVector, f Field, offset, length int) (*OwnedRecursiveStruct, error) {
	if ctx == nil || m == nil || src == nil || f.Type != TypeStruct || offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return nil, fmt.Errorf("invalid array Struct range")
	}
	if err := checkArrayContainerField(f, 0, ctx.Err); err != nil {
		return nil, err
	}
	if err := checkArrayBatchGraph(src.Values, f.Children, 1, ctx.Err); err != nil {
		return nil, err
	}
	var chain [MaxStructDepth]*StructVector
	if err := validateContainerVector(src, f, src.Rows, ctx.Err, &chain, 0, true); err != nil {
		return nil, err
	}
	leases := &recursiveBatchLease{}
	ok := false
	defer func() {
		if !ok {
			leases.Release()
		}
	}()
	var bits [MaxStructDepth]Bitmap
	v, err := cloneStructListsTree(ctx, m, src, f, offset, length, nil, &bits, 0, leases)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return &OwnedRecursiveStruct{Vector: v, Bytes: leases.bytes, lease: leases}, nil
}

// CloneCollectBatchNestedLists owns a mixed batch containing checked array nodes.
// New root reservations are rolled back together on failure; input is borrowed.
func CloneCollectBatchNestedLists(ctx context.Context, m Manager, src *Batch, schema Schema) (*Batch, Reservation, error) {
	if ctx == nil || m == nil {
		return nil, nil, fmt.Errorf("array Collect requires context and manager")
	}
	if err := ValidateNestedListsBatch(src, schema, ctx.Err); err != nil {
		return nil, nil, err
	}
	charge, err := ownedRangeAdd(8192, int64(len(schema.Fields)), 2048)
	if err != nil {
		return nil, nil, err
	}
	r, err := m.Reserve(ctx, charge)
	if err != nil {
		return nil, nil, err
	}
	ls := &recursiveBatchLease{leases: []Reservation{r}, bytes: charge}
	ok := false
	var out *Batch
	defer func() {
		if !ok {
			if out != nil {
				out.ReleaseOwned()
			}
			ls.Release()
		}
	}()
	flat := *src
	flat.Arrays = nil
	flat.Structs = make(map[string]*StructVector, len(src.Structs))
	fields := make([]Field, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
		if !fieldHasNestedLists(f, 0) {
			fields = append(fields, f)
			if f.Type == TypeStruct {
				flat.Structs[f.Name] = src.Structs[f.Name]
			}
		}
	}
	var lease Reservation
	out, lease, err = CloneCollectBatchStructLists(ctx, m, &flat, Schema{Fields: fields})
	if err != nil {
		return nil, nil, err
	}
	if err = structListsAddLease(ls, lease); err != nil {
		return nil, nil, err
	}
	for _, f := range schema.Fields {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
		if IsNestedList(f) {
			v, e := CloneNestedListRangeOwned(ctx, m, src.Arrays[f.Name], f, NestedListParentDomain{}, 0, src.Rows)
			if e != nil {
				return nil, nil, e
			}
			out.Arrays[f.Name] = v.Vector
			if e = structListsAddLease(ls, v.lease); e != nil {
				return nil, nil, e
			}
			v.lease = nil
		} else if f.Type == TypeStruct && fieldHasNestedLists(f, 0) {
			v, e := CloneNestedListsStructRangeOwned(ctx, m, src.Structs[f.Name], f, 0, src.Rows)
			if e != nil {
				return nil, nil, e
			}
			out.Structs[f.Name] = v.Vector
			if e = structListsAddLease(ls, v.lease); e != nil {
				return nil, nil, e
			}
			v.lease = nil
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, nil, err
	}
	ok = true
	return out, ls, nil
}

// BatchHasArrays recognizes shared array bindings in a valid acyclic runtime
// graph. Like legacy clone/estimate APIs it imposes no depth limit or traversal
// allocation. Checked source admission must validate graphs before using it.
func BatchHasArrays(b *Batch) bool {
	if b == nil {
		return false
	}
	if len(b.Arrays) != 0 {
		return true
	}
	for _, v := range b.Structs {
		if v != nil && BatchHasArrays(v.Values) {
			return true
		}
	}
	for _, v := range b.StructLists {
		if v != nil && v.Values != nil && BatchHasArrays(v.Values.Values) {
			return true
		}
	}
	return false
}

func arraySnapshotStringBytes(f Field, check func() error) (int64, error) {
	if err := recursiveCheck(check); err != nil {
		return 0, err
	}
	var total int64
	var err error
	for _, s := range []string{f.Name, f.ArrowType, f.DictionaryDomain} {
		total, err = ownedRangeAdd(total, int64(len(s)), 1)
		if err != nil {
			return 0, err
		}
	}
	for _, c := range f.Children {
		n, e := arraySnapshotStringBytes(c, check)
		if e != nil {
			return 0, e
		}
		total, err = ownedRangeAdd(total, n, 1)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// ValidateArrayContainers checks the unified native container field profile.
func (f Field) ValidateArrayContainers() error { return checkArrayContainerField(f, 0, nil) }

// ValidateArrayContainersWithContext validates a Struct tree containing shared
// array nodes without cloning its schema or weakening hidden-storage checks.
func (v *StructVector) ValidateArrayContainersWithContext(f Field, rows int, check func() error) error {
	if v == nil || f.Type != TypeStruct {
		return fmt.Errorf("expected array container Struct")
	}
	if err := checkArrayContainerField(f, 0, check); err != nil {
		return err
	}
	if err := checkArrayBatchGraph(v.Values, f.Children, 1, check); err != nil {
		return err
	}
	var chain [MaxStructDepth]*StructVector
	return validateContainerVector(v, f, rows, check, &chain, 0, true)
}

// TakeNestedListsStructRowsOwned copies selected parent rows, including repeats.
// All storage is validated; required values are demanded only in selected rows.
func TakeNestedListsStructRowsOwned(ctx context.Context, m Manager, src *StructVector, f Field, rows []int) (*OwnedRecursiveStruct, error) {
	if ctx == nil || m == nil || src == nil || f.Type != TypeStruct {
		return nil, fmt.Errorf("invalid array Struct take")
	}
	if err := checkArrayContainerField(f, 0, ctx.Err); err != nil {
		return nil, err
	}
	if err := checkArrayBatchGraph(src.Values, f.Children, 1, ctx.Err); err != nil {
		return nil, err
	}
	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if r < 0 || r >= src.Rows {
			return nil, fmt.Errorf("array Struct selected row out of bounds")
		}
	}
	var chain [MaxStructDepth]*StructVector
	if err := validateContainerVector(src, f, src.Rows, ctx.Err, &chain, 0, true, rows); err != nil {
		return nil, err
	}
	leases := &recursiveBatchLease{}
	ok := false
	defer func() {
		if !ok {
			leases.Release()
		}
	}()
	var bits [MaxStructDepth]Bitmap
	out, err := cloneStructListsTree(ctx, m, src, f, 0, len(rows), rows, &bits, 0, leases)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return &OwnedRecursiveStruct{Vector: out, Bytes: leases.bytes, lease: leases}, nil
}
