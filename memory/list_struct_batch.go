package memory

import (
	"context"
	"fmt"
)

// IsStructList reports the explicit List<Struct> shape without traversing it.
func IsStructList(f Field) bool {
	return f.Type == TypeList && len(f.Children) == 1 && f.Children[0].Type == TypeStruct
}

// HasStructLists identifies the extended profile using a bounded walk. Admission
// must still use its checked schema validator before any recursive clone.
func (s Schema) HasStructLists() bool {
	for _, f := range s.Fields {
		if fieldHasStructLists(f, 0) {
			return true
		}
	}
	return false
}
func fieldHasStructLists(f Field, depth int) bool {
	if IsStructList(f) {
		return true
	}
	if depth >= MaxStructDepth {
		return false
	}
	if f.Type == TypeStruct {
		for _, c := range f.Children {
			if fieldHasStructLists(c, depth+1) {
				return true
			}
		}
	}
	return false
}

// ValidateStructListsBatch validates the extended profile including all ancestor
// domains and undeclared nested storage. The input and schema must stay immutable.
func ValidateStructListsBatch(b *Batch, s Schema, check func() error) error {
	for _, f := range s.Fields {
		if err := checkStructListTree(f, 0, check); err != nil {
			return err
		}
	}
	if err := checkStructListsGraph(b, s.Fields, 0, check); err != nil {
		return err
	}
	var chain [MaxStructDepth]*StructVector
	for _, f := range s.Fields {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if f.Type == TypeStruct {
			if err := validateStructListsVector(b.Structs[f.Name], f, b.Rows, check, &chain, 0); err != nil {
				return err
			}
		} else if IsStructList(f) {
			if err := b.StructLists[f.Name].ValidateWithContext(f, b.Rows, StructListParentDomain{}, check); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkStructListsGraph(b *Batch, fields []Field, depth int, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if b == nil || b.Rows < 0 || depth > MaxStructDepth {
		return fmt.Errorf("invalid extended native batch graph")
	}
	for name, v := range b.StructLists {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		found := false
		for _, f := range fields {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if f.Name == name && IsStructList(f) {
				found = true
				break
			}
		}
		if !found || v == nil {
			return fmt.Errorf("undeclared or nil StructList %q", name)
		}
	}
	for name, v := range b.Structs {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		found := false
		for _, f := range fields {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if f.Name == name && f.Type == TypeStruct {
				found = true
				if v == nil {
					return fmt.Errorf("nil Struct %q", name)
				}
				if err := checkStructListsGraph(v.Values, f.Children, depth+1, check); err != nil {
					return err
				}
				break
			}
		}
		if !found {
			return fmt.Errorf("undeclared Struct %q", name)
		}
	}
	return nil
}

func validateStructListsVector(v *StructVector, f Field, rows int, check func() error, ancestors *[MaxStructDepth]*StructVector, depth int) error {
	return validateContainerVector(v, f, rows, check, ancestors, depth, false)
}

func validateContainerVector(v *StructVector, f Field, rows int, check func() error, ancestors *[MaxStructDepth]*StructVector, depth int, arrays bool, demand ...[]int) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	if depth >= MaxStructDepth || rows < 0 || v == nil || v.Rows != rows || v.Values == nil || v.Values.Rows != rows || len(v.Fields) != len(f.Children) {
		return fmt.Errorf("recursive Struct %q shape mismatch", f.Name)
	}
	if !structValidityFits(v.Validity, rows) {
		return fmt.Errorf("Struct %q validity is too short", f.Name)
	}
	for i := range f.Children {
		equal, err := recursiveFieldEqual(v.Fields[i], f.Children[i], 0, check)
		if err != nil {
			return err
		}
		if !equal {
			return fmt.Errorf("Struct %q child metadata mismatch", f.Name)
		}
	}
	n := rows
	rowAt := func(i int) int { return i }
	if len(demand) > 0 {
		n = len(demand[0])
		rowAt = func(i int) int { return demand[0][i] }
	}
	visible := func(row int) bool {
		for i := 0; i < depth; i++ {
			if !ancestors[i].IsValid(row) {
				return false
			}
		}
		return true
	}
	for k := 0; k < n; k++ {
		row := rowAt(k)
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if visible(row) && !f.Nullable && !v.IsValid(row) {
			return fmt.Errorf("required Struct %q is NULL at row %d", f.Name, row)
		}
	}
	ancestors[depth] = v
	for _, child := range f.Children {
		if child.Type == TypeStruct {
			if err := validateContainerVector(v.Values.Structs[child.Name], child, rows, check, ancestors, depth+1, arrays, demand...); err != nil {
				return err
			}
			continue
		}
		if arrays && IsNestedList(child) {
			var bits [MaxStructDepth]Bitmap
			for i := 0; i <= depth; i++ {
				bits[i] = ancestors[i].Validity
			}
			domain, err := NewNestedListParentDomain(rows, bits[:depth+1]...)
			if err != nil {
				return err
			}
			node := v.Values.Arrays[child.Name]
			if err = node.validateNestedStructure(child, rows, domain, check); err != nil {
				return err
			}
			for k := 0; k < n; k++ {
				if err = node.validateNestedParent(child, domain, rowAt(k), check); err != nil {
					return err
				}
			}
			continue
		}
		if IsStructList(child) {
			var bits [MaxStructDepth]Bitmap
			for i := 0; i <= depth; i++ {
				bits[i] = ancestors[i].Validity
			}
			domain, err := NewStructListParentDomain(rows, bits[:depth+1]...)
			if err != nil {
				return err
			}
			node := v.Values.StructLists[child.Name]
			if err = node.validateStructure(child, rows, domain, check); err != nil {
				return err
			}
			for k := 0; k < n; k++ {
				if err = node.validateParent(child, domain, rowAt(k), check); err != nil {
					return err
				}
			}
			continue
		}
		if err := validateStructChildStorageContext(v.Values, child, rows, check); err != nil {
			return fmt.Errorf("child %q: %w", child.Name, err)
		}
		for k := 0; k < n; k++ {
			row := rowAt(k)
			if err := recursiveCheck(check); err != nil {
				return err
			}
			if !visible(row) || !v.IsValid(row) {
				continue
			}
			valid := batchFieldValid(v.Values, child, row)
			if !valid {
				if !child.Nullable {
					return fmt.Errorf("required child %q is NULL at row %d", child.Name, row)
				}
				continue
			}
			if child.Type == TypeString && v.Values.Strings[child.Name] == nil {
				d := v.Values.DictStrings[child.Name]
				if uint64(d.Indices[row]) >= uint64(d.DictionaryLen()) {
					return fmt.Errorf("child %q dictionary code out of range", child.Name)
				}
			}
			if child.Type == TypeList && !child.Children[0].Nullable {
				var start, end int
				var bitmap Bitmap
				if child.Children[0].Type == TypeInt64 {
					l := v.Values.Int64Lists[child.Name]
					start, end, _ = l.ValueRange(row)
					bitmap = l.Values.Validity
				} else {
					l := v.Values.StringLists[child.Name]
					start, end, _ = l.ValueRange(row)
					bitmap = l.Values.Validity
				}
				for e := start; e < end; e++ {
					if err := recursiveCheck(check); err != nil {
						return err
					}
					if bitmap != nil && !IsSet(bitmap, e) {
						return fmt.Errorf("child %q has required NULL element", child.Name)
					}
				}
			}
		}
	}
	return nil
}

// CloneStructListsStructRangeOwned owns a bounded Struct subtree containing
// StructLists. Parent-domain masks are retained in the reservation envelope;
// this conservative admission is not a minimum-payload estimate.
func CloneStructListsStructRangeOwned(ctx context.Context, m Manager, src *StructVector, f Field, offset, length int) (*OwnedRecursiveStruct, error) {
	if ctx == nil || m == nil || src == nil || offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return nil, fmt.Errorf("invalid extended Struct owned range")
	}
	if err := checkStructListTree(f, 0, ctx.Err); err != nil {
		return nil, err
	}
	if f.Type != TypeStruct {
		return nil, fmt.Errorf("expected Struct root")
	}
	if err := checkStructListsGraph(src.Values, f.Children, 1, ctx.Err); err != nil {
		return nil, err
	}
	var chain [MaxStructDepth]*StructVector
	if err := validateStructListsVector(src, f, src.Rows, ctx.Err, &chain, 0); err != nil {
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
	out, err := cloneStructListsTree(ctx, m, src, f, offset, length, nil, &bits, 0, leases)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	ok = true
	return &OwnedRecursiveStruct{Vector: out, Bytes: leases.bytes, lease: leases}, nil
}
func structListsAddLease(ls *recursiveBatchLease, r Reservation) error {
	n, err := ownedRangeAdd(ls.bytes, r.Bytes(), 1)
	if err != nil {
		r.Release()
		return err
	}
	ls.leases = append(ls.leases, r)
	ls.bytes = n
	return nil
}
func cloneStructListsTree(ctx context.Context, m Manager, src *StructVector, f Field, offset, length int, selected []int, bits *[MaxStructDepth]Bitmap, depth int, leases *recursiveBatchLease) (*StructVector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshotCharge, err := structListsSnapshotBytes(f, ctx.Err)
	if err != nil {
		return nil, err
	}
	if fieldHasNestedLists(f, 0) {
		names, e := arraySnapshotStringBytes(f, ctx.Err)
		if e != nil {
			return nil, e
		}
		snapshotCharge, e = ownedRangeAdd(snapshotCharge, names, 1)
		if e != nil {
			return nil, e
		}
	}
	charge, err := ownedRangeAdd(8192, snapshotCharge, 1)
	if err != nil {
		return nil, err
	}
	charge, err = ownedRangeAdd(charge, int64(WordsFor(src.Rows)+WordsFor(length)), 8)
	if err != nil {
		return nil, err
	}
	r, err := m.Reserve(ctx, charge)
	if err != nil {
		return nil, err
	}
	if err = structListsAddLease(leases, r); err != nil {
		return nil, err
	}
	bits[depth] = src.Validity
	mask := make(Bitmap, WordsFor(src.Rows))
	for row := 0; row < src.Rows; row++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		valid := true
		for i := 0; i <= depth; i++ {
			if bits[i] != nil && !IsSet(bits[i], row) {
				valid = false
				break
			}
		}
		if valid {
			Set(mask, row)
		}
	}
	snapshot, err := cloneRecursiveFieldContext(f, ctx.Err)
	if err != nil {
		return nil, err
	}
	out := &StructVector{Rows: length, Fields: snapshot.Children, Values: NewBatch(), Validity: make(Bitmap, WordsFor(length)), Encoding: EncodingView}
	out.Values.Rows = length
	for i := 0; i < length; i++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		row := offset + i
		if selected != nil {
			row = selected[i]
		}
		if IsSet(mask, row) {
			Set(out.Validity, i)
		}
	}
	flatFields := make([]Field, 0, len(f.Children))
	for _, c := range f.Children {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if c.Type != TypeStruct && !IsStructList(c) && !IsNestedList(c) {
			flatFields = append(flatFields, c)
		}
	}
	if len(flatFields) > 0 {
		flatField := Field{Name: f.Name, Type: TypeStruct, Nullable: true, Encoding: EncodingView, Children: flatFields}
		view := *src
		view.Fields = flatFields
		view.Validity = mask
		var flat *OwnedRecursiveStruct
		var err error
		if selected != nil {
			flat, err = TakeRecursiveStructRowsOwned(ctx, m, &view, flatField, selected)
		} else {
			flat, err = CloneRecursiveStructRangeOwned(ctx, m, &view, flatField, offset, length)
		}
		if err != nil {
			return nil, err
		}
		out.Values = flat.Vector.Values
		if err = structListsAddLease(leases, flat.lease); err != nil {
			return nil, err
		}
		flat.lease = nil
	}
	for _, c := range f.Children {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if c.Type == TypeStruct {
			v, e := cloneStructListsTree(ctx, m, src.Values.Structs[c.Name], c, offset, length, selected, bits, depth+1, leases)
			if e != nil {
				return nil, e
			}
			out.Values.Structs[c.Name] = v
		}
		if IsNestedList(c) {
			domain, e := NewNestedListParentDomain(src.Rows, bits[:depth+1]...)
			if e != nil {
				return nil, e
			}
			var v *OwnedNestedList
			if selected != nil {
				v, e = TakeNestedListRowsOwned(ctx, m, src.Values.Arrays[c.Name], c, domain, selected)
			} else {
				v, e = CloneNestedListRangeOwned(ctx, m, src.Values.Arrays[c.Name], c, domain, offset, length)
			}
			if e != nil {
				return nil, e
			}
			out.Values.Arrays[c.Name] = v.Vector
			if e = structListsAddLease(leases, v.lease); e != nil {
				return nil, e
			}
			v.lease = nil
		}
		if IsStructList(c) {
			domain, e := NewStructListParentDomain(src.Rows, bits[:depth+1]...)
			if e != nil {
				return nil, e
			}
			var v *OwnedStructList
			if selected != nil {
				v, e = TakeStructListRowsOwned(ctx, m, src.Values.StructLists[c.Name], c, domain, selected)
			} else {
				v, e = CloneStructListRangeOwned(ctx, m, src.Values.StructLists[c.Name], c, domain, offset, length)
			}
			if e != nil {
				return nil, e
			}
			out.Values.StructLists[c.Name] = v.Vector
			if e = structListsAddLease(leases, v.lease); e != nil {
				return nil, e
			}
			v.lease = nil
		}
	}
	return out, nil
}

// CloneCollectBatchStructLists owns a batch in the explicit extended profile.
// Each root remains charged until the returned reservation is released. Failure
// releases every newly admitted root; caller-owned input is never modified.
func CloneCollectBatchStructLists(ctx context.Context, m Manager, src *Batch, schema Schema) (*Batch, Reservation, error) {
	if ctx == nil || m == nil {
		return nil, nil, fmt.Errorf("extended Collect requires context and manager")
	}
	if err := ValidateStructListsBatch(src, schema, ctx.Err); err != nil {
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
	defer func() {
		if !ok {
			ls.Release()
		}
	}()
	flat := *src
	flat.StructLists = nil
	flat.Structs = make(map[string]*StructVector, len(src.Structs))
	flatFields := make([]Field, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
		if !fieldHasStructLists(f, 0) {
			flatFields = append(flatFields, f)
			if f.Type == TypeStruct {
				flat.Structs[f.Name] = src.Structs[f.Name]
			}
		}
	}
	out, lease, err := CloneCollectBatchRecursive(ctx, m, &flat, Schema{Fields: flatFields})
	if err != nil {
		return nil, nil, err
	}
	if err = structListsAddLease(ls, lease); err != nil {
		out.ReleaseOwned()
		return nil, nil, err
	}
	defer func() {
		if !ok {
			out.ReleaseOwned()
		}
	}()
	for _, f := range schema.Fields {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
		if IsStructList(f) {
			v, e := CloneStructListRangeOwned(ctx, m, src.StructLists[f.Name], f, StructListParentDomain{}, 0, src.Rows)
			if e != nil {
				return nil, nil, e
			}
			out.StructLists[f.Name] = v.Vector
			if e = structListsAddLease(ls, v.lease); e != nil {
				return nil, nil, e
			}
			v.lease = nil
		} else if f.Type == TypeStruct && fieldHasStructLists(f, 0) {
			v, e := CloneStructListsStructRangeOwned(ctx, m, src.Structs[f.Name], f, 0, src.Rows)
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

// ValidateStructLists checks a field in the explicit extended native profile.
func (f Field) ValidateStructLists() error { return checkStructListTree(f, 0, nil) }

// ValidateStructListsWithContext validates an extended Struct root with all
// ancestor domains, including parent-to-element domain transitions.
func (v *StructVector) ValidateStructListsWithContext(f Field, rows int, check func() error) error {
	if f.Type != TypeStruct {
		return fmt.Errorf("expected Struct")
	}
	if err := checkStructListTree(f, 0, check); err != nil {
		return err
	}
	if v == nil {
		return fmt.Errorf("nil Struct")
	}
	if err := checkStructListsGraph(v.Values, f.Children, 1, check); err != nil {
		return err
	}
	var chain [MaxStructDepth]*StructVector
	return validateStructListsVector(v, f, rows, check, &chain, 0)
}

func structListsSnapshotBytes(f Field, check func() error) (int64, error) {
	if err := recursiveCheck(check); err != nil {
		return 0, err
	}
	total := int64(2048)
	for _, c := range f.Children {
		n, err := structListsSnapshotBytes(c, check)
		if err != nil {
			return 0, err
		}
		total, err = ownedRangeAdd(total, n, 1)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// TakeStructListsStructRowsOwned copies a parent-row permutation. All storage
// is checked; required values apply only to selected rows in the full ancestor
// domain. This internal owned gather does not enable selected expressions.
func TakeStructListsStructRowsOwned(ctx context.Context, m Manager, src *StructVector, f Field, rows []int) (*OwnedRecursiveStruct, error) {
	if ctx == nil || m == nil || src == nil {
		return nil, fmt.Errorf("invalid extended Struct Take")
	}
	if err := checkStructListTree(f, 0, ctx.Err); err != nil {
		return nil, err
	}
	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if r < 0 || r >= src.Rows {
			return nil, fmt.Errorf("extended Take row out of range")
		}
	}
	if f.Type != TypeStruct {
		return nil, fmt.Errorf("expected Struct root")
	}
	if err := checkStructListsGraph(src.Values, f.Children, 1, ctx.Err); err != nil {
		return nil, err
	}
	wrapper := Batch{Rows: src.Rows, Structs: map[string]*StructVector{f.Name: src}}
	if _, err := selectionRunFieldSize(&wrapper, f, rows, columnarSelectionDomain{check: ctx.Err}); err != nil {
		return nil, err
	}
	// Non-nil empty selects zero rows rather than the contiguous branch.
	if rows == nil {
		rows = []int{}
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

// BatchHasStructLists recognizes an extended runtime shape with a bounded walk.
// A true result does not replace full schema/storage validation.
func BatchHasStructLists(b *Batch) bool { return batchHasStructLists(b, 0) }
func batchHasStructLists(b *Batch, depth int) bool {
	if b == nil {
		return false
	}
	if len(b.StructLists) > 0 {
		return true
	}
	if depth >= MaxStructDepth {
		return false
	}
	for _, v := range b.Structs {
		if v != nil && batchHasStructLists(v.Values, depth+1) {
			return true
		}
	}
	return false
}
