package expr

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

func structConstructExtendedField(f memory.Field) bool {
	return f.Type == memory.TypeStruct || memory.IsStructList(f) || memory.IsNestedList(f)
}
func structConstructExtendedFields(fs []memory.Field) bool {
	for _, f := range fs {
		if structConstructExtendedField(f) {
			return true
		}
	}
	return false
}

type structConstructOwners struct {
	leases []memory.Reservation
	bytes  int64
}

func (o *structConstructOwners) Bytes() int64 { return o.bytes }
func (o *structConstructOwners) Release() {
	for _, l := range o.leases {
		l.Release()
	}
	o.leases = nil
	o.bytes = 0
}
func (o *structConstructOwners) add(l memory.Reservation) error {
	if l.Bytes() < 0 || l.Bytes() > math.MaxInt64-o.bytes {
		l.Release()
		return fmt.Errorf("STRUCT_FROM owner bytes overflow")
	}
	o.bytes += l.Bytes()
	o.leases = append(o.leases, l)
	return nil
}

// Admission covers deep parent snapshots and owner bookkeeping independently
// of each child's owned payload charge. Legacy-only construction is unchanged.
func structConstructMetadata(f memory.Field, depth int, check func() error) (int64, error) {
	if check != nil {
		if err := check(); err != nil {
			return 0, err
		}
	}
	if depth > memory.MaxStructDepth+memory.MaxNestedListDepth {
		return 0, fmt.Errorf("STRUCT_FROM metadata depth")
	}
	n := int64(1024)
	if !addJSONAllocation(&n, int64(len(f.Name)+len(f.DictionaryDomain)+len(f.ArrowType)), 1) {
		return 0, fmt.Errorf("STRUCT_FROM metadata overflow")
	}
	for _, c := range f.Children {
		v, e := structConstructMetadata(c, depth+1, check)
		if e != nil {
			return 0, e
		}
		if v > math.MaxInt64-n {
			return 0, fmt.Errorf("STRUCT_FROM metadata overflow")
		}
		n += v
	}
	return n, nil
}
func structConstructSnapshot(f memory.Field, check func() error) (memory.Field, error) {
	if check != nil {
		if err := check(); err != nil {
			return memory.Field{}, err
		}
	}
	out := f
	out.Children = make([]memory.Field, len(f.Children))
	for i, c := range f.Children {
		v, e := structConstructSnapshot(c, check)
		if e != nil {
			return memory.Field{}, e
		}
		out.Children[i] = v
	}
	return out, nil
}

func evalStructFromExtended(out *scalarRegister, regs []*scalarRegister, inputs []int, fields []memory.Field, rows int, active memory.Bitmap) error {
	check := out.checkContext
	if rows < 0 || rows == math.MaxInt || len(fields) != len(inputs) || active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("STRUCT_FROM extended shape")
	}
	// Full parent admission counts the additional enclosing Struct level.
	parent := memory.Field{Name: "expression", Type: memory.TypeStruct, Children: fields}
	if err := parent.ValidateArrayContainers(); err != nil {
		return err
	}
	meta, err := structConstructMetadata(parent, 0, check)
	if err != nil {
		return err
	}
	if !addJSONAllocation(&meta, int64(len(fields)), 1024) {
		return fmt.Errorf("STRUCT_FROM metadata overflow")
	}
	lease, err := out.acquire(meta)
	if err != nil {
		return err
	}
	owners := &structConstructOwners{leases: make([]memory.Reservation, 0, len(fields)+2)}
	if err = owners.add(lease); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			owners.Release()
		}
	}()
	// This scratch bitmap is reused only after each synchronous owned copy ends.
	scratchBytes := int64(1024)
	if !addJSONAllocation(&scratchBytes, int64(memory.WordsFor(rows)), 8) {
		return fmt.Errorf("STRUCT_FROM mask overflow")
	}
	scratch, err := out.acquire(scratchBytes)
	if err != nil {
		return err
	}
	defer scratch.Release()
	effective := make(memory.Bitmap, memory.WordsFor(rows))
	flatFields := make([]memory.Field, 0, len(fields))
	flatInputs := make([]int, 0, len(fields))
	for i, f := range fields {
		if inputs[i] < 0 || inputs[i] >= len(regs) || regs[inputs[i]] == nil {
			return fmt.Errorf("STRUCT_FROM invalid register")
		}
		if !structConstructExtendedField(f) {
			flatFields = append(flatFields, f)
			flatInputs = append(flatInputs, inputs[i])
		}
	}
	// Private flat result is never published into out. Its complete owner is
	// retained alongside every child until the single final commit.
	temp := scalarRegister{acquire: out.acquire, checkContext: check}
	if err = evalStructFrom(&temp, regs, flatInputs, flatFields, rows, active); err != nil {
		return err
	}
	if err = owners.add(temp.structFromReservation); err != nil {
		return err
	}
	result := temp.strct
	result.Fields = make([]memory.Field, len(fields))
	ctx := recursiveCopyContext{check}
	manager := recursiveCopyManager{out.acquire}
	for i, f := range fields {
		snap, e := structConstructSnapshot(f, check)
		if e != nil {
			return e
		}
		result.Fields[i] = snap
		if !structConstructExtendedField(f) {
			continue
		}
		r := regs[inputs[i]]
		if r.typ != f.Type || r.validity != nil && len(r.validity) < memory.WordsFor(rows) {
			return fmt.Errorf("STRUCT_FROM container register mismatch")
		}
		var vectorValid func(int) bool
		var vectorMask memory.Bitmap
		switch {
		case f.Type == memory.TypeStruct:
			if r.strct == nil || r.strct.Rows != rows {
				return fmt.Errorf("STRUCT_FROM missing Struct")
			}
			vectorValid = r.strct.IsValid
			vectorMask = r.strct.Validity
		case memory.IsStructList(f):
			if r.structList == nil || r.structList.Len() != rows {
				return fmt.Errorf("STRUCT_FROM missing StructList")
			}
			vectorValid = r.structList.IsValid
			vectorMask = r.structList.Validity
		default:
			if r.array == nil || r.array.Len() != rows {
				return fmt.Errorf("STRUCT_FROM missing nested List")
			}
			vectorValid = r.array.IsValid
			vectorMask = r.array.Validity
		}
		if vectorMask != nil && len(vectorMask) < memory.WordsFor(rows) {
			return fmt.Errorf("STRUCT_FROM short container validity")
		}
		clear(effective)
		for row := 0; row < rows; row++ {
			if e = ctx.Err(); e != nil {
				return e
			}
			if active != nil && !memory.IsSet(active, row) {
				continue
			}
			valid := isValidRegister(r, row) && vectorValid(row)
			if !valid && !f.Nullable {
				return fmt.Errorf("STRUCT_FROM required container %q is NULL", f.Name)
			}
			if valid {
				memory.Set(effective, row)
			}
		}
		visible := f
		visible.Nullable = true
		switch {
		case f.Type == memory.TypeStruct:
			view := *r.strct
			view.Validity = effective
			clone := memory.CloneRecursiveStructRangeOwned
			schema := memory.Schema{Fields: []memory.Field{f}}
			if schema.HasNestedLists() {
				clone = memory.CloneNestedListsStructRangeOwned
			} else if schema.HasStructLists() {
				clone = memory.CloneStructListsStructRangeOwned
			}
			owned, e := clone(ctx, manager, &view, visible, 0, rows)
			if e != nil {
				return e
			}
			if e = owners.add(&recursiveStructLease{owned}); e != nil {
				return e
			}
			result.Values.Structs[f.Name] = owned.Vector
		case memory.IsStructList(f):
			view := *r.structList
			view.Validity = effective
			owned, e := memory.CloneStructListRangeOwned(ctx, manager, &view, visible, memory.StructListParentDomain{}, 0, rows)
			if e != nil {
				return e
			}
			if e = owners.add(&structListLease{owned}); e != nil {
				return e
			}
			result.Values.StructLists[f.Name] = owned.Vector
		default:
			view := *r.array
			view.Validity = effective
			owned, e := memory.CloneNestedListRangeOwned(ctx, manager, &view, visible, memory.NestedListParentDomain{}, 0, rows)
			if e != nil {
				return e
			}
			if e = owners.add(&nestedArrayLease{owned}); e != nil {
				return e
			}
			result.Values.Arrays[f.Name] = owned.Vector
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	previous := out.structFromReservation
	out.strct, out.validity = result, result.Validity
	out.structFromReservation, out.structFromBytes = owners, owners.Bytes()
	committed = true
	if previous != nil {
		previous.Release()
	}
	return nil
}
