package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func structListBatchTestFixture(t *testing.T, depth int) (Schema, *Batch) {
	t.Helper()
	f, v, _ := listStructTestFixture(t)
	child := NewBatch()
	child.Rows = 7
	child.StructLists[f.Name] = v
	field := f
	for i := 0; i < depth; i++ {
		field = StructField("inner", true, field)
		node := &StructVector{Rows: 7, Fields: field.Children, Values: child, Encoding: EncodingView}
		if i == depth-1 {
			node.Validity = Bitmap{95}
		}
		child = NewBatch()
		child.Rows = 7
		child.Structs["inner"] = node
	}
	b := NewBatch()
	b.Rows = 7
	if depth > 0 {
		field.Name = "root"
		b.Structs["root"] = child.Structs["inner"]
	} else {
		b.StructLists[f.Name] = v
		v.Validity = Bitmap{94}
	}
	other, ov, _ := listStructTestFixture(t)
	other.Name = "other"
	ov.Validity = Bitmap{94}
	b.StructLists["other"] = ov
	b.Int64s["id"] = &Int64Vector{Values: []int64{100, 101, 102, 103, 104, 105, 106}}
	s, e := NewSchemaWithStructListsChecked(field, other, Field{Name: "id", Type: TypeInt64})
	if e != nil {
		t.Fatal(e)
	}
	return s, b
}
func structListBatchCheck(t *testing.T, b *Batch, s Schema, depth int) {
	t.Helper()
	if b.Rows != 7 || !reflect.DeepEqual(b.Int64s["id"].Values, []int64{100, 101, 102, 103, 104, 105, 106}) {
		t.Fatal("flat sibling")
	}
	current := b
	field := s.Fields[0]
	if depth > 0 {
		node := b.Structs["root"]
		for i := 0; i < depth; i++ {
			if node.Rows != 7 || node.Values.Rows != 7 {
				t.Fatal("ancestor row shape")
			}
			if i == 0 && node.IsValid(5) {
				t.Fatal("ancestor NULL lost")
			}
			current = node.Values
			field = field.Children[0]
			if i+1 < depth {
				node = current.Structs["inner"]
			}
		}
	}
	listStructTestCheck(t, &OwnedStructList{Vector: current.StructLists["items"], Field: field}, []int{0, 1, 2, 3, 4, 5, 6})
	listStructTestCheck(t, &OwnedStructList{Vector: b.StructLists["other"], Field: s.Fields[1]}, []int{0, 1, 2, 3, 4, 5, 6})
}
func TestStructListBatchCollectOwnedAllFieldsAndAncestors(t *testing.T) {
	for _, depth := range []int{0, 1, 7} {
		t.Run(string(rune('0'+depth)), func(t *testing.T) {
			s, b := structListBatchTestFixture(t, depth)
			if !s.HasStructLists() {
				t.Fatal("profile detection")
			}
			m := NewBudgetManager(0)
			out, lease, e := CloneCollectBatchStructLists(context.Background(), m, b, s)
			if e != nil {
				t.Fatal(e)
			}
			structListBatchCheck(t, out, s, depth)
			charge := lease.Bytes()
			if charge <= 0 || m.Used() != charge {
				t.Fatal("merged charge", charge, m.Used())
			}
			// The owned batch is independent of both element data and ancestor masks.
			b.Int64s["id"].Values[0] = -1
			b.StructLists["other"].Values.Values.Int64s["int64"].Values[3] = 123
			current := b
			if depth > 0 {
				node := b.Structs["root"]
				node.Validity[0] = 0
				for i := 1; i < depth; i++ {
					node = node.Values.Structs["inner"]
				}
				current = node.Values
			}
			current.StructLists["items"].Values.Values.Strings["string"].Data[0] = '!'
			structListBatchCheck(t, out, s, depth)
			out.ReleaseOwned()
			lease.Release()
			lease.Release()
			if m.Used() != 0 {
				t.Fatal("owned batch cleanup")
			}
			s, b = structListBatchTestFixture(t, depth)
			exact := NewBudgetManager(charge)
			out, lease, e = CloneCollectBatchStructLists(context.Background(), exact, b, s)
			if e != nil {
				t.Fatal("exact batch charge", e)
			}
			out.ReleaseOwned()
			lease.Release()
			short := NewBudgetManager(charge - 1)
			out, lease, e = CloneCollectBatchStructLists(context.Background(), short, b, s)
			if out != nil || lease != nil || !errors.Is(e, ErrBudgetExceeded) || short.Used() != 0 {
				t.Fatal("batch one byte short", out, lease, e, short.Used())
			}
		})
	}
}
func TestStructListBatchCollectMultiOwnerRollback(t *testing.T) {
	s, b := structListBatchTestFixture(t, 7)
	probe := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	out, lease, e := CloneCollectBatchStructLists(context.Background(), probe, b, s)
	if e != nil {
		t.Fatal(e)
	}
	calls := probe.calls
	out.ReleaseOwned()
	lease.Release()
	if calls < 2 {
		t.Fatal("expected independently admitted owner/setup paths", calls)
	}
	for fail := 1; fail <= calls; fail++ {
		m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: fail}
		out, lease, e := CloneCollectBatchStructLists(context.Background(), m, b, s)
		if out != nil || lease != nil || !errors.Is(e, ErrBudgetExceeded) || m.Used() != 0 {
			t.Fatal("multi-root rollback", fail, out, lease, e, m.Used())
		}
		m.failAt = 0
		out, lease, e = CloneCollectBatchStructLists(context.Background(), m, b, s)
		if e != nil {
			t.Fatal(e)
		}
		structListBatchCheck(t, out, s, 7)
		out.ReleaseOwned()
		lease.Release()
		if m.Used() != 0 {
			t.Fatal("retry leak")
		}
	}
	for at := 1; at <= calls; at++ {
		ctx, cancel := context.WithCancel(context.Background())
		m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), cancelAt: at, cancel: cancel}
		out, lease, e := CloneCollectBatchStructLists(ctx, m, b, s)
		cancel()
		if out != nil || lease != nil || !errors.Is(e, context.Canceled) || m.Used() != 0 {
			t.Fatal("multi-root cancellation", at, out, lease, e, m.Used())
		}
	}
}

func TestStructListBatchNewMapCannotHideInsideFlatElement(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		t.Run(map[bool]string{false: "undeclared", true: "cycle"}[cycle], func(t *testing.T) {
			f, v, d := listStructTestFixture(t)
			extra := &StructListVector{Offsets: []uint32{0}, Values: &StructVector{Values: NewBatch()}}
			if cycle {
				extra = v
			}
			v.Values.Values.StructLists["hidden"] = extra
			if e := v.ValidateWithContext(f, 7, d, nil); e == nil {
				t.Fatal("flat element admitted extra StructLists")
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			for _, rows := range [][]int{nil, {6}} {
				out, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, rows)
				if out != nil || e == nil || m.calls != 0 || m.Used() != 0 {
					t.Fatal("unselected/hidden new map bypassed structure", out, e, m.calls, m.Used())
				}
			}
		})
	}
}

func TestStructListBatchValidationAndCodecSupport(t *testing.T) {
	for _, depth := range []int{0, 7} {
		s, b := structListBatchTestFixture(t, depth)
		if e := ValidateStructListsBatch(b, s, nil); e != nil {
			t.Fatal("extended validation", e)
		}
		m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
		run, e := EncodeColumnarRun(context.Background(), s, b, m)
		if run == nil || e != nil || m.calls == 0 || m.Used() == 0 {
			t.Fatal("C codec support", run, e, m.calls, m.Used())
		}
		run.Close()
		if m.Used() != 0 {
			t.Fatal("C codec cleanup")
		}
		if _, e = EstimateColumnarRunSelectionBytes(s, b, []int{0}); e != nil {
			t.Fatal("C selection support", e)
		}
	}
	for _, kind := range []string{"missing", "undeclared", "bad_hidden", "cycle"} {
		t.Run(kind, func(t *testing.T) {
			s, b := structListBatchTestFixture(t, 7)
			switch kind {
			case "missing":
				delete(b.StructLists, "other")
			case "undeclared":
				b.StructLists["extra"] = b.StructLists["other"]
			case "bad_hidden":
				b.StructLists["other"].Offsets[1] = 2
			case "cycle":
				b.Structs["root"].Values.Structs["unexpected"] = b.Structs["root"]
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			out, lease, e := CloneCollectBatchStructLists(context.Background(), m, b, s)
			if out != nil || lease != nil || e == nil || m.calls != 0 || m.Used() != 0 {
				t.Fatal("bad batch structure must fail before charge", out, lease, e, m.calls, m.Used())
			}
		})
	}
}

func TestStructListBatchLegacyCloneCannotSilentlyDrop(t *testing.T) {
	_, b := structListBatchTestFixture(t, 0)
	for name, fn := range map[string]func() (*Batch, error){"whole": func() (*Batch, error) { return CloneBatch(b) }, "range": func() (*Batch, error) { return CloneBatchRange(b, 0, b.Rows) }} {
		t.Run(name, func(t *testing.T) {
			out, err := fn()
			if out != nil {
				defer out.ReleaseOwned()
			}
			if err == nil || out != nil {
				t.Fatal("legacy clone silently dropped StructLists", err)
			}
		})
	}
}

func TestStructListBatchDirectStructOwnerAdmission(t *testing.T) {
	s, b := structListBatchTestFixture(t, 7)
	f, v := s.Fields[0], b.Structs["root"]
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: 1}
	out, e := CloneStructListsStructRangeOwned(context.Background(), m, v, f, 0, 7)
	if out != nil || !errors.Is(e, ErrBudgetExceeded) || m.calls != 1 || m.Used() != 0 {
		t.Fatal("first admission", out, e, m.calls, m.Used())
	}
	m.failAt = 0
	out, e = CloneStructListsStructRangeOwned(context.Background(), m, v, f, 0, 7)
	if e != nil {
		t.Fatal(e)
	}
	original := out.Vector.Fields[0].Name
	f.Children[0].Name = "poison"
	if out.Vector.Fields[0].Name != original {
		t.Fatal("owned metadata aliases source")
	}
	out.Close()
	if m.Used() != 0 {
		t.Fatal("direct owner leak")
	}
	for _, kind := range []string{"extra", "cycle"} {
		t.Run(kind, func(t *testing.T) {
			s, b := structListBatchTestFixture(t, 7)
			v := b.Structs["root"]
			if kind == "cycle" {
				v.Values.Structs["cycle"] = v
			} else {
				v.Values.StructLists["extra"] = b.StructLists["other"]
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			out, e := CloneStructListsStructRangeOwned(context.Background(), m, v, s.Fields[0], 0, 0)
			if out != nil || e == nil || m.calls != 0 || m.Used() != 0 {
				t.Fatal("direct empty range bypassed graph", out, e, m.calls, m.Used())
			}
		})
	}
}

func TestStructListBatchElementHelperRejectExtraStorage(t *testing.T) {
	for _, kind := range []string{"extra_scalar", "constant", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			f, v, _ := listStructTestFixture(t)
			switch kind {
			case "extra_scalar":
				v.Values.Values.Int64s["extra"] = &Int64Vector{Values: make([]int64, v.Values.Rows)}
			case "constant":
				v.Values.Encoding = EncodingConstant
			case "unknown":
				v.Values.Encoding = Encoding(255)
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			out, e := TakeNullableStructListElementsOwned(context.Background(), m, v.Values, f.Children[0], nil)
			if out != nil || e == nil || m.calls != 0 || m.Used() != 0 {
				t.Fatal("invalid flat element before reserve", out, e, m.calls, m.Used())
			}
		})
	}
}
func TestStructListBatchArrowOffsetArithmetic(t *testing.T) {
	const max = uint64(1<<31 - 1)
	for _, c := range []struct {
		a, b, w uint64
		bad     bool
	}{{0, 0, 0, false}, {max, 0, max, false}, {max - 1, 1, max, false}, {0, max, max, false}, {max, 1, 0, true}, {max + 1, 0, 0, true}, {0, max + 1, 0, true}, {^uint64(0), 1, 0, true}, {1, ^uint64(0), 0, true}} {
		got, e := checkedStructListArrowOffset(c.a, c.b)
		if (e != nil) != c.bad || (!c.bad && got != c.w) {
			t.Fatal(c, got, e)
		}
	}
}
