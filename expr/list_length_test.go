package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"strings"
	"testing"
)

func listLengthInput(kind memory.DataType, n int) (memory.Schema, *memory.Batch) {
	b := memory.NewBatch()
	b.Rows = n
	offsets := make([]uint32, n+1)
	valid := make(memory.Bitmap, memory.WordsFor(n))
	for i := 0; i < n; i++ {
		offsets[i+1] = offsets[i] + uint32(i%4)
		if i%7 != 0 {
			memory.Set(valid, i)
		}
	}
	children := int(offsets[n])
	cv := make(memory.Bitmap, memory.WordsFor(children))
	for i := 0; i < children; i++ {
		if i%3 != 0 {
			memory.Set(cv, i)
		}
	}
	if kind == memory.TypeInt64 {
		b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: offsets, Validity: valid, Values: &memory.Int64Vector{Values: make([]int64, children), Validity: cv}}
		return memory.NewSchema(memory.ListInt64Field("xs", true, true)), b
	}
	b.StringLists["xs"] = &memory.StringListVector{Offsets: offsets, Validity: valid, Values: &memory.StringViewVector{Offsets: make([]uint32, children+1), Validity: cv}}
	return memory.NewSchema(memory.ListStringField("xs", true, true)), b
}
func TestListLengthSemanticsAndReuse(t *testing.T) {
	for _, kind := range []memory.DataType{memory.TypeString, memory.TypeInt64} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%d", kind, mode), func(t *testing.T) {
				schema, _ := listLengthInput(kind, 0)
				p, err := Compile(schema, ListLength(Col("xs")))
				if err != nil {
					t.Fatal(err)
				}
				if p.SupportsSelection() {
					t.Fatal("nested input must keep selection fallback")
				}
				manager := memory.NewBudgetManager(0)
				w, err := NewWorkspace(context.Background(), p, manager)
				if err != nil {
					t.Fatal(err)
				}
				for _, n := range []int{130, 0, 1, 65, 3} {
					_, b := listLengthInput(kind, n)
					got, err := w.Evaluate(context.Background(), b, kernel.New(mode))
					if err != nil {
						t.Fatal(err)
					}
					v := got[0].Int64
					if len(v.Values) != n {
						t.Fatal("wrong rows")
					}
					for i := 0; i < n; i++ {
						if v.IsValid(i) != (i%7 != 0) || v.IsValid(i) && v.Values[i] != int64(i%4) {
							t.Fatalf("row%d: %+v", i, v)
						}
					}
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 {
					t.Fatal("leak")
				}
			})
		}
	}
}
func TestListLengthBindingAndComposition(t *testing.T) {
	for _, f := range []memory.Field{{Name: "x", Type: memory.TypeInt64}, {Name: "x", Type: memory.TypeString}, {Name: "x", Type: memory.TypeArrow}, {Name: "x", Type: memory.TypeList, Children: []memory.Field{{Name: "item", Type: memory.TypeFloat64}}}, {Name: "x", Type: memory.TypeList, Children: []memory.Field{{Name: "item", Type: memory.TypeInt64, Children: []memory.Field{{Name: "bad", Type: memory.TypeInt64}}}}}} {
		if _, err := Compile(memory.NewSchema(f), ListLength(Col("x"))); err == nil {
			t.Fatal("accepted", f)
		}
	}
	schema, b := listLengthInput(memory.TypeInt64, 3)
	if _, err := Compile(schema, Length(Col("xs"))); err == nil {
		t.Fatal("string Length changed")
	}
	for _, e := range []Expr{ListLength(Split(Str("a,,b"), Str(","))), If(Bool(true), ListLength(Split(Str("a,,b"), Str(","))), I64(9)), Case([]WhenThen{{When: Bool(true), Then: ListLength(Split(Str("a,,b"), Str(",")))}}, I64(9))} {
		p, err := Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		got, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range got[0].Int64.Values {
			if v != 3 {
				t.Fatal(v)
			}
		}
		w.Close()
	}
	// NULL masking applies to the parent; invalid row-value work in an inactive
	// branch is skipped before producing a list (the list length itself is total).
	e := If(Bool(false), ListLength(Split(CastTo(CastTo(Str("not integer"), memory.TypeInt64), memory.TypeString), Str(","))), I64(5))
	p, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	got, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil || got[0].Int64.Values[0] != 5 {
		t.Fatal(err)
	}
}
func TestListLengthPrefixAndStorageValidation(t *testing.T) {
	for _, kind := range []memory.DataType{memory.TypeString, memory.TypeInt64} {
		schema, b := listLengthInput(kind, 5)
		b.Rows = 2 // physical tail remains valid and unread by result.
		p, err := Compile(schema, ListLength(Col("xs")))
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		got, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil || len(got[0].Int64.Values) != 2 {
			t.Fatal(err)
		}
		w.Close()
		for _, bad := range []string{"decreasing", "terminal", "childvalidity", "nilchild", "wrongstorage"} {
			t.Run(fmt.Sprintf("%s/%s", kind, bad), func(t *testing.T) {
				_, b := listLengthInput(kind, 65)
				var offsets []uint32
				if kind == memory.TypeInt64 {
					v := b.Int64Lists["xs"]
					offsets = v.Offsets
					switch bad {
					case "childvalidity":
						v.Values.Validity = memory.Bitmap{}
					case "nilchild":
						v.Values = nil
					case "wrongstorage":
						delete(b.Int64Lists, "xs")
						_, other := listLengthInput(memory.TypeString, 65)
						b.StringLists["xs"] = other.StringLists["xs"]
					}
				} else {
					v := b.StringLists["xs"]
					offsets = v.Offsets
					switch bad {
					case "childvalidity":
						v.Values.Validity = memory.Bitmap{}
					case "nilchild":
						v.Values = nil
					case "wrongstorage":
						delete(b.StringLists, "xs")
						_, other := listLengthInput(memory.TypeInt64, 65)
						b.Int64Lists["xs"] = other.Int64Lists["xs"]
					}
				}
				if bad == "decreasing" {
					offsets[3] = 0
				}
				if bad == "terminal" {
					offsets[65] = ^uint32(0)
				}
				manager := memory.NewBudgetManager(0)
				w, _ := NewWorkspace(context.Background(), p, manager)
				_, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err == nil || !strings.Contains(err.Error(), "LIST_LENGTH") {
					t.Fatalf("wrong error %v", err)
				}
				w.Close()
				if manager.Used() != 0 {
					t.Fatal("leak")
				}
			})
		}
	}
}
func TestListLengthBudgetCancelAndSelection(t *testing.T) {
	schema, b := listLengthInput(memory.TypeInt64, 130)
	p, err := Compile(schema, ListLength(Col("xs")))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(1)
	w, err := NewWorkspace(context.Background(), p, manager)
	if err == nil {
		_, err = w.Evaluate(context.Background(), b, testKernelSet())
		w.Close()
	}
	if !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 {
		t.Fatalf("budget %v used%d", err, manager.Used())
	}
	manager = memory.NewBudgetManager(0)
	w, _ = NewWorkspace(context.Background(), p, manager)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = w.Evaluate(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("cancel leak")
	}
	w, _ = NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	if _, err = w.EvaluateSelected(context.Background(), b, []int{1, 2}, testKernelSet()); err == nil {
		t.Fatal("nested selection unexpectedly enabled")
	}
}

func TestListLengthBorrowedOffsetViews(t *testing.T) {
	for _, kind := range []memory.DataType{memory.TypeString, memory.TypeInt64} {
		schema, b := listLengthInput(kind, 5)
		b.Rows = 2
		if kind == memory.TypeInt64 {
			v := b.Int64Lists["xs"]
			v.Offsets = v.Offsets[2:]
			v.Validity = nil
		} else {
			v := b.StringLists["xs"]
			v.Offsets = v.Offsets[2:]
			v.Validity = nil
		}
		// Existing range materialization also accepts this nonzero-offset view.
		if _, err := memory.CloneBatchRange(b, 0, 2); err != nil {
			t.Fatal("existing range contract", err)
		}
		p, err := Compile(schema, ListLength(Col("xs")))
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		got, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Int64.Values[0] != 2 || got[0].Int64.Values[1] != 3 {
			t.Fatal(got[0].Int64.Values)
		}
		w.Close()
	}
}
func TestListLengthMalformedStringChildren(t *testing.T) {
	for _, bad := range []string{"decreasing", "outofbounds", "missingzero"} {
		t.Run(bad, func(t *testing.T) {
			schema, b := listLengthInput(memory.TypeString, 5)
			v := b.StringLists["xs"].Values
			switch bad {
			case "decreasing":
				v.Data = []byte("a")
				v.Offsets[1] = 1
			case "outofbounds":
				v.Offsets[1] = 2
			case "missingzero":
				b.Rows = 0
				v.Offsets = nil
			}
			p, _ := Compile(schema, ListLength(Col("xs")))
			w, _ := NewWorkspace(context.Background(), p, nil)
			defer w.Close()
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
				t.Fatal("accepted malformed child")
			}
		})
	}
}

func TestListLengthGuardedValueErrors(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "text", Type: memory.TypeString}, memory.Field{Name: "keep", Type: memory.TypeBool})
	b := memory.NewBatch()
	b.Rows = 3
	b.Strings["text"] = testStringVector([]string{"bad", "12", "bad"}, nil)
	b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0}}
	list := Split(CastTo(CastTo(Col("text"), memory.TypeInt64), memory.TypeString), Str(","))
	for _, e := range []Expr{If(Col("keep"), ListLength(list), I64(7)), Case([]WhenThen{{When: Col("keep"), Then: ListLength(list)}}, I64(7))} {
		p, err := Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		got, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		v := got[0].Int64.Values
		if !got[0].Int64.IsValid(0) || !got[0].Int64.IsValid(1) || !got[0].Int64.IsValid(2) || v[0] != 7 || v[1] != 1 || v[2] != 7 {
			t.Fatal(v)
		}
		w.Close()
	}
	p, err := Compile(schema, ListLength(list))
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "cast string to int64 at row 0") {
		t.Fatalf("control did not demand bad cast: %v", err)
	}
}
func TestListLengthBudgetGrowthRetry(t *testing.T) {
	schema, small := listLengthInput(memory.TypeInt64, 3)
	_, large := listLengthInput(memory.TypeInt64, 4096)
	p, _ := Compile(schema, ListLength(Col("xs")))
	needed, err := p.WorkspaceBytes(3)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(needed + 1024)
	w, _ := NewWorkspace(context.Background(), p, manager)
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), small, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	used := manager.Used()
	if _, err = w.Evaluate(context.Background(), large, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if manager.Used() != used {
		t.Fatal("growth reservation leaked")
	}
	got, err := w.Evaluate(context.Background(), small, testKernelSet())
	if err != nil || got[0].Int64.Values[2] != 2 {
		t.Fatal("retry failed", err)
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("close leak")
	}
}
func TestListLengthCancelDuringValidation(t *testing.T) {
	_, b := listLengthInput(memory.TypeString, 130)
	in := scalarRegister{strList: b.StringLists["xs"], validity: b.StringLists["xs"].Validity}
	checks := 0
	probe := scalarRegister{ownedValidity: make(memory.Bitmap, memory.WordsFor(130)), checkContext: func() error { checks++; return nil }}
	if err := evalListLength(&probe, &in, memory.TypeString, 130); err != nil {
		t.Fatal(err)
	}
	for _, stop := range []int{1, 130, 131, checks} {
		seen := 0
		out := scalarRegister{ownedValidity: make(memory.Bitmap, memory.WordsFor(130)), checkContext: func() error {
			seen++
			if seen == stop {
				return context.Canceled
			}
			return nil
		}}
		if err := evalListLength(&out, &in, memory.TypeString, 130); !errors.Is(err, context.Canceled) {
			t.Fatalf("checkpoint %d err %v", stop, err)
		}
	}
}

func TestListLengthGuardedStructuralValidation(t *testing.T) {
	for _, kind := range []memory.DataType{memory.TypeString, memory.TypeInt64} {
		for _, bad := range []string{"short_parent", "short_offsets", "null_payload", "tail"} {
			t.Run(fmt.Sprintf("%s/%s", kind, bad), func(t *testing.T) {
				schema, b := listLengthInput(kind, 65)
				var offsets []uint32
				if kind == memory.TypeInt64 {
					v := b.Int64Lists["xs"]
					offsets = v.Offsets
					if bad == "short_parent" {
						v.Validity = memory.Bitmap{1}
					}
					if bad == "short_offsets" {
						v.Offsets = v.Offsets[:2]
					}
				} else {
					v := b.StringLists["xs"]
					offsets = v.Offsets
					if bad == "short_parent" {
						v.Validity = memory.Bitmap{1}
					}
					if bad == "short_offsets" {
						v.Offsets = v.Offsets[:2]
					}
				}
				if bad == "null_payload" {
					offsets[1] = ^uint32(0)
				} // row 0 is NULL; structure still matters.
				if bad == "tail" {
					b.Rows = 2
					offsets[64] = ^uint32(0)
				} // outside the logical prefix.
				for _, e := range []Expr{If(Bool(true), ListLength(Col("xs")), I64(0)), Case([]WhenThen{{When: Bool(true), Then: ListLength(Col("xs"))}}, I64(0))} {
					p, err := Compile(schema, e)
					if err != nil {
						t.Fatal(err)
					}
					w, _ := NewWorkspace(context.Background(), p, nil)
					_, err = w.Evaluate(context.Background(), b, testKernelSet())
					w.Close()
					if bad == "tail" {
						if err != nil {
							t.Fatal(err)
						}
					} else if err == nil {
						t.Fatal("malformed structure accepted")
					}
					if bad == "short_parent" && !strings.Contains(err.Error(), "validity has 1 words, want at least 2") {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
func TestListLengthMetadataAndCSE(t *testing.T) {
	for _, parent := range []bool{false, true} {
		for _, child := range []bool{false, true} {
			schema := memory.NewSchema(memory.ListInt64Field("xs", parent, child))
			p, err := Compile(schema, ListLength(Col("xs")))
			if err != nil {
				t.Fatal(err)
			}
			if p.Type() != memory.TypeInt64 || p.Nullable() != parent || p.ResultField().Nullable != parent {
				t.Fatal("wrong result metadata")
			}
			duplicate, err := CompileExpressions(schema, []NamedExpr{Named(ListLength(Col("xs")), "a"), Named(ListLength(Col("xs")), "b")})
			if err != nil {
				t.Fatal(err)
			}
			if duplicate.InstructionCount() != p.InstructionCount() {
				t.Fatal("duplicate list length computation")
			}
			_, b := listLengthInput(memory.TypeInt64, 3)
			if !parent {
				b.Int64Lists["xs"].Validity = nil
			}
			if !child {
				b.Int64Lists["xs"].Values.Validity = nil
			}
			w, _ := NewWorkspace(context.Background(), duplicate, nil)
			got, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if got[0].Int64.Values[2] != 2 || got[1].Int64.Values[2] != 2 || got[0].Int64.IsValid(0) == parent || got[1].Int64.IsValid(0) == parent {
				t.Fatal("CSE result")
			}
			w.Close()
		}
	}
}

func ExampleListLength() {
	schema := memory.NewSchema(memory.ListInt64Field("xs", true, true))
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 3, 3, 3}, Validity: memory.Bitmap{3}, Values: &memory.Int64Vector{Values: []int64{1, 0, 2}, Validity: memory.Bitmap{5}}}
	p, err := Compile(schema, ListLength(Col("xs")))
	if err != nil {
		panic(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		panic(err)
	}
	defer w.Close()
	result, err := w.Evaluate(context.Background(), batch, kernel.New(kernel.ModeScalar))
	if err != nil {
		panic(err)
	}
	v := result[0].Int64
	fmt.Println(v.Values[0], v.Values[1], v.IsValid(2))
	// Output: 3 0 false
}
