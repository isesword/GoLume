package expr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func recursiveBFixture(t *testing.T, depth, n int) (memory.Schema, *memory.Batch) {
	t.Helper()
	leaf := memory.NewBatch()
	leaf.Rows = n
	vals := make([]int64, n)
	validity := make(memory.Bitmap, memory.WordsFor(n))
	offsets := make([]uint32, n+1)
	var data []byte
	for i := 0; i < n; i++ {
		vals[i] = int64(1000 + i)
		if i%5 != 0 {
			memory.Set(validity, i)
		}
		data = append(data, []byte(fmt.Sprintf("中-%d", i))...)
		offsets[i+1] = uint32(len(data))
	}
	leaf.Int64s["v"] = &memory.Int64Vector{Values: vals}
	leaf.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: validity}
	listOffsets := make([]uint32, n+1)
	elemOffsets := make([]uint32, 2*n+1)
	nums := make([]int64, 2*n)
	elemValidity := make(memory.Bitmap, memory.WordsFor(2*n))
	var words []byte
	for row := 0; row < n; row++ {
		listOffsets[row+1] = uint32(2 * (row + 1))
		for j := 0; j < 2; j++ {
			i := 2*row + j
			nums[i] = int64(row*10 + j)
			words = append(words, []byte(fmt.Sprintf("e%d", i))...)
			elemOffsets[i+1] = uint32(len(words))
			if j == 0 {
				memory.Set(elemValidity, i)
			}
		}
	}
	leaf.Int64Lists["ints"] = &memory.Int64ListVector{Offsets: listOffsets, Validity: validity, Values: &memory.Int64Vector{Values: nums, Validity: elemValidity}}
	leaf.StringLists["words"] = &memory.StringListVector{Offsets: listOffsets, Validity: validity, Values: &memory.StringViewVector{Offsets: elemOffsets, Data: words, Validity: elemValidity}}
	fields := []memory.Field{{Name: "v", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeString, Nullable: true}, memory.ListInt64Field("ints", true, true), memory.ListStringField("words", true, true)}
	var field memory.Field
	var node *memory.StructVector
	for level := depth; level >= 1; level-- {
		nullable := level%2 == 1
		var mask memory.Bitmap
		if nullable {
			mask = make(memory.Bitmap, memory.WordsFor(n))
			for i := 0; i < n; i++ {
				if i%(level+3) != 0 {
					memory.Set(mask, i)
				}
			}
		}
		name := "node.中"
		if level == 1 {
			name = "root"
		}
		field = memory.StructField(name, nullable, fields...)
		node = &memory.StructVector{Rows: n, Fields: field.Children, Values: leaf, Validity: mask, Encoding: memory.EncodingView}
		leaf = memory.NewBatch()
		leaf.Rows = n
		leaf.Structs[name] = node
		fields = []memory.Field{field}
	}
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	leaf.Int64s["id"] = &memory.Int64Vector{Values: ids}
	schema, err := memory.NewSchemaChecked(field, memory.Field{Name: "id", Type: memory.TypeInt64})
	if err != nil {
		t.Fatal(err)
	}
	return schema, leaf
}
func recursiveBValid(depth, row int) bool {
	for level := 1; level <= depth; level += 2 {
		if row%(level+3) == 0 {
			return false
		}
	}
	return true
}

func recursiveBLeaf(depth int, name string) Expr {
	var e Expr = Col("root")
	for i := 1; i < depth; i++ {
		e = StructGet(e, "node.中")
	}
	return StructGet(e, name)
}
func TestRecursiveStructBExpr(t *testing.T) {
	for _, depth := range []int{2, 8} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("d%d/m%d", depth, mode), func(t *testing.T) {
				schema, _ := recursiveBFixture(t, depth, 0)
				for _, guard := range []bool{false, true} {
					var e Expr = recursiveBLeaf(depth, "v")
					if guard {
						e = If(GT(Col("id"), I64(32)), e, I64(-1))
					}
					p, err := Compile(schema, e)
					if err != nil {
						t.Fatal(err)
					}
					if p.ResultField().Type != memory.TypeInt64 || !p.ResultField().Nullable || p.SupportsSelection() {
						t.Fatal("metadata/selection")
					}
					m := memory.NewBudgetManager(0)
					w, err := NewWorkspace(context.Background(), p, m)
					if err != nil {
						t.Fatal(err)
					}
					for _, n := range []int{0, 65} {
						_, b := recursiveBFixture(t, depth, n)
						got, err := w.Evaluate(context.Background(), b, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						for row := 0; row < n; row++ {
							valid := recursiveBValid(depth, row)
							want := int64(1000 + row)
							if guard && row <= 32 {
								valid = true
								want = -1
							}
							if got[0].Int64.IsValid(row) != valid || valid && got[0].Int64.Values[row] != want {
								t.Fatalf("row%d validity/value", row)
							}
						}
					}
					w.Close()
					if m.Used() != 0 {
						t.Fatal("leak")
					}
				}
				p, err := Compile(schema, StructGet(Col("root"), "node.中"))
				if err != nil {
					t.Fatal(err)
				}
				if p.ResultField().Type != memory.TypeStruct || !p.ResultField().Nullable {
					t.Fatal("owned struct metadata")
				}
				field := p.ResultField()
				field.Children[0].Name = "changed"
				if p.ResultField().Children[0].Name == "changed" {
					t.Fatal("metadata aliases")
				}
				_, b := recursiveBFixture(t, depth, 65)
				m := memory.NewBudgetManager(0)
				w, err := NewWorkspace(context.Background(), p, m)
				if err != nil {
					t.Fatal(err)
				}
				got, err := w.Evaluate(context.Background(), b, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				src := b.Structs["root"]
				out := got[0].Struct
				for level := 1; level < depth; level++ {
					src = src.Values.Structs["node.中"]
					if level > 1 {
						out = out.Values.Structs["node.中"]
					}
				}
				old := out.Values.Int64s["v"].Values[1]
				src.Values.Int64s["v"].Values[1] = -99
				for i := range src.Values.Strings["s"].Data {
					src.Values.Strings["s"].Data[i] = '!'
				}
				if string(out.Values.Strings["s"].ValueBytes(1)) != "中-1" {
					t.Fatal("owned String aliases")
				}
				src.Values.Int64Lists["ints"].Values.Values[2] = -111
				if out.Values.Int64Lists["ints"].Values.Values[int(out.Values.Int64Lists["ints"].Offsets[1])] != 10 {
					t.Fatal("owned list aliases")
				}
				src.Validity = nil // changing the input descriptor must not alter output masks.
				if got[0].Struct.IsValid(0) {
					t.Fatal("ancestor mask alias")
				}
				if out.Values.Int64s["v"].Values[1] != old || old != 1001 {
					t.Fatal("owned values alias")
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err = w.Evaluate(ctx, b, kernel.New(mode)); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("cancel leak")
				}
				deny := memory.NewBudgetManager(1)
				w, err = NewWorkspace(context.Background(), p, deny)
				if err == nil {
					_, err = w.Evaluate(context.Background(), b, kernel.New(mode))
					w.Close()
				}
				if !errors.Is(err, memory.ErrBudgetExceeded) || deny.Used() != 0 {
					t.Fatalf("budget %v used%d", err, deny.Used())
				}
				for _, bad := range []Expr{JSONDecode(Col("text"), schema)} {
					s, err := memory.NewSchemaChecked(append(schema.Fields, memory.Field{Name: "text", Type: memory.TypeString})...)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = Compile(s, bad); err == nil {
						t.Fatal("unexpanded recursive constructor accepted")
					}
				}
			})
		}
	}
}

func TestRecursiveStructBReplacement(t *testing.T) {
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprintf("d%d", depth), func(t *testing.T) {
			schema, b := recursiveBFixture(t, depth, 65)
			p, err := Compile(schema, StructGet(Col("root"), "node.中"))
			if err != nil {
				t.Fatal(err)
			}
			probe := &structGetAuditManager{base: memory.NewBudgetManager(0)}
			pw, err := NewWorkspace(context.Background(), p, probe)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = pw.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			start := probe.calls
			if _, err = pw.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			calls := probe.calls - start
			pw.Close()
			if calls == 0 {
				t.Fatal("no replacement admissions")
			}
			for fail := 1; fail <= calls; fail++ {
				m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
				w, err := NewWorkspace(context.Background(), p, m)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
					t.Fatal(err)
				}
				before := m.Used()
				m.fail = m.calls + fail
				if _, err = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("admission%d: %v", fail, err)
				}
				if m.Used() != before {
					t.Fatalf("failed replacement admission%d charge%d want%d", fail, m.Used(), before)
				}
				m.fail = 0
				got, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				if !got[0].Struct.IsValid(1) || got[0].Struct.IsValid(0) {
					t.Fatal("retry result")
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("replacement leak")
				}
			}
			base, cancel := context.WithCancel(context.Background())
			ctx := &structGetCheckpointContext{Context: base, cancel: cancel}
			cw, _ := NewWorkspace(context.Background(), p, nil)
			if _, err = cw.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			if _, err = cw.Evaluate(ctx, b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			checks := ctx.checks
			cw.Close()
			cancel()
			for _, stop := range []int{1, checks / 2, checks - 1} {
				m := memory.NewBudgetManager(0)
				w, err := NewWorkspace(context.Background(), p, m)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
					t.Fatal(err)
				}
				before := m.Used()
				base, cancel := context.WithCancel(context.Background())
				ctx := &structGetCheckpointContext{Context: base, cancel: cancel, stop: stop}
				_, err = w.Evaluate(ctx, b, testKernelSet())
				cancel()
				if !errors.Is(err, context.Canceled) {
					t.Fatal("cancel checkpoint", stop, err)
				}
				if m.Used() != 0 {
					t.Fatalf("canceled workspace must release all owners: %d", m.Used())
				}
				_ = before
				w.Close()
				if m.Used() != 0 {
					t.Fatal("cancel replacement leak")
				}
				retry, e := NewWorkspace(context.Background(), p, m)
				if e != nil {
					t.Fatal(e)
				}
				got, e := retry.Evaluate(context.Background(), b, testKernelSet())
				if e != nil || !got[0].Struct.IsValid(1) {
					t.Fatal("fresh workspace retry", e)
				}
				retry.Close()
				if m.Used() != 0 {
					t.Fatal("retry leak")
				}
			}
		})
	}
}

// Direct operator cancellation isolates the transactional owner contract from
// Workspace.Evaluate's intentional close-on-context-cancellation lifecycle.
func TestRecursiveStructBOperatorCancel(t *testing.T) {
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprintf("d%d", depth), func(t *testing.T) {
			schema, b := recursiveBFixture(t, depth, 65)
			parent := schema.Fields[0]
			plan := &structGetPlan{parent: parent, child: parent.Children[0], index: 0}
			input := scalarRegister{typ: memory.TypeStruct, strct: b.Structs["root"]}
			m := memory.NewBudgetManager(0)
			out := scalarRegister{typ: memory.TypeStruct}
			out.acquire = func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }
			if err := evalStructGet(&out, &input, plan, 65); err != nil {
				t.Fatal(err)
			}
			checks := 0
			out.checkContext = func() error { checks++; return nil }
			if err := evalStructGet(&out, &input, plan, 65); err != nil {
				t.Fatal(err)
			}
			if checks < 3 {
				t.Fatal("missing checkpoints")
			}
			before, _ := json.Marshal(out.strct)
			charge := m.Used()
			for _, stop := range []int{1, checks / 2, checks - 1} {
				seen := 0
				out.checkContext = func() error {
					seen++
					if seen >= stop {
						return context.Canceled
					}
					return nil
				}
				err := evalStructGet(&out, &input, plan, 65)
				if !errors.Is(err, context.Canceled) {
					t.Fatal("operator cancel", stop, err)
				}
				after, _ := json.Marshal(out.strct)
				if string(after) != string(before) || m.Used() != charge {
					t.Fatalf("operator failed copy changed owner stop%d used%d want%d", stop, m.Used(), charge)
				}
			}
			out.structGetReservation.Release()
			if m.Used() != 0 {
				t.Fatal("operator owner leak")
			}
		})
	}
}
