package expr

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"strings"
	"sync"
	"testing"

	"gocolumn/memory"
)

// Concrete constructor results may be reboxed at each call. Their descendants
// still retain their source handles and must not expand as a binary tree.
func TestDAGBindingReviewConcreteValueDiamond(t *testing.T) {
	const depth = 128
	e := Add(Col("x"), I64(1))
	for range depth {
		e = Add(e, e)
	}
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	bound, err := Bind(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[*BoundExpr]bool)
	var walk func(*BoundExpr)
	walk = func(b *BoundExpr) {
		if seen[b] {
			return
		}
		seen[b] = true
		for _, child := range b.Children {
			walk(child)
		}
	}
	walk(bound)
	if len(seen) > 2*depth+4 {
		t.Fatalf("binding expanded source DAG: %d nodes", len(seen))
	}
	program, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	if program.InstructionCount() != depth+3 {
		t.Fatalf("instructions=%d, want %d", program.InstructionCount(), depth+3)
	}
}

func TestDAGBindingReviewIndirectSliceCycles(t *testing.T) {
	for _, kind := range []string{"coalesce", "case", "in"} {
		t.Run(kind, func(t *testing.T) {
			values := make([]Expr, 1)
			var a Expr = CoalesceExpr{Values: values}
			switch kind {
			case "coalesce":
				values[0] = CoalesceExpr{Values: []Expr{a}}
			case "case":
				values[0] = CaseExpr{Whens: []WhenThen{{When: Bool(true), Then: a}}}
			case "in":
				values[0] = InExpr{Value: I64(1), Values: []Expr{a}}
			}
			_, err := Bind(memory.NewSchema(), a)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "cycl") {
				t.Fatalf("expected cycle rejection, got %v", err)
			}
		})
	}
}

func TestDAGBindingReviewSharedJSONDetached(t *testing.T) {
	target := memory.NewSchema(memory.ListStringField("nested", true, true))
	var json Expr = JSONDecode(Str(`{"nested":["a"]}`), target)
	root := And(IsNull(json), IsNotNull(json))
	bound, err := Bind(memory.NewSchema(), root)
	if err != nil {
		t.Fatal(err)
	}
	left, right := bound.Children[0].Children[0], bound.Children[1].Children[0]
	if left != right {
		t.Fatal("source JSON node was not shared")
	}
	target.Fields[0].Children[0].Type = memory.TypeInt64
	source := json.(JSONDecodeExpr)
	source.Schema.Fields[0].Children[0].Type = memory.TypeBool
	if left.Field.Children[0].Children[0].Type != memory.TypeString {
		t.Fatal("bound field aliases source schema")
	}
	if left.Expr.(JSONDecodeExpr).Schema.Fields[0].Children[0].Type != memory.TypeString {
		t.Fatal("bound JSON expression aliases source schema")
	}
}

func TestDAGBindingReviewRegionStringsConcurrentMasks(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "c", Type: memory.TypeBool}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	var shared Expr = Upper(Col("s"))
	p, err := CompileExpressions(schema, []NamedExpr{
		Named(If(Col("c"), shared, Str("left")), "a"),
		Named(If(Not(Col("c")), shared, Str("right")), "b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				errs <- err
				return
			}
			defer w.Close()
			for pass, rows := range []int{130, 1, 65, 0, 129, 257, 63} {
				batch := memory.NewBatch()
				batch.Rows = rows
				cond := make([]uint32, rows)
				values := make([]string, rows)
				valid := make([]bool, rows)
				for row := range rows {
					cond[row] = uint32((row + pass + worker) % 2)
					values[row] = fmt.Sprintf("worker%d-row%d", worker, row)
					valid[row] = (row+pass)%3 != 0
				}
				batch.Uint32s["c"] = &memory.Uint32Vector{Values: cond}
				batch.Strings["s"] = testStringVector(values, valid)
				outputs, err := w.Evaluate(context.Background(), batch, kernel.New(kernel.ModeScalar))
				if err != nil {
					errs <- err
					return
				}
				for row := range rows {
					for column := range 2 {
						takeShared := (cond[row] == 1) == (column == 0)
						wantValid := !takeShared || valid[row]
						want := []string{"left", "right"}[column]
						if takeShared {
							want = strings.ToUpper(values[row])
						}
						got := outputs[column].String
						if got.IsValid(row) != wantValid || (wantValid && string(got.ValueBytes(row)) != want) {
							errs <- fmt.Errorf("worker=%d pass=%d column=%d row=%d output or validity mismatch", worker, pass, column, row)
							return
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestDAGBindingReviewSelectionPlanOwnership(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true})
	p, err := Compile(schema, If(GT(Col("x"), I64(0)), Add(Col("x"), I64(1)), I64(-1)))
	if err != nil {
		t.Fatal(err)
	}
	if !p.SupportsSelection() {
		t.Fatal("numeric conditional should support selection")
	}
	if n := testing.AllocsPerRun(100, func() {
		if !p.SupportsSelection() {
			panic("unexpected capability change")
		}
	}); n != 0 {
		t.Fatalf("capability check allocates: %v", n)
	}
	for _, program := range []Program{p, withoutStoragePlan(p)} {
		w, err := NewWorkspace(context.Background(), program, nil)
		if err != nil {
			t.Fatal(err)
		}
		batch := memory.NewBatch()
		batch.Rows = 3
		batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}, Validity: memory.Bitmap{7}}
		got, err := w.EvaluateSelected(context.Background(), batch, []int{0, 2}, kernel.New(kernel.ModeScalar))
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Int64.Values[0] != 2 || got[0].Int64.Values[1] != 4 {
			t.Fatal("selected clone evaluation differs")
		}
		if len(w.selected.columns) != 1 || cap(w.selected.columns) != 1 {
			t.Fatal("workspace descriptors exceed exact column count")
		}
		if len(p.selection.columns) != 1 || p.selection.columns[0].validity != nil {
			t.Fatal("workspace mutated immutable plan")
		}
		w.selected.columns[0].name = "changed workspace copy"
		if p.selection.columns[0].name != "x" {
			t.Fatal("workspace aliases immutable descriptors")
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
