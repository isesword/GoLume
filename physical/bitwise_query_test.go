package physical

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

// Real filter/project streams cover selected materialization, three source
// batches and lossless high-bit uint64 results without a floating-point oracle.
func TestBitwiseReviewWholeQuery(t *testing.T) {
	const rows = 8199
	b := memory.NewBatch()
	b.Rows = rows
	a, c, ids := make([]uint64, rows), make([]uint64, rows), make([]int64, rows)
	av, cv := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
	var selected []int
	for i := range a {
		a[i] = uint64(i) | 1<<63 | 1<<53
		c[i] = uint64(i*17) ^ 0xaaaaaaaaaaaaaaaa
		ids[i] = int64(i)
		if i%7 != 0 {
			memory.Set(av, i)
		}
		if i%11 != 0 {
			memory.Set(cv, i)
		}
		if i%7 != 0 && a[i]&3 == 1 {
			selected = append(selected, i)
		}
	}
	b.Uint64s["a"] = &memory.Uint64Vector{Values: a, Validity: av}
	b.Uint64s["b"] = &memory.Uint64Vector{Values: c, Validity: cv}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	input, err := inmemory.New(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeUint64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeUint64, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(input, expr.EQ(expr.Col("a").BitAnd(expr.I64(3)), expr.I64(1)))
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD, kernel.ModeAuto} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			manager := memory.NewBudgetManager(0)
			project, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Col("id"), "id"), expr.Named(expr.Col("a").BitAnd(expr.Col("b")), "and"), expr.Named(expr.Col("a").BitOr(expr.Col("b")), "or"), expr.Named(expr.Col("a").BitXor(expr.Col("b")), "xor"), expr.Named(expr.Col("a").BitNot(), "not")}, false, manager)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := project.NewBatchStream(context.Background(), source.ReadOptions{Kernels: kernel.New(mode), BatchRows: 4096, MemoryManager: manager})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := stream.Close(); err != nil {
					t.Error(err)
				}
				if manager.Used() != 0 {
					t.Errorf("leaked %d bytes", manager.Used())
				}
			}()
			out := memory.NewBatch()
			count := 0
			for {
				ok, err := stream.Next(context.Background(), out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				for row, id := range out.Int64s["id"].Values {
					if count >= len(selected) || id != int64(selected[count]) {
						t.Fatalf("row identity %d: %d", count, id)
					}
					i := int(id)
					for _, name := range []string{"and", "or", "xor", "not"} {
						v := out.Uint64s[name]
						if v == nil {
							t.Fatalf("missing uint64 %s", name)
						}
						valid := i%11 != 0
						var want uint64
						switch name {
						case "and":
							want = a[i] & c[i]
						case "or":
							want = a[i] | c[i]
						case "xor":
							want = a[i] ^ c[i]
						case "not":
							want = ^a[i]
							valid = true
						}
						if v.IsValid(row) != valid || valid && v.Values[row] != want {
							t.Fatalf("%s row %d got %x/%v want %x/%v", name, i, v.Values[row], v.IsValid(row), want, valid)
						}
					}
					count++
				}
			}
			if count != len(selected) {
				t.Fatalf("count=%d want=%d", count, len(selected))
			}
		})
	}
}
