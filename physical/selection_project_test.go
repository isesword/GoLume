package physical

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Hiding the concrete filter keeps the public materializing boundary as an
// oracle even when the adjacent native Filter -> Project edge is optimized.
type materializedSelectionSource struct{ source.Source }

func (s materializedSelectionSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return s.Source.(source.BatchStreamProvider).NewBatchStream(ctx, opts)
}

func selectionFixture(t testing.TB, rows, width, copies int, nullable bool, extra string, scattered ...bool) source.Source {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = rows
	fields := make([]memory.Field, 0, width+1)
	for c := 0; c < width; c++ {
		name := fmt.Sprintf("c%d", c)
		v := &memory.Int64Vector{Values: make([]int64, rows)}
		if nullable {
			v.Validity = make(memory.Bitmap, memory.WordsFor(rows))
		}
		for r := 0; r < rows; r++ {
			v.Values[r] = int64(r + c)
			if c == 0 && len(scattered) > 0 && scattered[0] {
				v.Values[r] = int64((r * 37) % rows)
			}
			if nullable && (r+c)%11 != 0 {
				v.Validity[r>>6] |= uint64(1) << uint(r&63)
			}
		}
		b.Int64s[name] = v
		fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64, Nullable: nullable})
	}
	if extra != "" {
		v := &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, rows), Encoding: memory.EncodingView}
		for r := 0; r < rows; r++ {
			v.Offsets[r+1] = uint32(r + 1)
			v.Data[r] = 'x'
		}
		if extra == "string" {
			b.Strings["extra"] = v
			fields = append(fields, memory.Field{Name: "extra", Type: memory.TypeString, Encoding: memory.EncodingView})
		} else {
			child := memory.NewBatch()
			child.Rows = rows
			child.Strings["text"] = v
			f := memory.Field{Name: "extra", Type: memory.TypeStruct, Children: []memory.Field{{Name: "text", Type: memory.TypeString, Encoding: memory.EncodingView}}}
			b.Structs["extra"] = &memory.StructVector{Rows: rows, Values: child, Fields: f.Children}
			fields = append(fields, f)
		}
	}
	batches := make([]*memory.Batch, copies)
	for i := range batches {
		batches[i] = b
	}
	s, err := inmemory.New(memory.NewSchema(fields...), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func selectionProjection(t testing.TB, input source.Source, threshold int, fused, include bool, manager memory.Manager) *ProjectSource {
	t.Helper()
	filtered, err := NewFilterSource(input, expr.LT(expr.Col("c0"), expr.I64(int64(threshold))))
	if err != nil {
		t.Fatal(err)
	}
	if !fused {
		filtered = materializedSelectionSource{filtered}
	}
	p, err := NewProjectSource(filtered, []expr.NamedExpr{
		expr.Named(expr.Add(expr.Col("c1"), expr.I64(7)), "c0"),
		expr.Named(expr.If(expr.LT(expr.Col("c1"), expr.I64(20)), expr.Mul(expr.Col("c1"), expr.I64(3)), expr.Sub(expr.Col("c1"), expr.I64(2))), "result"),
	}, include, manager)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

type selectionCell struct {
	Value int64
	Valid bool
}

func selectionDrain(ctx context.Context, p *ProjectSource, opts source.ReadOptions) ([][2]selectionCell, error) {
	stream, err := p.NewBatchStream(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	var values [][2]selectionCell
	for turn := 0; ; turn++ {
		// Alternate destinations: no ownership may depend on reusing a Batch header.
		out := memory.NewBatch()
		ok, err := stream.Next(ctx, out)
		if err != nil {
			return nil, err
		}
		if !ok {
			return values, nil
		}
		for r := 0; r < out.Rows; r++ {
			var pair [2]selectionCell
			for i, n := range []string{"c0", "result"} {
				v := out.Int64s[n]
				valid := v.Validity == nil || v.Validity[r>>6]&(uint64(1)<<uint(r&63)) != 0
				pair[i].Valid = valid
				if valid {
					pair[i].Value = v.Values[r]
				}
			}
			values = append(values, pair)
		}
	}
}

func TestSelectionProjectDifferential(t *testing.T) {
	for _, rows := range []int{65, 257} {
		for _, pct := range []int{0, 1, 50, 99, 100} {
			for _, nullable := range []bool{false, true} {
				for _, include := range []bool{false, true} {
					t.Run(fmt.Sprintf("rows%d/pct%d/null%t/input%t", rows, pct, nullable, include), func(t *testing.T) {
						input := selectionFixture(t, rows, 8, 3, nullable, "")
						threshold := rows * pct / 100
						var want [][2]selectionCell
						for _, fused := range []bool{false, true} {
							manager := memory.NewBudgetManager(1 << 24)
							got, err := selectionDrain(context.Background(), selectionProjection(t, input, threshold, fused, include, manager), testReadOptions())
							if err != nil {
								t.Fatal(err)
							}
							if manager.Used() != 0 {
								t.Fatalf("fused=%t leaked %d", fused, manager.Used())
							}
							if !fused {
								want = got
							} else if !reflect.DeepEqual(got, want) {
								t.Fatalf("differential mismatch: got=%v want=%v", got, want)
							}
						}
					})
				}
			}
		}
	}
}

func TestSelectionProjectMixedFallback(t *testing.T) {
	for _, extra := range []string{"string", "struct"} {
		for _, include := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/input%t", extra, include), func(t *testing.T) {
				input := selectionFixture(t, 65, 3, 2, true, extra)
				want, err := selectionDrain(context.Background(), selectionProjection(t, input, 33, false, include, nil), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				got, err := selectionDrain(context.Background(), selectionProjection(t, input, 33, true, include, nil), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("fallback output differs")
				}
			})
		}
	}
}

func TestSelectionProjectBudgetRetry(t *testing.T) {
	input := selectionFixture(t, 4096, 4, 2, false, "")
	want, err := selectionDrain(context.Background(), selectionProjection(t, input, 3000, false, false, nil), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	manager := &projectRequestLimit{base: memory.NewBudgetManager(1 << 20), max: 32768}
	got, err := selectionDrain(context.Background(), selectionProjection(t, input, 3000, true, false, manager), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || manager.denied == 0 || manager.Used() != 0 {
		t.Fatalf("retry mismatch=%t denied=%d retained=%d", !reflect.DeepEqual(got, want), manager.denied, manager.Used())
	}
}

func TestSelectionProjectCancelAndClose(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			p := selectionProjection(t, selectionFixture(t, 65, 4, 3, true, ""), 40, true, false, manager)
			stream, err := p.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatalf("first=%t %v", ok, err)
			}
			if cancel {
				ctx, stop := context.WithCancel(context.Background())
				stop()
				if ok, err := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel=%t %v", ok, err)
				}
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 {
				t.Fatalf("retained %d", manager.Used())
			}
		})
	}
}

func TestSelectionProjectConcurrentStreams(t *testing.T) {
	p := selectionProjection(t, selectionFixture(t, 257, 8, 3, true, ""), 129, true, false, nil)
	want, err := selectionDrain(context.Background(), p, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := selectionDrain(context.Background(), p, testReadOptions())
			if err != nil {
				t.Error(err)
			} else if !reflect.DeepEqual(got, want) {
				t.Error("concurrent result differs")
			}
		}()
	}
	wg.Wait()
}

func TestSelectionProjectRejectedRowsDoNotRaiseCastErrors(t *testing.T) {
	for _, threshold := range []int{100, 200} {
		for _, fused := range []bool{false, true} {
			t.Run(fmt.Sprintf("threshold%d/fused%t", threshold, fused), func(t *testing.T) {
				input := selectionFixture(t, 200, 2, 1, false, "")
				filtered, err := NewFilterSource(input, expr.LT(expr.Col("c0"), expr.I64(int64(threshold))))
				if err != nil {
					t.Fatal(err)
				}
				if !fused {
					filtered = materializedSelectionSource{filtered}
				}
				manager := memory.NewBudgetManager(1 << 20)
				p, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.CastTo(expr.Col("c1"), memory.TypeInt8), "small")}, false, manager)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := p.NewBatchStream(context.Background(), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				ok, err := stream.Next(context.Background(), out)
				if threshold == 100 {
					if !ok || err != nil || out.Rows != 100 {
						t.Fatalf("rejected overflow rows evaluated: ok=%t err=%v rows=%d", ok, err, out.Rows)
					}
					for r, v := range out.Int8s["small"].Values[:out.Rows] {
						if v != int8(r+1) {
							t.Fatalf("row %d=%d", r, v)
						}
					}
				} else {
					if ok || err == nil || errors.Is(err, memory.ErrBudgetExceeded) {
						t.Fatalf("selected overflow did not raise semantic error: %t %v", ok, err)
					}
					if manager.Used() != 0 {
						t.Fatalf("semantic failure retained %d", manager.Used())
					}
				}
				stream.Close()
				if manager.Used() != 0 {
					t.Fatalf("close retained %d", manager.Used())
				}
			})
		}
	}
}

func TestSelectionProjectPathAndBorrowInvalidation(t *testing.T) {
	for _, extra := range []string{"", "string", "struct"} {
		for _, include := range []bool{false, true} {
			p := selectionProjection(t, selectionFixture(t, 65, 3, 3, false, extra), 33, true, include, nil)
			stream, err := p.NewBatchStream(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			native := stream.(*projectBatchStream)
			if (native.selection != nil) != (extra == "" && !include) {
				t.Fatalf("schema=%q selected=%t", extra, native.selection != nil)
			}
			first, second := memory.NewBatch(), memory.NewBatch()
			if ok, err := stream.Next(context.Background(), first); !ok || err != nil {
				t.Fatalf("first %t %v", ok, err)
			}
			if ok, err := stream.Next(context.Background(), second); !ok || err != nil {
				t.Fatalf("second %t %v", ok, err)
			}
			if first.Rows != 0 || len(first.Int64s) != 0 {
				t.Fatal("previous destination retained borrowed bindings")
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if second.Rows != 0 || len(second.Int64s) != 0 {
				t.Fatal("close retained borrowed destination")
			}
		}
	}

}

func TestSelectionProjectInvalidCallDoesNotAdvance(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	p := selectionProjection(t, selectionFixture(t, 65, 3, 2, false, ""), 33, true, false, manager)
	stream, err := p.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if ok, err := stream.Next(context.Background(), nil); ok || err == nil {
		t.Fatalf("nil destination=%t %v", ok, err)
	}
	if ok, err := stream.Next(nil, memory.NewBatch()); ok || err == nil {
		t.Fatalf("nil context=%t %v", ok, err)
	}
	count := 0
	for {
		out := memory.NewBatch()
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		count += out.Rows
	}
	if count != 66 || manager.Used() != 0 {
		t.Fatalf("rows=%d retained=%d", count, manager.Used())
	}
}

func TestSelectionProjectWidePeakMemory(t *testing.T) {
	input := selectionFixture(t, 8192, 32, 1, false, "")
	var peaks [2]int64
	for i, fused := range []bool{false, true} {
		manager := &selectionPeakManager{base: memory.NewBudgetManager(0)}
		opts := testReadOptions()
		opts.MemoryManager = manager
		opts.BatchRows = 8192
		got, err := selectionDrain(context.Background(), selectionProjection(t, input, 4096, fused, false, manager), opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 4096 || manager.Used() != 0 {
			t.Fatalf("rows=%d retained=%d", len(got), manager.Used())
		}
		peaks[i] = manager.peak
	}
	if peaks[1]*2 >= peaks[0] {
		t.Fatalf("wide narrow-project peak did not halve: materialized=%d selected=%d", peaks[0], peaks[1])
	}
}

func TestSelectionProjectScatteredRows(t *testing.T) {
	for _, pct := range []int{1, 50, 99} {
		input := selectionFixture(t, 257, 8, 3, true, "", true)
		want, err := selectionDrain(context.Background(), selectionProjection(t, input, 257*pct/100, false, false, nil), testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		got, err := selectionDrain(context.Background(), selectionProjection(t, input, 257*pct/100, true, false, nil), testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("pct%d scattered results differ", pct)
		}
	}
}

func TestSelectionProjectFullDependencyFallback(t *testing.T) {
	input := selectionFixture(t, 65, 2, 1, false, "")
	filtered, err := NewFilterSource(input, expr.LT(expr.Col("c0"), expr.I64(33)))
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Add(expr.Col("c0"), expr.Col("c1")), "sum")}, false)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := p.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if stream.(*projectBatchStream).selection != nil {
		t.Fatal("all-input dependency program should keep materialized path")
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next=%t %v", ok, err)
	}
	if out.Rows != 33 {
		t.Fatalf("rows=%d", out.Rows)
	}
	for r, v := range out.Int64s["sum"].Values[:out.Rows] {
		if v != int64(r*2+1) {
			t.Fatalf("row%d=%d", r, v)
		}
	}
}

func TestSelectionProjectKernelParity(t *testing.T) {
	for _, pct := range []int{1, 50, 99, 100} {
		input := selectionFixture(t, 257, 8, 3, true, "", true)
		want, err := selectionDrain(context.Background(), selectionProjection(t, input, 257*pct/100, false, false, nil), testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []kernel.Mode{kernel.ModeAuto, kernel.ModeSIMD} {
			opts := testReadOptions()
			opts.Kernels = kernel.New(mode)
			got, err := selectionDrain(context.Background(), selectionProjection(t, input, 257*pct/100, true, false, nil), opts)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("pct%d mode%s results differ", pct, mode)
			}
		}
	}
}

func TestSelectionProjectChangingDomainsAndPreparedPartitions(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "c0", Type: memory.TypeInt64}, memory.Field{Name: "c1", Type: memory.TypeInt64})
	var batches []*memory.Batch
	for phase, n := range []int{65, 33, 101, 129} {
		batch := memory.NewBatch()
		batch.Rows = n
		batch.Int64s["c0"] = &memory.Int64Vector{Values: make([]int64, n)}
		batch.Int64s["c1"] = &memory.Int64Vector{Values: make([]int64, n)}
		for r := 0; r < n; r++ {
			keep := phase == 1 || (phase != 2 && r%2 == 0)
			if !keep {
				batch.Int64s["c0"].Values[r] = 1
			}
			batch.Int64s["c1"].Values[r] = int64(phase*1000 + r)
		}
		batches = append(batches, batch)
	}
	input, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	want, err := selectionDrain(context.Background(), selectionProjection(t, input, 1, false, false, nil), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	p := selectionProjection(t, input, 1, true, false, nil)
	got, err := selectionDrain(context.Background(), p, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || len(got) != 131 {
		t.Fatalf("changing domains differ: got=%d want=%d", len(got), len(want))
	}
	parts, err := p.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := p.NewBatchStreamWithPartitions(context.Background(), testReadOptions(), parts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	count := 0
	for {
		out := memory.NewBatch()
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for r := 0; r < out.Rows; r++ {
			if count >= len(want) || out.Int64s["c0"].Values[r] != want[count][0].Value || out.Int64s["result"].Values[r] != want[count][1].Value {
				t.Fatalf("prepared row%d differs", count)
			}
			count++
		}
	}
	if count != len(want) {
		t.Fatalf("prepared count=%d want=%d", count, len(want))
	}
}

func TestSelectionProjectQueryOwnership(t *testing.T) {
	engine := memory.NewEngineBudget(4 << 20)
	query, err := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	other, err := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	prepared := memory.NewBudgetManager(1)
	ctx := memory.WithQueryBudget(context.Background(), query)
	p := selectionProjection(t, selectionFixture(t, 65, 4, 4, true, ""), 33, true, false, prepared)
	opts := testReadOptions()
	opts.MemoryManager = prepared
	stream, err := p.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for _, nextCtx := range []context.Context{ctx, context.Background()} {
		if ok, err := stream.Next(nextCtx, memory.NewBatch()); !ok || err != nil {
			t.Fatalf("bound/omitted ownership=%t %v", ok, err)
		}
		if query.Used() == 0 || prepared.Used() != 0 {
			t.Fatalf("query=%d prepared=%d", query.Used(), prepared.Used())
		}
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(memory.WithQueryBudget(context.Background(), other), out); ok || err == nil {
		t.Fatalf("mismatched owner=%t %v", ok, err)
	}
	if query.Used() != 0 || other.Used() != 0 || prepared.Used() != 0 || out.Rows != 0 {
		t.Fatalf("mismatch retained query=%d other=%d prepared=%d rows=%d", query.Used(), other.Used(), prepared.Used(), out.Rows)
	}
}
