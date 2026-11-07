package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
	"unsafe"
)

func TestFutureReviewWideStructAdmission(t *testing.T) {
	const children = 512
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1}}
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{7, 8}}
	child := memory.NewBatch()
	child.Rows = 2
	fields := make([]memory.Field, children)
	for i := range fields {
		name := fmt.Sprintf("c%d", i)
		fields[i] = memory.Field{Name: name, Type: memory.TypeInt8}
		child.Int8s[name] = &memory.Int8Vector{Values: []int8{1, 2}}
	}
	b.Structs["record"] = &memory.StructVector{Rows: 2, Fields: fields, Values: child, Encoding: memory.EncodingView}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "record", Type: memory.TypeStruct, Encoding: memory.EncodingView, Children: fields})
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	// This limit cannot hold even the one cloned Fields backing slice, before
	// considering its child Batch/maps/vector descriptors or numeric payload.
	limit := int64(children) * int64(unsafe.Sizeof(memory.Field{})) / 2
	manager := memory.NewBudgetManager(limit)
	win := futureTestWindow(t, src, manager, 1)
	st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
	if err != nil {
		if errors.Is(err, memory.ErrBudgetExceeded) {
			return
		}
		t.Fatal(err)
	}
	defer st.Close()
	_, err = st.Next(context.Background(), memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("wide struct clone escaped metadata admission: used=%d limit=%d error=%v", manager.Used(), limit, err)
	}
}

func TestFutureReviewIndependentCursorsAndPartitions(t *testing.T) {
	const rows = 259
	b := memory.NewBatch()
	b.Rows = rows
	b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, rows)}
	b.Int64s["g"] = &memory.Int64Vector{Values: make([]int64, rows)}
	b.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, rows), Validity: memory.EnsureBitmap(nil, rows)}
	for i := 0; i < rows; i++ {
		b.Int64s["id"].Values[i] = int64(i)
		b.Int64s["g"].Values[i] = int64(i / 65)
		b.Int64s["v"].Values[i] = int64(i * 2)
		if i%11 != 0 {
			memory.Set(b.Int64s["v"].Validity, i)
		}
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	ex := []WindowExpression{{Function: WindowRowNumber, Alias: "rn"}}
	fs := []memory.Field{{Name: "rn", Type: memory.TypeInt64}}
	for i, lead := range []int64{1, 7, 73} {
		bound := WindowBound{Kind: WindowFollowing, Offset: lead}
		ex = append(ex, WindowExpression{Function: WindowShift, Column: "v", Alias: fmt.Sprint("s", i), Frame: WindowFrame{Start: bound, End: bound}})
		fs = append(fs, memory.Field{Name: fmt.Sprint("s", i), Type: memory.TypeInt64, Nullable: true})
	}
	for _, batchRows := range []int{1, 3, 64, 129} {
		manager := memory.NewBudgetManager(1 << 20)
		win, err := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, ex, fs, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
		if err != nil {
			t.Fatal(err)
		}
		st, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: batchRows})
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		seen := 0
		for {
			ok, err := st.Next(context.Background(), out)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			for j := 0; j < out.Rows; j++ {
				row := seen + j
				if out.Int64s["rn"].Values[j] != int64(row%65+1) {
					t.Fatal("rank partition", row)
				}
				for i, lead := range []int{1, 7, 73} {
					target := row + lead
					valid := target < rows && target/65 == row/65 && target%11 != 0
					v := out.Int64s[fmt.Sprint("s", i)]
					if v.IsValid(j) != valid || (valid && v.Values[j] != int64(target*2)) {
						t.Fatalf("batch%d row%d lead%d: %d/%v", batchRows, row, lead, v.Values[j], v.IsValid(j))
					}
				}
			}
			seen += out.Rows
		}
		st.Close()
		if seen != rows || manager.Used() != 0 {
			t.Fatal(seen, manager.Used())
		}
	}
}

func TestFutureReviewWideHiddenOutputsAdmission(t *testing.T) {
	const outputs = 2048
	input := futureTestSource(t, 2)
	manager := memory.NewBudgetManager(80 << 10)
	future := &futureWindowSource{input: input, manager: manager, maxLead: 1}
	for i := 0; i < outputs; i++ {
		name := fmt.Sprintf("hidden%d", i)
		future.fields = append(future.fields, memory.Field{Name: name, Type: memory.TypeInt64, Nullable: true})
		future.names = append(future.names, name)
		bound := WindowBound{Kind: WindowFollowing, Offset: 1}
		future.expressions = append(future.expressions, WindowExpression{Function: WindowShift, Column: "v", Frame: WindowFrame{Start: bound, End: bound}})
	}
	future.schema = memory.NewSchema(append(input.Schema().Fields, future.fields...)...)
	if int64(outputs)*int64(unsafe.Sizeof(memory.Int64Vector{})) <= 80<<10 {
		t.Fatal("fixture's vector headers must exceed budget")
	}
	st, err := future.NewBatchStream(context.Background(), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
	if err != nil {
		if errors.Is(err, memory.ErrBudgetExceeded) {
			return
		}
		t.Fatal(err)
	}
	defer st.Close()
	_, err = st.Next(context.Background(), memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("hidden output descriptors escaped admission: used=%d err=%v", manager.Used(), err)
	}
}
