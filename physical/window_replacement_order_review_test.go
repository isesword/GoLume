package physical

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Replacing partition/order keys must not change the keys or original values
// read by later expressions, even across batches or native-width changes.
func TestWindowReplacementOrderReviewKeyIsolation(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, stop := range []string{"complete", "early", "cancel", "input_error"} {
			if stop == "input_error" && mode != WindowExecutionOrdered {
				continue
			}
			t.Run(fmt.Sprintf("mode%d/%s", mode, stop), func(t *testing.T) {
				ctx, _, query := spillOutputQuery(t)
				schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt8, Nullable: true}, memory.Field{Name: "p", Type: memory.TypeInt8}, memory.Field{Name: "k", Type: memory.TypeInt16}, memory.Field{Name: "s", Type: memory.TypeInt8}, memory.Field{Name: "a", Type: memory.TypeInt8})
				ps := []int8{1, 1, 1, 2, 2, 2}
				ks := []int16{1, 1, 2, 1, 1, 2}
				vs := []int8{9, 0, 3, 0, 7, 4}
				var batches []*memory.Batch
				for i := range ps {
					b := memory.NewBatch()
					b.Rows = 1
					b.Int8s["s"] = &memory.Int8Vector{Values: []int8{111}}
					b.Int8s["a"] = &memory.Int8Vector{Values: []int8{112}}
					b.Int8s["p"] = &memory.Int8Vector{Values: []int8{ps[i]}}
					b.Int16s["k"] = &memory.Int16Vector{Values: []int16{ks[i]}}
					valid := memory.Bitmap(nil)
					if i == 1 || i == 3 {
						valid = memory.Bitmap{0}
					}
					b.Int8s["v"] = &memory.Int8Vector{Values: []int8{vs[i]}, Validity: valid}
					batches = append(batches, b)
				}
				src, err := inmemory.New(schema, batches...)
				if err != nil {
					t.Fatal(err)
				}
				current := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
				running := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}
				expressions := []WindowExpression{
					{Function: WindowRank, Alias: "p"},
					{Function: WindowMin, Column: "v", Alias: "x", Frame: current},
					{Function: WindowMin, Column: "v", Alias: "k", Frame: running},
					{Function: WindowMax, Column: "p", Alias: "y", Frame: current},
					{Function: WindowRowNumber, Alias: "v"},
					{Function: WindowSum, Column: "v", Alias: "s", Frame: running},
					{Function: WindowAvg, Column: "v", Alias: "a", Frame: running},
				}
				fields := []memory.Field{{Name: "p", Type: memory.TypeInt64}, {Name: "x", Type: memory.TypeInt8, Nullable: true}, {Name: "k", Type: memory.TypeInt8, Nullable: true}, {Name: "y", Type: memory.TypeInt8, Nullable: true}, {Name: "v", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeInt64, Nullable: true}, {Name: "a", Type: memory.TypeFloat64, Nullable: true}}
				win, err := NewWindowSourceWithOptions(src, []string{"p"}, []SortKey{{Column: "k"}}, expressions, fields, query, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				names := []string{}
				for _, f := range win.Schema().Fields {
					names = append(names, f.Name)
				}
				if !reflect.DeepEqual(names, []string{"v", "p", "k", "s", "a", "x", "y"}) {
					t.Fatalf("schema order %v", names)
				}
				stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				defer out.ReleaseOwned()
				seen := 0
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					if out.Int8s["p"] != nil || out.Int8s["v"] != nil || out.Int16s["k"] != nil || out.Int8s["s"] != nil || out.Int8s["a"] != nil {
						t.Fatal("stale replaced bindings")
					}
					for j := 0; j < out.Rows; j++ {
						i := seen + j
						if out.Int64s["s"].IsValid(j) != (i != 3) || (i != 3 && out.Int64s["s"].Values[j] != []int64{9, 9, 12, 0, 7, 11}[i]) {
							t.Fatalf("sum at %d", i)
						}
						if out.Float64s["a"].IsValid(j) != (i != 3) || (i != 3 && out.Float64s["a"].Values[j] != []float64{9, 9, 6, 0, 7, 5.5}[i]) {
							t.Fatalf("average at %d", i)
						}

						if out.Int64s["p"].Values[j] != []int64{1, 1, 3, 1, 1, 3}[i] || out.Int64s["v"].Values[j] != int64(i%3+1) {
							t.Fatalf("rank/row number at %d", i)
						}
						if out.Int8s["y"].Values[j] != ps[i] {
							t.Fatalf("later expression read replaced partition key at %d", i)
						}
						if out.Int8s["x"].IsValid(j) != (i != 1 && i != 3) || (out.Int8s["x"].IsValid(j) && out.Int8s["x"].Values[j] != vs[i]) {
							t.Fatalf("original value/null at %d", i)
						}
						if out.Int8s["k"].IsValid(j) != (i != 3) || (i != 3 && out.Int8s["k"].Values[j] != []int8{9, 9, 3, 0, 7, 4}[i]) {
							t.Fatalf("running minimum at %d", i)
						}
					}
					seen += out.Rows
					if stop == "input_error" {
						failure := errors.New("injected error after alias replacement")
						ordered := stream.(*orderedWindowStream)
						ordered.child = &replacementOrderReviewErrorStream{BatchStream: ordered.child, err: failure}
						ok, err := stream.Next(ctx, out)
						if ok || !errors.Is(err, failure) || out.Rows != 0 {
							t.Fatalf("input failure: ok=%t err=%v rows=%d", ok, err, out.Rows)
						}
						break
					}
					if stop == "early" {
						break
					}
					if stop == "cancel" {
						canceled, cancel := context.WithCancel(ctx)
						cancel()
						_, err = stream.Next(canceled, out)
						// Resident materialization can already be exhausted after its one batch.
						if mode != WindowExecutionResident && !errors.Is(err, context.Canceled) {
							t.Fatalf("cancellation: %v", err)
						}
						break
					}
				}
				if stop == "complete" && seen != len(ps) {
					t.Fatalf("rows=%d", seen)
				}
				for i, b := range batches {
					if b.Int8s["p"].Values[0] != ps[i] || b.Int16s["k"].Values[0] != ks[i] || b.Int8s["v"].Values[0] != vs[i] {
						t.Fatal("source mutated")
					}
				}
				checkSpillOutputCleanup(t, stream, query)
			})
		}
	}
}

// Initializing a replacement must detach every old typed binding without
// releasing the payload still owned by the child batch.
func TestWindowReplacementOrderReviewBorrowedOwnership(t *testing.T) {
	for _, function := range []WindowFunction{WindowRowNumber, WindowRank, WindowDenseRank, WindowCountRows, WindowCountValues, WindowSum, WindowMin, WindowMax, WindowAvg, WindowShift, WindowDiff, WindowForwardFill, WindowBackwardFill} {
		for _, kind := range []string{"opaque", "struct", "list"} {
			t.Run(fmt.Sprintf("function%d/%s", function, kind), func(t *testing.T) {
				input := memory.NewBatch()
				input.Rows = 2
				ref := &extremaReviewRef{}
				owned := &memory.OpaqueVector{Value: ref, Owned: true}
				switch kind {
				case "opaque":
					input.Opaque["replacement"] = owned
				case "struct":
					child := memory.NewBatch()
					child.Rows = 2
					child.Opaque["child"] = owned
					input.Structs["replacement"] = &memory.StructVector{Rows: 2, Values: child}
				case "list":
					input.StringLists["replacement"] = &memory.StringListVector{Offsets: []uint32{0, 1, 2}, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
				}
				out := memory.NewBatch()
				bindAll(out, input)
				initializeWindowOutputs(out, []memory.Field{{Name: "replacement", Type: memory.TypeInt64}}, 2, []WindowExpression{{Function: function, Alias: "replacement"}})
				if out.Opaque["replacement"] != nil || out.Structs["replacement"] != nil || out.StringLists["replacement"] != nil {
					t.Error("borrowed binding survived replacement")
				}
				if ref.releases != 0 || owned.Value != ref || !owned.Owned {
					t.Fatal("initialization released or modified child ownership")
				}
				// Detach even on failure so the test itself never releases a borrowed
				// payload. The owner must still release once after output cleanup.
				delete(out.Opaque, "replacement")
				delete(out.Structs, "replacement")
				delete(out.StringLists, "replacement")
				out.ClearColumns()
				if ref.releases != 0 {
					t.Fatal("output cleanup released child payload")
				}
				if kind == "list" {
					if string(input.StringLists["replacement"].Values.Data) != "ab" {
						t.Fatal("list child modified")
					}
				} else {
					input.ReleaseOwned()
					if ref.releases != 1 {
						t.Fatalf("child release count=%d", ref.releases)
					}
				}
			})
		}
	}
}

// Inject after a successful replacement batch so the next read must unwind
// the previous output reservation as well as retained partition state.
type replacementOrderReviewErrorStream struct {
	source.BatchStream
	err error
}

func (s *replacementOrderReviewErrorStream) Next(context.Context, *memory.Batch) (bool, error) {
	return false, s.err
}
