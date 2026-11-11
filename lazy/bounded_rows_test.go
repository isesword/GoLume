package lazy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func boundedRowsBound(offset int64) RowBound {
	if offset < 0 {
		return Preceding(-offset)
	}
	if offset > 0 {
		return Following(offset)
	}
	return CurrentRow
}

// The oracle enumerates partition positions, comparing relative offsets rather
// than adding them to row numbers. It is independent of all frame executors and
// remains well-defined for MaxInt64 bounds.
func boundedRowsOracle(values reflect.Value, valid []bool, id int, low, high int64, op string) any {
	var best any
	var count int64
	for j := id / 7 * 7; j < (id/7+1)*7; j++ {
		delta := int64(j - id)
		if delta < low || delta > high {
			continue
		}
		if op == "rows" {
			count++
			continue
		}
		if !valid[j] {
			continue
		}
		count++
		x := values.Index(j % 7).Interface()
		if best == nil || op == "min" && boundedRowsLess(x, best) || op == "max" && boundedRowsLess(best, x) {
			best = x
		}
	}
	if op == "rows" || op == "count" {
		return count
	}
	return best
}
func boundedRowsLess(a, b any) bool {
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	switch x.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return x.Int() < y.Int()
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return x.Uint() < y.Uint()
	default:
		f, g := x.Float(), y.Float()
		// Existing numeric ordering puts NaN after all ordinary values, retaining
		// the first equal value (including the sign of zero).
		return !math.IsNaN(f) && (math.IsNaN(g) || f < g)
	}
}
func boundedRowsEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	if x.Kind() == reflect.Float32 || x.Kind() == reflect.Float64 {
		f, g := x.Float(), y.Float()
		return math.IsNaN(f) && math.IsNaN(g) || f == g && (f != 0 || math.Signbit(f) == math.Signbit(g))
	}
	return reflect.DeepEqual(a, b)
}

func TestBoundedRowsIndependentNumericOracle(t *testing.T) {
	cases := []struct {
		typ       memory.DataType
		values    any
		vectorMap string
	}{
		{memory.TypeInt8, []int8{127, -128, 0, 0, 42, -1, 5}, "Int8s"},
		{memory.TypeInt16, []int16{32767, -32768, 0, 0, 42, -1, 5}, "Int16s"},
		{memory.TypeInt32, []int32{math.MaxInt32, math.MinInt32, 0, 0, 42, -1, 5}, "Int32s"},
		{memory.TypeInt64, []int64{math.MaxInt64, math.MinInt64, 0, 0, 1<<53 + 1, -1, 5}, "Int64s"},
		{memory.TypeUint8, []uint8{255, 0, 1, 1, 42, 7, 5}, "Uint8s"},
		{memory.TypeUint16, []uint16{65535, 0, 1, 1, 42, 7, 5}, "Uint16s"},
		{memory.TypeUint32, []uint32{math.MaxUint32, 0, 1, 1, 42, 7, 5}, "Uint32s"},
		{memory.TypeUint64, []uint64{math.MaxUint64, 0, 1<<63 + 1, 1<<63 + 1, 1<<53 + 1, 7, 5}, "Uint64s"},
		{memory.TypeFloat32, []float32{float32(math.Copysign(0, -1)), 0, float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), 3, -2}, "Float32s"},
		{memory.TypeFloat64, []float64{math.Copysign(0, -1), 0, math.NaN(), math.Inf(1), math.Inf(-1), 3, -2}, "Float64s"},
	}
	offsets := []int64{-math.MaxInt64, -4, -1, 0, 1, 4, math.MaxInt64}
	for _, tc := range cases {
		for _, mode := range []string{"resident", "ordered", "external"} {
			for _, shape := range []string{"valid", "nullable", "empty"} {
				t.Run(fmt.Sprintf("%s/%s/%s", tc.typ, mode, shape), func(t *testing.T) {
					order := []int{9, 0, 13, 5, 7, 2, 12, 1, 8, 6, 3, 11, 4, 10}
					if mode == "ordered" {
						order = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
					}
					if shape == "empty" {
						order = nil
					}
					valid := make([]bool, 14)
					for i := range valid {
						valid[i] = shape == "valid" || i < 7 && i != 1 && i != 5
					}
					vals := reflect.ValueOf(tc.values)
					var batches []*memory.Batch
					// Deliberately cut through both the partition boundary and frame edges.
					for start := 0; start < len(order); start += 3 {
						ids := order[start:min(start+3, len(order))]
						b := memory.NewBatch()
						b.Reset(len(ids))
						iv := make([]int64, len(ids))
						groups := make([]int32, len(ids))
						private := make([]int64, len(ids))
						typed := reflect.MakeSlice(vals.Type(), len(ids), len(ids))
						bits := memory.EnsureBitmap(nil, len(ids))
						for row, id := range ids {
							iv[row] = int64(id)
							groups[row] = int32(id / 7)
							private[row] = int64(700 + id)
							typed.Index(row).Set(vals.Index(id % 7))
							if valid[id] {
								memory.Set(bits, row)
							}
						}
						b.Int64s["id"] = &memory.Int64Vector{Values: iv}
						b.Int32s["grp"] = &memory.Int32Vector{Values: groups}
						b.Int64s["__golume_window_future_0"] = &memory.Int64Vector{Values: private}
						b.Int64s["__golume_window_ordinal"] = &memory.Int64Vector{Values: private}
						m := reflect.ValueOf(b).Elem().FieldByName(tc.vectorMap)
						v := reflect.New(m.Type().Elem().Elem())
						v.Elem().FieldByName("Values").Set(typed)
						if shape != "valid" {
							v.Elem().FieldByName("Validity").Set(reflect.ValueOf(bits))
						}
						m.SetMapIndex(reflect.ValueOf("x"), v)
						batches = append(batches, b)
					}
					schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt32}, memory.Field{Name: "x", Type: tc.typ, Nullable: true}, memory.Field{Name: "__golume_window_future_0", Type: memory.TypeInt64}, memory.Field{Name: "__golume_window_ordinal", Type: memory.TypeInt64})
					src, err := inmemory.New(schema, batches...)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "ordered" {
						src.WithOrdering(source.OrderingColumn{Name: "grp"}, source.OrderingColumn{Name: "id", NullsLast: true})
					}
					session := NewSession(engine.DefaultOptions())
					if mode == "external" {
						session.Close()
						session = boundedJoinSession(t, 512<<10)
					}
					defer func() {
						session.Close()
						if used := session.Engine().Memory().Used(); used != 0 {
							t.Errorf("leaked %d bytes", used)
						}
					}()
					for i, lo := range offsets {
						for _, hi := range offsets[i:] {
							t.Run(fmt.Sprintf("%d_%d", lo, hi), func(t *testing.T) {
								bounds := RowsBetween(boundedRowsBound(lo), boundedRowsBound(hi))
								query := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("id"))), RollingCountRows(bounds, "rows"), RollingCount("x", bounds, "count"), RollingMin("x", bounds, "min"), RollingMax("x", bounds, "max"), RollingMin("x", bounds, "x"), RollingMax("x", bounds, "id"))
								plan, err := query.ExplainPhysical(context.Background())
								if err != nil {
									t.Fatal(err)
								}
								strategy := map[string]string{"resident": "strategy=resident", "ordered": "strategy=ordered-state", "external": "strategy=external-sort-restore"}[mode]
								if !strings.Contains(plan, strategy) {
									t.Fatalf("missing %s: %s", strategy, plan)
								}
								result, err := query.Collect(context.Background())
								// Huge finite frames remain valid logical frames, but a
								// streaming executor may reject impossible allocation
								// sizes before consuming input. Resident still checks
								// exact clipped results for these same frames.
								if err != nil && mode != "resident" && (lo == -math.MaxInt64 || hi == math.MaxInt64 && lo != hi) {
									if !strings.Contains(err.Error(), "capacity") && !strings.Contains(err.Error(), "overflows") {
										t.Fatal(err)
									}
									if used := session.Engine().Memory().Used(); used != 0 {
										t.Fatalf("failed admission leaked %d bytes", used)
									}
									return
								}
								if err != nil {
									t.Fatal(err)
								}
								defer result.Close()
								for name, op := range map[string]string{"rows": "rows", "count": "count", "min": "min", "max": "max", "x": "min", "id": "max", "__golume_window_future_0": "private", "__golume_window_ordinal": "private"} {
									col, err := result.Column(name)
									if err != nil {
										t.Fatal(err)
									}
									got, err := col.ToAnySlice()
									if err != nil {
										t.Fatal(err)
									}
									if len(got) != len(order) {
										t.Fatalf("%s length %d want %d", name, len(got), len(order))
									}
									for row, id := range order {
										var want any
										if op == "private" {
											want = int64(700 + id)
										} else {
											want = boundedRowsOracle(vals, valid, id, lo, hi, op)
										}
										if !boundedRowsEqual(got[row], want) {
											t.Fatalf("%s row id=%d got %v (%T) want %v (%T)", name, id, got[row], got[row], want, want)
										}
									}
								}
							})
						}
					}
				})
			}
		}
	}
}

// Each stream overwrites its own three small buffers on every call. Keeping
// vector bindings across Next without copying therefore corrupts results.
type boundedRowsBorrowedSource struct {
	n, chunk             int
	ordered              bool
	failAt               int
	failure              error
	opens, closes, nexts int
}

func (s *boundedRowsBorrowedSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "grp", Type: memory.TypeInt32})
}
func (s *boundedRowsBorrowedSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.n), Partitions: 1}, nil
}
func (s *boundedRowsBorrowedSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.n)}}, nil
}
func (s *boundedRowsBorrowedSource) Replayability() source.Replayability { return source.ReplayStable }
func (s *boundedRowsBorrowedSource) Ordering() []source.OrderingColumn {
	if !s.ordered {
		return nil
	}
	return []source.OrderingColumn{{Name: "grp"}, {Name: "id", NullsLast: true}}
}
func (s *boundedRowsBorrowedSource) NewBatchStream(_ context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	s.opens++
	return &boundedRowsBorrowedStream{owner: s, ids: make([]int64, s.chunk), values: make([]int64, s.chunk), groups: make([]int32, s.chunk), bits: memory.EnsureBitmap(nil, s.chunk)}, nil
}

type boundedRowsBorrowedStream struct {
	owner       *boundedRowsBorrowedSource
	index       int
	closed      bool
	ids, values []int64
	groups      []int32
	bits        memory.Bitmap
}

func (s *boundedRowsBorrowedStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	s.owner.nexts++
	if s.owner.failure != nil && s.index >= s.owner.failAt {
		return false, s.owner.failure
	}
	b.ClearColumns()
	if s.index == s.owner.n {
		return false, nil
	}
	n := min(s.owner.chunk, s.owner.n-s.index)
	clear(s.bits)
	for j := 0; j < n; j++ {
		id := s.index + j
		if !s.owner.ordered {
			id = s.owner.n - 1 - id
		}
		s.ids[j] = int64(id)
		s.groups[j] = int32(id / 31)
		s.values[j] = int64((id*17)%43 - 21)
		if id%5 != 0 {
			memory.Set(s.bits, j)
		}
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: s.ids[:n]}
	b.Int64s["x"] = &memory.Int64Vector{Values: s.values[:n], Validity: s.bits}
	b.Int32s["grp"] = &memory.Int32Vector{Values: s.groups[:n]}
	b.Reset(n)
	s.index += n
	return true, nil
}
func (s *boundedRowsBorrowedStream) Close() error {
	if !s.closed {
		s.closed = true
		s.owner.closes++
		for i := range s.values {
			s.values[i] = -999999
			s.ids[i] = -999999
		}
	}
	return nil
}

func TestBoundedRowsBorrowedLifecycle(t *testing.T) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, ending := range []string{"complete", "early", "error", "cancel"} {
			t.Run(mode+"/"+ending, func(t *testing.T) {
				src := &boundedRowsBorrowedSource{n: 310, chunk: 3, ordered: mode == "ordered"}
				injected := fmt.Errorf("bounded rows injected read failure")
				if ending == "error" {
					src.failAt = 19
					src.failure = injected
				}
				opts := engine.DefaultOptions()
				if mode == "ordered" {
					opts.ExecutionPolicy = engine.ExecutionBounded
					opts.MemoryLimitBytes = 64 << 10
				}
				session := NewSession(opts)
				if mode == "external" {
					session.Close()
					session = boundedJoinSession(t, 512<<10)
				}
				defer session.Close()
				bounds := RowsBetween(Preceding(2), Following(4))
				query := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("id"))), RollingCountRows(bounds, "rows"), RollingCount("x", bounds, "count"), RollingMin("x", bounds, "min"), RollingMax("x", bounds, "max"), RollingMin("x", RowsBetween(Preceding(4), Preceding(1)), "past_min"), RollingMax("x", RowsBetween(Following(1), Following(3)), "future_max"))
				prepared, err := query.Prepare(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reader, err := prepared.ExecuteBatches(ctx, BatchOptions{})
				if err != nil {
					t.Fatal(err)
				}
				seen := 0
				for reader.Next(ctx) {
					batch := reader.Batch()
					cols := map[string][]any{}
					for _, name := range []string{"id", "rows", "count", "min", "max", "past_min", "future_max"} {
						col, err := batch.Column(name)
						if err != nil {
							t.Fatal(err)
						}
						cols[name], err = col.ToAnySlice()
						if err != nil {
							t.Fatal(err)
						}
					}
					for row, raw := range cols["id"] {
						id := int(raw.(int64))
						expectedID := seen
						if mode != "ordered" {
							expectedID = src.n - 1 - seen
						}
						if id != expectedID {
							t.Fatalf("row %d id=%d want %d", seen, id, expectedID)
						}
						var count, rows int64
						var low, high any
						for j := max(id-2, id/31*31); j <= min(id+4, (id/31+1)*31-1); j++ {
							rows++
							if j%5 == 0 {
								continue
							}
							count++
							v := int64((j*17)%43 - 21)
							if low == nil || v < low.(int64) {
								low = v
							}
							if high == nil || v > high.(int64) {
								high = v
							}
						}
						for name, want := range map[string]any{"rows": rows, "count": count, "min": low, "max": high} {
							if !boundedRowsEqual(cols[name][row], want) {
								t.Fatalf("%s id=%d got %v want %v", name, id, cols[name][row], want)
							}
						}
						for name, span := range map[string][2]int{"past_min": {-4, -1}, "future_max": {1, 3}} {
							var want any
							for j := max(id+span[0], id/31*31); j <= min(id+span[1], (id/31+1)*31-1); j++ {
								if j%5 == 0 {
									continue
								}
								v := int64((j*17)%43 - 21)
								if want == nil || name == "past_min" && v < want.(int64) || name == "future_max" && v > want.(int64) {
									want = v
								}
							}
							if !boundedRowsEqual(cols[name][row], want) {
								t.Fatalf("%s id=%d got %v want %v", name, id, cols[name][row], want)
							}
						}
						seen++
					}
					if ending == "early" {
						break
					}
					if ending == "cancel" {
						cancel()
					}
				}
				runErr := errors.Join(reader.Err(), reader.Close())
				if ending == "error" {
					if !errors.Is(runErr, injected) {
						t.Fatalf("error=%v want injected", runErr)
					}
				} else if ending == "cancel" {
					if !errors.Is(runErr, context.Canceled) {
						t.Fatalf("error=%v want canceled", runErr)
					}
				} else if runErr != nil {
					t.Fatal(runErr)
				}
				if ending == "complete" && seen != src.n {
					t.Fatalf("rows=%d want %d", seen, src.n)
				}
				if src.opens != src.closes {
					t.Errorf("streams opens=%d closes=%d", src.opens, src.closes)
				}
				if used := session.Engine().Memory().Used(); used != 0 {
					t.Errorf("leaked %d bytes before prepared/session close", used)
				}
			})
		}
	}
}

func TestBoundedRowsNestedCountOracle(t *testing.T) {
	for _, kind := range []string{"bool", "list", "struct"} {
		for _, mode := range []string{"resident", "ordered", "external"} {
			for _, shape := range []string{"mixed", "valid", "null", "empty"} {
				for _, pair := range [][2]int64{{-3, -1}, {-1, 2}, {0, 2}, {1, 3}, {3, 3}, {-20, 20}} {
					t.Run(fmt.Sprintf("%s/%s/%s/%d_%d", kind, mode, shape, pair[0], pair[1]), func(t *testing.T) {
						order := []int{3, 0, 6, 1, 4, 7, 2, 5}
						if mode == "ordered" {
							order = []int{0, 1, 2, 3, 4, 5, 6, 7}
						}
						if shape == "empty" {
							order = nil
						}
						valid := []bool{true, false, true, true, false, false, false, false}
						for i := range valid {
							if shape == "valid" {
								valid[i] = true
							}
							if shape == "null" {
								valid[i] = false
							}
						}
						var field memory.Field
						switch kind {
						case "bool":
							field = memory.Field{Name: "x", Type: memory.TypeBool, Nullable: true}
						case "list":
							field = memory.ListStringField("x", true, true)
						case "struct":
							field = memory.StructField("x", true, memory.Field{Name: "child", Type: memory.TypeInt64, Nullable: true})
						}
						schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt32}, field)
						var batches []*memory.Batch
						for _, id := range order {
							b := memory.NewBatch()
							b.Rows = 1
							b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(id)}}
							b.Int32s["grp"] = &memory.Int32Vector{Values: []int32{int32(id / 4)}}
							var bits memory.Bitmap
							if shape != "valid" {
								bits = memory.EnsureBitmap(nil, 1)
								if valid[id] {
									memory.Set(bits, 0)
								}
							}
							switch kind {
							case "bool":
								b.Uint32s["x"] = &memory.Uint32Vector{Values: []uint32{uint32(id % 2)}, Validity: bits}
							case "list":
								// Valid empty lists and lists containing NULL both count as one.
								child := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
								end := uint32(0)
								if id%3 != 0 {
									end = 1
									child.Offsets = []uint32{0, 1}
									child.Data = []byte("x")
									child.Validity = memory.EnsureBitmap(nil, 1)
									if id%2 == 1 {
										memory.Set(child.Validity, 0)
									}
								}
								b.StringLists["x"] = &memory.StringListVector{Offsets: []uint32{0, end}, Values: child, Validity: bits, Encoding: memory.EncodingView}
							case "struct":
								child := memory.NewBatch()
								child.Rows = 1
								childBits := memory.EnsureBitmap(nil, 1)
								if !valid[id] {
									memory.Set(childBits, 0)
								}
								child.Int64s["child"] = &memory.Int64Vector{Values: []int64{999}, Validity: childBits}
								b.Structs["x"] = &memory.StructVector{Rows: 1, Fields: field.Children, Values: child, Validity: bits, Encoding: memory.EncodingView}
							}
							batches = append(batches, b)
						}
						src, err := inmemory.New(schema, batches...)
						if err != nil {
							t.Fatal(err)
						}
						if mode == "ordered" {
							src.WithOrdering(source.OrderingColumn{Name: "grp"}, source.OrderingColumn{Name: "id", NullsLast: true})
						}
						session := NewSession(engine.DefaultOptions())
						if mode == "external" {
							session.Close()
							session = boundedJoinSession(t, 512<<10)
						}
						defer func() {
							session.Close()
							if used := session.Engine().Memory().Used(); used != 0 {
								t.Errorf("leaked %d bytes", used)
							}
						}()
						bounds := RowsBetween(boundedRowsBound(pair[0]), boundedRowsBound(pair[1]))
						query := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("id"))),
							RollingCount("x", bounds, "x"), RollingCount("x", bounds, "count"), RollingCount("x", bounds, "id"), RollingCount("id", bounds, "original_id_count"), RollingCountRows(bounds, "rows"))
						plan, err := query.ExplainPhysical(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						strategy := map[string]string{"resident": "strategy=resident", "ordered": "strategy=ordered-state", "external": "strategy=external-sort-restore"}[mode]
						if !strings.Contains(plan, strategy) {
							t.Fatalf("missing %s: %s", strategy, plan)
						}
						// Repeated execution must neither mutate the source nor retain state.
						for repeat := 0; repeat < 2; repeat++ {
							result, err := query.Collect(context.Background())

							if err != nil {
								t.Fatal(err)
							}
							for _, name := range []string{"x", "count", "id", "original_id_count", "rows"} {
								f, ok := result.Schema().Field(name)
								if !ok || f.Type != memory.TypeInt64 || f.Nullable {
									t.Fatalf("%s field=%+v", name, f)
								}
								col, err := result.Column(name)
								if err != nil {
									t.Fatal(err)
								}
								got, err := col.ToAnySlice()
								if err != nil {
									t.Fatal(err)
								}
								want := make([]any, len(order))
								for row, id := range order {
									var count int64
									for j := id / 4 * 4; j < (id/4+1)*4; j++ {
										delta := int64(j - id)
										if delta >= pair[0] && delta <= pair[1] && (valid[j] || name == "rows" || name == "original_id_count") {
											count++
										}
									}
									want[row] = count
								}
								if !reflect.DeepEqual(got, want) {
									t.Errorf("%s=%v want %v", name, got, want)
								}
							}
							for _, batch := range result.batches() {
								maps := reflect.ValueOf(batch).Elem()
								for i := 0; i < maps.NumField(); i++ {
									m := maps.Field(i)
									if m.Kind() == reflect.Map {
										present := m.MapIndex(reflect.ValueOf("x")).IsValid()
										if present != (maps.Type().Field(i).Name == "Int64s") {
											t.Errorf("alias x binding %s=%t", maps.Type().Field(i).Name, present)
										}
									}
								}
							}
							result.Close()
						}
					})
				}
			}
		}
	}
}

// A low soft limit forces multiple disk runs in sorting and ordinal
// restoration; the hard limit also accounts for simultaneous run metadata.
func TestBoundedRowsExternalMultiRunSpill(t *testing.T) {
	const rows = 31 * 1024
	dir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 16 << 20
	opts.QueryMemorySoftLimitBytes = 512 << 10
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	session := NewSession(opts)
	defer session.Close()
	src := &boundedRowsBorrowedSource{n: rows, chunk: 127}
	bounds := RowsBetween(Preceding(2), Following(4))
	query := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("id"))),
		RollingCountRows(bounds, "rows"), RollingCount("x", bounds, "count"),
		RollingMin("x", RowsBetween(Preceding(4), Preceding(1)), "past_min"),
		RollingMax("x", RowsBetween(Following(1), Following(3)), "future_max"),
		Shift("x", -2, "lead"), BackwardFill("x", "fill"),
		RollingMin("x", bounds, "x"), RollingMax("x", bounds, "id"))
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "strategy=external-sort-restore") {
		t.Fatalf("not external window: %s", plan)
	}
	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	reader, err := query.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 8 << 10, MaxBatchBytes: 32 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen, batches, liveSpillFiles := 0, 0, 0
	for reader.Next(ctx) {
		batches++
		if batches == 1 {
			if err := filepath.WalkDir(dir, func(_ string, entry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if !entry.IsDir() {
					liveSpillFiles++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		batch := reader.Batch()
		cols := map[string][]any{}
		for _, name := range []string{"grp", "rows", "count", "past_min", "future_max", "lead", "fill", "x", "id"} {
			c, err := batch.Column(name)
			if err != nil {
				t.Fatal(err)
			}
			cols[name], err = c.ToAnySlice()
			if err != nil {
				t.Fatal(err)
			}
		}
		for row := 0; row < int(batch.Len()); row++ {
			id := rows - 1 - seen // original source order, independent of overwritten id alias
			if cols["grp"][row] != int32(id/31) {
				t.Fatalf("restore row=%d group=%v", seen, cols["grp"][row])
			}
			for name, span := range map[string][2]int{"rows": {-2, 4}, "count": {-2, 4}, "past_min": {-4, -1}, "future_max": {1, 3}, "x": {-2, 4}, "id": {-2, 4}} {
				var want any
				var count int64
				for j := max(id+span[0], id/31*31); j <= min(id+span[1], (id/31+1)*31-1); j++ {
					if name == "rows" {
						count++
						continue
					}
					if j%5 == 0 {
						continue
					}
					count++
					v := int64((j*17)%43 - 21)
					if want == nil || (name == "past_min" || name == "x") && v < want.(int64) || (name == "future_max" || name == "id") && v > want.(int64) {
						want = v
					}
				}
				if name == "rows" || name == "count" {
					want = count
				}
				if !boundedRowsEqual(cols[name][row], want) {
					t.Fatalf("%s original id=%d got %v want %v", name, id, cols[name][row], want)
				}
			}
			var lead, fill any
			if j := id + 2; j < (id/31+1)*31 && j%5 != 0 {
				lead = int64((j*17)%43 - 21)
			}
			for j := id; j < (id/31+1)*31; j++ {
				if j%5 != 0 {
					fill = int64((j*17)%43 - 21)
					break
				}
			}
			for name, want := range map[string]any{"lead": lead, "fill": fill} {
				if !boundedRowsEqual(cols[name][row], want) {
					t.Fatalf("%s original id=%d got %v want %v", name, id, cols[name][row], want)
				}
			}
			seen++
		}
	}
	if err := errors.Join(reader.Err(), reader.Close()); err != nil {
		t.Fatal(err)
	}
	if seen != rows || batches < 2 || liveSpillFiles < 2 {
		t.Fatalf("rows=%d batches=%d live spill files=%d", seen, batches, liveSpillFiles)
	}
	snapshot := budget.Snapshot()
	if snapshot.SpilledBytes == 0 || snapshot.PeakBytes > budget.Limit() || budget.Used() != 0 {
		t.Fatalf("spill accounting: %+v", snapshot)
	}
	t.Logf("rows=%d batches=%d live_spill_files=%d spilled_bytes=%d peak_bytes=%d", seen, batches, liveSpillFiles, snapshot.SpilledBytes, snapshot.PeakBytes)
	if src.opens != src.closes {
		t.Fatalf("source opens=%d closes=%d", src.opens, src.closes)
	}
	if err := budget.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("spill leftovers=%v error=%v", entries, err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("memory leaked=%d", used)
	}
}
