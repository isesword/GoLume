package physical

import (
	"context"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// boolOrderTestSource returns a two-batch source with an int64 ordering key and
// a nullable bool payload. Bools are physically uint32 0/1 vectors whose schema
// field type is TypeBool, matching every engine source binding.
func boolOrderTestSource(t *testing.T) *inmemory.Source {
	t.Helper()
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeBool, Nullable: true},
	)
	valid := memory.EnsureBitmap(nil, 4)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 3) // row 2 is NULL
	b1 := memory.NewBatch()
	b1.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3}}
	b1.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 1, 2, 0}}
	b1.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1, 0}, Validity: valid}
	b1.Reset(4)
	b2 := memory.NewBatch()
	b2.Int64s["id"] = &memory.Int64Vector{Values: []int64{4, 5, 6}}
	b2.Int64s["key"] = &memory.Int64Vector{Values: []int64{5, 4, 1}}
	b2.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{0, 1, 1}}
	b2.Reset(3)
	src, err := inmemory.New(schema, b1, b2)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// collectBatch materializes every output batch of src into one owned batch.
func collectBatch(t *testing.T, src source.Source) *memory.Batch {
	t.Helper()
	ctx := context.Background()
	stream, err := source.OpenBatchStream(ctx, src, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	scratch := memory.NewBatch()
	defer scratch.ReleaseOwned()
	full, err := materializeStream(ctx, src.Schema(), stream, scratch)
	if err != nil {
		t.Fatal(err)
	}
	return full
}

func checkBoolColumn(t *testing.T, batch *memory.Batch, name string, values []uint32, valid []bool) {
	t.Helper()
	v, err := batch.Uint32(name)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Values) != len(values) {
		t.Fatalf("bool column %q rows=%d want=%d", name, len(v.Values), len(values))
	}
	for i := range values {
		if valid[i] != v.IsValid(i) {
			t.Fatalf("bool column %q row %d valid=%v want=%v", name, i, v.IsValid(i), valid[i])
		}
		if valid[i] && v.Values[i] != values[i] {
			t.Fatalf("bool column %q row %d value=%d want=%d", name, i, v.Values[i], values[i])
		}
	}
}

func TestSortTopKAndLimitGatherBoolPayload(t *testing.T) {
	base := boolOrderTestSource(t)
	for _, tt := range []struct {
		name  string
		make  func() (source.Source, error)
		ids   []int64
		bools []uint32
		valid []bool
	}{
		{
			name: "sort",
			make: func() (source.Source, error) {
				return NewSortSource(base, []SortKey{{Column: "key"}}, nil)
			},
			ids:   []int64{3, 1, 6, 2, 0, 5, 4},
			bools: []uint32{0, 0, 1, 0, 1, 1, 0},
			valid: []bool{true, true, true, false, true, true, true},
		},
		{
			name: "top-k crosses winner batches",
			make: func() (source.Source, error) {
				return NewTopKSourceWithManager(base, 3, []SortKey{{Column: "key"}}, nil)
			},
			ids:   []int64{3, 1, 6},
			bools: []uint32{0, 0, 1},
			valid: []bool{true, true, true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ordered, err := tt.make()
			if err != nil {
				t.Fatal(err)
			}
			out := collectBatch(t, ordered)
			defer out.ReleaseOwned()
			ids, err := out.Int64("id")
			if err != nil {
				t.Fatal(err)
			}
			if !equalInt64s(ids.Values, tt.ids) {
				t.Fatalf("ids=%v want=%v", ids.Values, tt.ids)
			}
			checkBoolColumn(t, out, "b", tt.bools, tt.valid)
		})
	}
}

// TestLimitGatherKeepsBoolPayloadAcrossBatchCut reads the limit stream
// batch-by-batch so the takeRows batch-cut path is verified directly instead of
// through the materializeStream helper shared with Sort.
func TestLimitGatherKeepsBoolPayloadAcrossBatchCut(t *testing.T) {
	base := boolOrderTestSource(t)
	limited, err := NewLimitSource(base, 5)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stream, err := source.OpenBatchStream(ctx, limited, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	dst := memory.NewBatch()
	defer dst.ReleaseOwned()
	var ids []int64
	var valid []bool
	for {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		ids = append(ids, dst.Int64s["id"].Values...)
		b := dst.Uint32s["b"]
		if b == nil {
			t.Fatal("limit dropped the bool payload column")
		}
		for row := 0; row < dst.Rows; row++ {
			valid = append(valid, b.IsValid(row))
		}
	}
	if want := []int64{0, 1, 2, 3, 4}; !equalInt64s(ids, want) {
		t.Fatalf("ids=%v want=%v", ids, want)
	}
	wantValid := []bool{true, true, false, true, true}
	for i := range wantValid {
		if valid[i] != wantValid[i] {
			t.Fatalf("bool row %d valid=%v want=%v", i, valid[i], wantValid[i])
		}
	}
}

func TestSortAndTopKOrderByBoolKey(t *testing.T) {
	base := boolOrderTestSource(t)
	for _, tt := range []struct {
		name string
		make func() (source.Source, error)
		ids  []int64
	}{
		{
			name: "sort nulls first then false then true",
			make: func() (source.Source, error) {
				return NewSortSource(base, []SortKey{{Column: "b"}}, nil)
			},
			ids: []int64{2, 1, 3, 4, 0, 5, 6},
		},
		{
			name: "sort descending keeps nulls first",
			make: func() (source.Source, error) {
				return NewSortSource(base, []SortKey{{Column: "b", Descending: true}}, nil)
			},
			ids: []int64{2, 0, 5, 6, 1, 3, 4},
		},
		{
			name: "top-k",
			make: func() (source.Source, error) {
				return NewTopKSourceWithManager(base, 2, []SortKey{{Column: "b"}}, nil)
			},
			ids: []int64{2, 1},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ordered, err := tt.make()
			if err != nil {
				t.Fatal(err)
			}
			out := collectBatch(t, ordered)
			defer out.ReleaseOwned()
			ids, err := out.Int64("id")
			if err != nil {
				t.Fatal(err)
			}
			if !equalInt64s(ids.Values, tt.ids) {
				t.Fatalf("ids=%v want=%v", ids.Values, tt.ids)
			}
		})
	}
}

func TestExternalSortGathersBoolPayload(t *testing.T) {
	const rows = 4096
	batches := make([]*memory.Batch, 0, rows/512)
	for offset := 0; offset < rows; offset += 512 {
		batch := memory.NewBatch()
		ids := make([]int64, 0, 512)
		keys := make([]int64, 0, 512)
		bools := make([]uint32, 0, 512)
		for i := 0; i < 512 && offset+i < rows; i++ {
			id := int64(offset + i)
			ids = append(ids, id)
			keys = append(keys, (rows-id)%257)
			bools = append(bools, uint32(id%2))
		}
		batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
		batch.Int64s["key"] = &memory.Int64Vector{Values: keys}
		batch.Uint32s["b"] = &memory.Uint32Vector{Values: bools}
		batch.Reset(len(keys))
		batches = append(batches, batch)
	}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeBool},
	), batches...)
	if err != nil {
		t.Fatal(err)
	}
	const hard = int64(64 << 10)
	engineBudget := memory.NewEngineBudget(hard)
	query, err := engineBudget.BeginQuery(memory.BudgetOptions{
		SoftLimitBytes: 48 << 10, HardLimitBytes: hard,
		Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	ordered, err := NewSortSourceWithOptions(src, []SortKey{{Column: "key"}, {Column: "id"}}, engineBudget, SortSourceOptions{Spillable: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), query)
	stream, err := ordered.(*SortSource).NewBatchStream(ctx, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	dst := memory.NewBatch()
	defer dst.ReleaseOwned()
	seen := 0
	prevKey, prevID := int64(-1), int64(-1)
	for {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		ids := dst.Int64s["id"]
		keys := dst.Int64s["key"]
		bools := dst.Uint32s["b"]
		if ids == nil || keys == nil || bools == nil {
			t.Fatalf("external sort output lost a column: ids=%v keys=%v bools=%v", ids != nil, keys != nil, bools != nil)
		}
		for row := 0; row < dst.Rows; row++ {
			if keys.Values[row] < prevKey || keys.Values[row] == prevKey && ids.Values[row] < prevID {
				t.Fatalf("external sort output not ordered at row %d", seen+row)
			}
			prevKey, prevID = keys.Values[row], ids.Values[row]
			if want := uint32(ids.Values[row] % 2); bools.Values[row] != want {
				t.Fatalf("bool payload row %d: id=%d b=%d want=%d", seen+row, ids.Values[row], bools.Values[row], want)
			}
		}
		seen += dst.Rows
	}
	if seen != rows {
		t.Fatalf("external sort rows=%d want=%d", seen, rows)
	}
}
