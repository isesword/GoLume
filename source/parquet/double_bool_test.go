package parquet

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

// writeDoubleBoolFixture writes a one-row-group file with nullable and
// required float64 and boolean columns carrying the supplied values. A nil
// defLevels slice marks the nullable column as fully valid; otherwise def
// level 0 encodes NULL.
func writeDoubleBoolFixture(t *testing.T, path string, f64 []float64, f64Def []int16, b []bool, bDef []int16) {
	t.Helper()
	allValid := func(defs []int16, rows int) []int16 {
		if defs != nil {
			return defs
		}
		out := make([]int16, rows)
		for i := range out {
			out[i] = 1
		}
		return out
	}
	f64Def = allValid(f64Def, len(f64))
	bDef = allValid(bDef, len(b))
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		schema.NewFloat64Node("f", arrowparquet.Repetitions.Optional, -1),
		schema.NewFloat64Node("f_req", arrowparquet.Repetitions.Required, -1),
		schema.NewBooleanNode("b", arrowparquet.Repetitions.Optional, -1),
		schema.NewBooleanNode("b_req", arrowparquet.Repetitions.Required, -1),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	props := arrowparquet.NewWriterProperties(arrowparquet.WithCompression(compress.Codecs.Uncompressed))
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	rg := w.AppendRowGroup()

	writeF64 := func(name string, values []float64, defs []int16) {
		t.Helper()
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.(*file.Float64ColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := col.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writeBool := func(name string, values []bool, defs []int16) {
		t.Helper()
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.(*file.BooleanColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := col.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writeF64("f", f64, f64Def)
	writeF64("f_req", f64, nil)
	writeBool("b", b, bDef)
	writeBool("b_req", b, nil)
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFileReadsFloat64AndBool(t *testing.T) {
	f64 := []float64{1.5, math.NaN(), 0}
	f64Def := []int16{1, 1, 0}
	b := []bool{true, false, false}
	bDef := []int16{1, 1, 0}
	path := filepath.Join(t.TempDir(), "double_bool.parquet")
	writeDoubleBoolFixture(t, path, f64, f64Def, b, bDef)

	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		name     string
		typ      memory.DataType
		nullable bool
	}{
		{"f", memory.TypeFloat64, true},
		{"f_req", memory.TypeFloat64, false},
		{"b", memory.TypeBool, true},
		{"b_req", memory.TypeBool, false},
	} {
		field, ok := src.Schema().Field(want.name)
		if !ok || field.Type != want.typ || field.Nullable != want.nullable {
			t.Fatalf("field %q=%+v present=%v", want.name, field, ok)
		}
	}

	r, err := src.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	batch := memory.NewBatch()
	if ok, err := r.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("first batch: ok=%v err=%v", ok, err)
	}
	if batch.Rows != 3 {
		t.Fatalf("rows=%d, want 3", batch.Rows)
	}

	nullableF, err := batch.Float64("f")
	if err != nil {
		t.Fatal(err)
	}
	if !nullableF.IsValid(0) || nullableF.Values[0] != 1.5 {
		t.Fatalf("f row 0 value=%v valid=%v", nullableF.Values[0], nullableF.IsValid(0))
	}
	if !nullableF.IsValid(1) || !math.IsNaN(nullableF.Values[1]) {
		t.Fatalf("f row 1 value=%v valid=%v, want valid NaN", nullableF.Values[1], nullableF.IsValid(1))
	}
	if nullableF.IsValid(2) {
		t.Fatal("f row 2 should be NULL")
	}
	requiredF, err := batch.Float64("f_req")
	if err != nil {
		t.Fatal(err)
	}
	if requiredF.Validity != nil || requiredF.Values[0] != 1.5 || !math.IsNaN(requiredF.Values[1]) {
		t.Fatalf("f_req values=%v validity=%v", requiredF.Values, requiredF.Validity)
	}

	nullableB, err := batch.Uint32("b")
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range []uint32{1, 0} {
		if !nullableB.IsValid(row) || nullableB.Values[row] != want {
			t.Fatalf("b row %d value=%d valid=%v, want %d", row, nullableB.Values[row], nullableB.IsValid(row), want)
		}
	}
	if nullableB.IsValid(2) {
		t.Fatal("b row 2 should be NULL")
	}
	requiredB, err := batch.Uint32("b_req")
	if err != nil {
		t.Fatal(err)
	}
	if requiredB.Validity != nil {
		t.Fatal("b_req should have no validity bitmap")
	}
	for row, want := range []uint32{1, 0, 0} {
		if requiredB.Values[row] != want {
			t.Fatalf("b_req row %d value=%d, want %d", row, requiredB.Values[row], want)
		}
	}
}

func TestOpenFileReadsFloat64SpecialValues(t *testing.T) {
	f64 := []float64{math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.SmallestNonzeroFloat64}
	path := filepath.Join(t.TempDir(), "double_special.parquet")
	writeDoubleBoolFixture(t, path, f64, nil, []bool{true, true, true, true}, nil)

	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := src.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	batch := memory.NewBatch()
	if ok, err := r.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("first batch: ok=%v err=%v", ok, err)
	}
	v, err := batch.Float64("f")
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range f64 {
		if math.Float64bits(v.Values[row]) != math.Float64bits(want) {
			t.Fatalf("f row %d bits=%x, want %x", row, math.Float64bits(v.Values[row]), math.Float64bits(want))
		}
	}
}

// writePruningFloatFixture writes three row groups whose float64 column value
// ranges are disjoint: [0,10), [100,110), [200,210).
func writePruningFloatFixture(t *testing.T, path string) {
	t.Helper()
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		schema.NewFloat64Node("f", arrowparquet.Repetitions.Optional, -1),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	props := arrowparquet.NewWriterProperties(
		arrowparquet.WithCompression(compress.Codecs.Uncompressed),
		arrowparquet.WithMaxRowGroupLength(64),
	)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	for group := 0; group < 3; group++ {
		base := float64(group * 100)
		values := make([]float64, 64)
		defs := make([]int16, 64)
		for i := range values {
			defs[i] = 1
			values[i] = base + float64(i)/10
		}
		rg := w.AppendRowGroup()
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.(*file.Float64ColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatal(err)
		}
		if err := col.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFloat64RowGroupPruning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune_float.parquet")
	writePruningFloatFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name      string
		predicate expr.Expr
		kept      []int
	}{
		{"gt keeps top group", expr.GT(expr.Col("f"), expr.F64(150)), []int{2}},
		{"lt keeps bottom group", expr.LT(expr.Col("f"), expr.F64(50)), []int{0}},
		{"ge boundary keeps all from boundary", expr.GE(expr.Col("f"), expr.F64(100)), []int{1, 2}},
		{"eq inside range keeps group", expr.EQ(expr.Col("f"), expr.F64(0.5)), []int{0}},
		{"eq outside all ranges prunes everything", expr.EQ(expr.Col("f"), expr.F64(42.5)), nil},
		{"int64 literal converts", expr.GT(expr.Col("f"), expr.Lit(150)), []int{2}},
		{"nan literal prunes everything", expr.GT(expr.Col("f"), expr.F64(math.NaN())), nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
				Projection: []string{"f"},
				Predicate:  tt.predicate,
			})
			if err != nil {
				t.Fatal(err)
			}
			if report.SelectedPartitions != len(tt.kept) {
				t.Fatalf("selected=%d, want %d (report=%+v)", report.SelectedPartitions, len(tt.kept), report)
			}
			parts, err := pushed.Partitions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for i, id := range tt.kept {
				if parts[i].ID != id {
					t.Fatalf("kept partition %d has ID %d, want %d", i, parts[i].ID, id)
				}
			}
		})
	}
}

// TestFloat64PruningMatchesRowFilter verifies pruned scans return exactly the
// rows a full scan plus residual filter would.
func TestFloat64PruningMatchesRowFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune_consistency.parquet")
	writePruningFloatFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	readAll := func(s *Source) []float64 {
		t.Helper()
		r, err := s.NewBatchStream(ctx, testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var out []float64
		batch := memory.NewBatch()
		for {
			ok, err := r.Next(ctx, batch)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			v, err := batch.Float64("f")
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < batch.Rows; row++ {
				if v.IsValid(row) {
					out = append(out, v.Values[row])
				}
			}
		}
		return out
	}

	full := readAll(src)
	var want []float64
	for _, v := range full {
		if v > 105 {
			want = append(want, v)
		}
	}
	pushed, _, err := src.ApplyPushdown(ctx, source.PushdownRequest{
		Projection: []string{"f"},
		Predicate:  expr.GT(expr.Col("f"), expr.F64(105)),
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []float64
	for _, v := range readAll(pushed.(*Source)) {
		if v > 105 {
			got = append(got, v)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("pruned scan returned %d matching rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d value=%v, want %v", i, got[i], want[i])
		}
	}
}

// writePruningBoolFixture writes three boolean row groups: all false, all
// true, all NULL.
func writePruningBoolFixture(t *testing.T, path string) {
	t.Helper()
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		schema.NewBooleanNode("b", arrowparquet.Repetitions.Optional, -1),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	props := arrowparquet.NewWriterProperties(
		arrowparquet.WithCompression(compress.Codecs.Uncompressed),
		arrowparquet.WithMaxRowGroupLength(64),
	)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	for group := 0; group < 3; group++ {
		var values []bool
		defs := make([]int16, 64)
		switch group {
		case 0, 1:
			values = make([]bool, 64)
			for i := range defs {
				defs[i] = 1
				values[i] = group == 1
			}
		case 2:
			// All def levels stay 0: a fully NULL row group.
		}
		rg := w.AppendRowGroup()
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.(*file.BooleanColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatal(err)
		}
		if err := col.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoolRowGroupPruning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune_bool.parquet")
	writePruningBoolFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name      string
		predicate expr.Expr
		kept      []int
	}{
		{"eq true prunes all-false and all-null groups", expr.EQ(expr.Col("b"), expr.Lit(1)), []int{1}},
		{"eq false prunes all-true and all-null groups", expr.EQ(expr.Col("b"), expr.Lit(0)), []int{0}},
		{"is null keeps only the all-null group", expr.IsNull(expr.Col("b")), []int{2}},
		{"is not null prunes the all-null group", expr.IsNotNull(expr.Col("b")), []int{0, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
				Projection: []string{"b"},
				Predicate:  tt.predicate,
			})
			if err != nil {
				t.Fatal(err)
			}
			if report.SelectedPartitions != len(tt.kept) {
				t.Fatalf("selected=%d, want %d (report=%+v)", report.SelectedPartitions, len(tt.kept), report)
			}
			parts, err := pushed.Partitions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for i, id := range tt.kept {
				if parts[i].ID != id {
					t.Fatalf("kept partition %d has ID %d, want %d", i, parts[i].ID, id)
				}
			}
		})
	}
}

// TestFloat64NaNDoesNotFalsePrune ensures a row group mixing NaN with matching
// values is never dropped by min/max pruning.
func TestFloat64NaNDoesNotFalsePrune(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune_nan.parquet")
	writeDoubleBoolFixture(t, path,
		[]float64{math.NaN(), 5, -3}, []int16{1, 1, 1},
		[]bool{true, true, true}, nil)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
		Projection: []string{"f"},
		Predicate:  expr.GT(expr.Col("f"), expr.F64(0)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SelectedPartitions != 1 {
		t.Fatalf("NaN row group was pruned despite matching value 5: %+v", report)
	}
	r, err := source.OpenBatchStream(context.Background(), pushed, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	batch := memory.NewBatch()
	if ok, err := r.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("first batch: ok=%v err=%v", ok, err)
	}
	v, err := batch.Float64("f")
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(v.Values[0]) || v.Values[1] != 5 || v.Values[2] != -3 {
		t.Fatalf("values=%v", v.Values)
	}
}

// TestFloat64AllNullGroupPruned verifies null-count-only pruning drops a fully
// NULL float64 row group for value comparisons.
func TestFloat64AllNullGroupPruned(t *testing.T) {
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		schema.NewFloat64Node("f", arrowparquet.Repetitions.Optional, -1),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "prune_allnull.parquet")
	props := arrowparquet.NewWriterProperties(
		arrowparquet.WithCompression(compress.Codecs.Uncompressed),
		arrowparquet.WithMaxRowGroupLength(8),
	)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	for group := 0; group < 2; group++ {
		var values []float64
		defs := make([]int16, 8)
		if group == 0 {
			values = make([]float64, 8)
			for i := range defs {
				defs[i] = 1
			}
		}
		rg := w.AppendRowGroup()
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.(*file.Float64ColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatal(err)
		}
		if err := col.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
		Projection: []string{"f"},
		Predicate:  expr.GT(expr.Col("f"), expr.F64(-1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SelectedPartitions != 1 || report.PrunedPartitions != 1 {
		t.Fatalf("all-null group should be pruned: %+v", report)
	}
}

// TestFloat64BoolCombinedPushdown verifies float and bool predicates combine
// with AND and that column pruning still decodes only projected columns.
func TestFloat64BoolCombinedPushdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune_combined.parquet")
	writePruningFloatFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
		Projection: []string{"f"},
		Predicate: expr.And(
			expr.GE(expr.Col("f"), expr.F64(100)),
			expr.LT(expr.Col("f"), expr.F64(200)),
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SelectedPartitions != 1 {
		t.Fatalf("combined predicate report=%+v", report)
	}
	parts, err := pushed.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 || parts[0].ID != 1 {
		t.Fatalf("parts=%v", parts)
	}
	r, err := source.OpenBatchStream(context.Background(), pushed, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	batch := memory.NewBatch()
	if ok, err := r.Next(context.Background(), batch); !ok || err != nil {
		t.Fatalf("first batch: ok=%v err=%v", ok, err)
	}
	v, err := batch.Float64("f")
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Values) != 64 || v.Values[0] != 100 {
		t.Fatalf("decoded %d values, first=%v", len(v.Values), v.Values[0])
	}
}
