package lazy

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/optimizer"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
)

const decimalExpressionRows = 143

// decimalExpressionValue deliberately includes coefficients outside int64 and
// float64's exact integer range. Expected values never use a floating conversion.
func decimalExpressionValue(row int, column string) (memory.Decimal128, bool) {
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	values := [...]memory.Decimal128{max, max.Negate(), {}, decimal128.FromI64(1), decimal128.FromI64(-1), decimal128.New(1, 17)}
	switch column {
	case "amount":
		return values[row%len(values)], row%5 != 1
	case "fallback":
		return values[(row+3)%len(values)], row%7 != 2
	default:
		return decimal128.FromI64(int64(row) - 70), true
	}
}

func decimalExpressionCondition(row int) bool { return row%4 != 0 && row%3 == 0 }

func writeDecimalExpressionParquet(t *testing.T) string {
	t.Helper()
	fields := schema.FieldList{
		schema.NewInt64Node("id", arrowparquet.Repetitions.Required, -1),
		schema.NewBooleanNode("condition", arrowparquet.Repetitions.Optional, -1),
	}
	for _, column := range []struct {
		name             string
		precision, scale int32
	}{{"amount", 38, 12}, {"fallback", 38, 12}, {"other_scale", 38, 11}, {"other_precision", 37, 12}} {
		node, err := schema.NewPrimitiveNodeLogical(column.name, arrowparquet.Repetitions.Optional, schema.NewDecimalLogicalType(column.precision, column.scale), arrowparquet.Types.FixedLenByteArray, 16, -1)
		if err != nil {
			t.Fatal(err)
		}
		fields = append(fields, node)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "decimal-expressions.parquet")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := file.NewParquetWriter(out, root, file.WithWriterProps(arrowparquet.NewWriterProperties(
		arrowparquet.WithDictionaryDefault(true), arrowparquet.WithDataPageVersion(arrowparquet.DataPageV2),
		arrowparquet.WithDataPageSize(128), arrowparquet.WithBatchSize(7),
	)))
	t.Cleanup(func() { _ = writer.Close() })
	offset := 0
	for _, rows := range []int{0, 73, 3, 67} {
		group := writer.AppendRowGroup()
		for index, field := range fields {
			column, err := group.NextColumn()
			if err != nil {
				t.Fatal(err)
			}
			defs := make([]int16, rows)
			var ints []int64
			var bools []bool
			var decimals []arrowparquet.FixedLenByteArray
			for i := 0; i < rows; i++ {
				row := offset + i
				switch index {
				case 0:
					ints = append(ints, int64(row))
				case 1:
					if row%4 != 0 {
						defs[i] = 1
						bools = append(bools, row%3 == 0)
					}
				default:
					value, valid := decimalExpressionValue(row, field.Name())
					if valid {
						defs[i] = 1
						data := make([]byte, 16)
						binary.BigEndian.PutUint64(data[:8], uint64(value.HighBits()))
						binary.BigEndian.PutUint64(data[8:], value.LowBits())
						decimals = append(decimals, data)
					}
				}
			}
			switch c := column.(type) {
			case *file.Int64ColumnChunkWriter:
				_, err = c.WriteBatch(ints, nil, nil)
			case *file.BooleanColumnChunkWriter:
				_, err = c.WriteBatch(bools, defs, nil)
			case *file.FixedLenByteArrayColumnChunkWriter:
				_, err = c.WriteBatch(decimals, defs, nil)
			default:
				t.Fatalf("unexpected writer %T", column)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := column.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := group.Close(); err != nil {
			t.Fatal(err)
		}
		offset += rows
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func decimalExpressionOptions(bounded bool) engine.Options {
	opts := engine.DefaultOptions()
	opts.Workers = 1
	opts.MemoryLimitBytes = 2 << 20
	opts.Pipeline = pipeline.Options{Workers: 1, TargetMorselBytes: 8 << 10, MaxMorselBytes: 16 << 10, MaxInFlightBytes: 32 << 10, MinMorselRows: 1, MaxMorselRows: 65, Ordering: pipeline.OrderingStable}
	if bounded {
		opts.ExecutionPolicy = engine.ExecutionBounded
	}
	return opts
}

type decimalExpressionExpected struct {
	name  string
	value func(int) (memory.Decimal128, bool)
}

func decimalExpressionColumn(name, original string) decimalExpressionExpected {
	return decimalExpressionExpected{name, func(row int) (memory.Decimal128, bool) { return decimalExpressionValue(row, original) }}
}

func decimalExpressionConditionals() ([]expr.NamedExpr, []decimalExpressionExpected) {
	expressions := []expr.NamedExpr{
		Col("id").Alias("id"),
		If(Col("condition"), Col("amount"), Col("fallback")).Alias("if_value"),
		Case([]expr.WhenThen{{When: Col("condition"), Then: Col("amount")}, {When: Col("amount").IsNull(), Then: Col("fallback")}}, Col("amount")).Alias("case_value"),
		Case([]expr.WhenThen{{When: Col("condition"), Then: Col("amount")}}, nil).Alias("case_null"),
		Coalesce(Col("amount"), Col("fallback")).Alias("coalesced"),
		Coalesce(Case([]expr.WhenThen{{When: Col("condition"), Then: Col("amount")}}, nil), Col("fallback")).Alias("nested"),
	}
	expected := []decimalExpressionExpected{
		{"if_value", func(row int) (memory.Decimal128, bool) {
			if decimalExpressionCondition(row) {
				return decimalExpressionValue(row, "amount")
			}
			return decimalExpressionValue(row, "fallback")
		}},
		{"case_value", func(row int) (memory.Decimal128, bool) {
			value, valid := decimalExpressionValue(row, "amount")
			if decimalExpressionCondition(row) || valid {
				return value, valid
			}
			return decimalExpressionValue(row, "fallback")
		}},
		{"case_null", func(row int) (memory.Decimal128, bool) {
			value, valid := decimalExpressionValue(row, "amount")
			return value, valid && decimalExpressionCondition(row)
		}},
		{"coalesced", func(row int) (memory.Decimal128, bool) {
			if value, valid := decimalExpressionValue(row, "amount"); valid {
				return value, true
			}
			return decimalExpressionValue(row, "fallback")
		}},
		{"nested", func(row int) (memory.Decimal128, bool) {
			if value, valid := decimalExpressionValue(row, "amount"); decimalExpressionCondition(row) && valid {
				return value, true
			}
			return decimalExpressionValue(row, "fallback")
		}},
	}
	return expressions, expected
}

// assertDecimalExpressionBatch checks both native coefficients and an owned
// Arrow export, including logical precision/scale and per-row validity.
func assertDecimalExpressionBatch(t *testing.T, schema memory.Schema, batch *memory.Batch, expected []decimalExpressionExpected) []int64 {
	t.Helper()
	record, err := memory.ArrowRecord(arrowmem.DefaultAllocator, schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	defer record.Release()
	ids := batch.Int64s["id"].Values
	for _, want := range expected {
		field, ok := schema.Field(want.name)
		if !ok || field.Type != memory.TypeDecimal || field.DecimalPrecision != 38 || field.DecimalScale != 12 || !field.Nullable {
			t.Fatalf("%s lost decimal metadata: %+v", want.name, field)
		}
		indices := record.Schema().FieldIndices(want.name)
		if len(indices) != 1 {
			t.Fatalf("Arrow column %q indices=%v", want.name, indices)
		}
		arrowField := record.Schema().Field(indices[0])
		if !arrow.TypeEqual(arrowField.Type, &arrow.Decimal128Type{Precision: 38, Scale: 12}) || !arrowField.Nullable {
			t.Fatalf("Arrow field=%v", arrowField)
		}
		values, ok := record.Column(indices[0]).(*array.Decimal128)
		if !ok {
			t.Fatalf("Arrow decimal array=%T", record.Column(indices[0]))
		}
		native := batch.Decimals[want.name]
		if err := native.ValidateField(field, batch.Rows); err != nil {
			t.Fatal(err)
		}
		for row, id := range ids {
			value, valid := want.value(int(id))
			if native.IsValid(row) != valid || values.IsValid(row) != valid || valid && (native.Values[row] != value || values.Value(row) != value) {
				t.Fatalf("%s id=%d native=(%v,%v) Arrow=(%v,%v) want=(%v,%v)", want.name, id, native.Values[row], native.IsValid(row), values.Value(row), values.IsValid(row), value, valid)
			}
		}
	}
	return ids
}

func TestParquetDecimal128LazyExpressionCollect(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	expressions, conditionalExpected := decimalExpressionConditionals()
	for _, bounded := range []bool{false, true} {
		mode := "auto"
		if bounded {
			mode = "bounded"
		}
		t.Run(mode, func(t *testing.T) {
			session := NewSession(decimalExpressionOptions(bounded))
			defer session.Close()
			scan, err := session.ScanParquet(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name     string
				query    LazyFrame
				expected []decimalExpressionExpected
				keep     func(int) bool
			}{
				{"plain", scan.Select("id", "amount"), []decimalExpressionExpected{decimalExpressionColumn("amount", "amount")}, nil},
				{"alias", scan.SelectExprs(Col("id").Alias("id"), Col("amount").Alias("renamed")), []decimalExpressionExpected{decimalExpressionColumn("renamed", "amount")}, nil},
				{"with_columns", scan.Select("id", "amount", "fallback").WithColumns(Coalesce(Col("amount"), Col("fallback")).Alias("filled")), []decimalExpressionExpected{decimalExpressionColumn("amount", "amount"), decimalExpressionColumn("fallback", "fallback"), {"filled", conditionalExpected[3].value}}, nil},
				{"null_filter", scan.Filter(Col("amount").IsNull()).SelectExprs(Col("id").Alias("id"), Col("amount").Alias("renamed")), []decimalExpressionExpected{decimalExpressionColumn("renamed", "amount")}, func(row int) bool { return row%5 == 1 }},
				{"not_null_filter", scan.Filter(Col("amount").IsNotNull()).Select("id", "amount"), []decimalExpressionExpected{decimalExpressionColumn("amount", "amount")}, func(row int) bool { return row%5 != 1 }},
				{"conditional_null_filter", scan.WithColumns(Coalesce(Col("amount"), Col("fallback")).Alias("filled")).Filter(Col("filled").IsNull()).Select("id", "filled"), []decimalExpressionExpected{{"filled", conditionalExpected[3].value}}, func(row int) bool { return row%5 == 1 && row%7 == 2 }},
				{"replace_column", scan.WithColumns(If(Col("condition"), Col("amount"), Col("fallback")).Alias("amount")).Select("id", "amount"), []decimalExpressionExpected{{"amount", conditionalExpected[0].value}}, nil},
				{"conditionals", scan.SelectExprs(expressions...), conditionalExpected, nil},
				{"selected_conditionals", scan.Filter(expr.And(Col("id").Gt(Lit(62)), Col("id").Lt(Lit(132)))).SelectExprs(expressions...), conditionalExpected, func(row int) bool { return row > 62 && row < 132 }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result, err := tc.query.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer result.Close()
					var got []int64
					for _, batch := range result.batches() {
						got = append(got, assertDecimalExpressionBatch(t, result.Schema(), batch, tc.expected)...)
					}
					var want []int64
					for row := 0; row < decimalExpressionRows; row++ {
						if tc.keep == nil || tc.keep(row) {
							want = append(want, int64(row))
						}
					}
					if !equalI64(got, want) || result.Height() != int64(len(want)) {
						t.Fatalf("selected ids=%v want=%v", got, want)
					}
					if err := result.Close(); err != nil {
						t.Fatal(err)
					}
					if used := session.Engine().Memory().Used(); used != 0 {
						t.Fatalf("Collect leaked %d bytes", used)
					}
				})
			}
		})
	}
}

func TestParquetDecimal128LazyExpressionRejectsUnsafeCoercions(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	session := NewSession(decimalExpressionOptions(false))
	defer session.Close()
	scan, err := session.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, expression := range map[string]expr.Expr{
		"add_decimal":        Col("amount").Add(Col("fallback")),
		"add_integer":        Col("amount").Add(Lit(1)),
		"compare_decimal":    Col("amount").Eq(Col("fallback")),
		"compare_integer":    Col("amount").Eq(Lit(1)),
		"cast_float":         Cast(Col("amount"), memory.TypeFloat64),
		"cast_integer":       Cast(Col("amount"), memory.TypeInt64),
		"cast_to_decimal":    Cast(Col("id"), memory.TypeDecimal),
		"if_scale":           If(Col("condition"), Col("amount"), Col("other_scale")),
		"if_precision":       If(Col("condition"), Col("amount"), Col("other_precision")),
		"if_integer":         If(Col("condition"), Col("amount"), Col("id")),
		"case_scale":         Case([]expr.WhenThen{{When: Col("condition"), Then: Col("amount")}}, Col("other_scale")),
		"case_precision":     Case([]expr.WhenThen{{When: Col("condition"), Then: Col("amount")}}, Col("other_precision")),
		"coalesce_scale":     Coalesce(Col("amount"), Col("other_scale")),
		"coalesce_precision": Coalesce(Col("amount"), Col("other_precision")),
		"coalesce_float":     Coalesce(Col("amount"), F64(1)),
	} {
		t.Run(name, func(t *testing.T) {
			prepared, err := scan.SelectExprs(Named(expression, "unsafe")).Prepare(context.Background())
			if prepared != nil {
				_ = prepared.Close()
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "decimal") {
				t.Fatalf("unsafe decimal coercion accepted or unclear error: %v", err)
			}
			if used := session.Engine().Memory().Used(); used != 0 {
				t.Fatalf("rejected expression leaked %d bytes", used)
			}
		})
	}
}

func TestParquetDecimal128LazyExpressionBoundedLifecycle(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	for _, mode := range []string{"eof", "early_close", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			session := NewSession(decimalExpressionOptions(true))
			defer session.Close()
			scan, err := session.ScanParquet(path)
			if err != nil {
				t.Fatal(err)
			}
			expressions, expected := decimalExpressionConditionals()
			budget, err := session.Engine().BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			reader, err := scan.Filter(Col("id").Gt(Lit(1))).SelectExprs(expressions...).CollectBatches(ctx, BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if !reader.Next(ctx) {
				t.Fatalf("first batch: %v", reader.Err())
			}
			view := reader.Batch()
			series, err := view.Column("coalesced")
			if err != nil {
				t.Fatal(err)
			}
			chunks, err := series.Decimal128Chunks()
			if err != nil || len(chunks) != 1 {
				t.Fatalf("decimal chunks=%d error=%v", len(chunks), err)
			}
			got := append([]int64(nil), assertDecimalExpressionBatch(t, reader.Schema(), reader.batch, expected)...)
			if reader.budget != budget || reader.ownedBudget {
				t.Fatal("stream did not borrow the caller's query budget")
			}
			switch mode {
			case "eof":
				for reader.Next(ctx) {
					got = append(got, assertDecimalExpressionBatch(t, reader.Schema(), reader.batch, expected)...)
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				if len(got) != decimalExpressionRows-2 {
					t.Fatalf("drained rows=%d", len(got))
				}
				for i, id := range got {
					if id != int64(i+2) {
						t.Fatalf("row %d id=%d", i, id)
					}
				}
			case "cancel":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if reader.Next(canceled) || !errors.Is(reader.Err(), context.Canceled) {
					t.Fatalf("cancellation error=%v", reader.Err())
				}
			}
			if err := reader.Close(); err != nil && !(mode == "cancel" && errors.Is(err, context.Canceled)) {
				t.Fatal(err)
			}
			if view.Valid() {
				t.Fatal("borrowed decimal view survived close or next generation")
			}
			if _, _, err := chunks[0].Value(0); !errors.Is(err, ErrDataFrameClosed) {
				t.Fatalf("expired decimal chunk error=%v", err)
			}
			if snapshot := budget.Snapshot(); budget.Used() != 0 || snapshot.PeakBytes > budget.Limit() || snapshot.Closed {
				t.Fatalf("bounded lifecycle accounting=%+v", snapshot)
			}
		})
	}
}

// The expression result is written to the actual query spill store, then read
// after the input stream has closed. This covers decimal output transport, not
// decimal ordering or join execution, which remain outside this slice.
func TestParquetDecimal128LazyExpressionSpillRoundTrip(t *testing.T) {
	path := writeDecimalExpressionParquet(t)
	dir := t.TempDir()
	opts := decimalExpressionOptions(true)
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 1 << 20}
	session := NewSession(opts)
	defer session.Close()
	scan, err := session.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	expressions, expected := decimalExpressionConditionals()
	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	reader, err := scan.Filter(Col("amount").IsNotNull()).SelectExprs(expressions...).CollectBatches(ctx, BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	outputSchema := reader.Schema()
	fingerprint := memory.SchemaFingerprint(outputSchema)
	store, err := budget.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	var handles []memory.SpillHandle
	for reader.Next(ctx) {
		encoded, err := memory.EncodeColumnarRun(ctx, outputSchema, reader.batch, budget.Operator("DecimalTestEncode", memory.MemorySpillBuffer))
		if err != nil {
			t.Fatal(err)
		}
		handle, spillErr := store.(memory.MetadataSpillManager).SpillWithMetadata(ctx, memory.SpillObject{
			Kind: memory.SpillColumnarRun, Codec: memory.ColumnarRunCodec, SchemaFingerprint: fingerprint, Payload: encoded.Payload,
		}, budget.Operator("DecimalTestMetadata", memory.MemoryRetainedState))
		closeErr := encoded.Close()
		if err := errors.Join(spillErr, closeErr); err != nil {
			t.Fatal(err)
		}
		handles = append(handles, handle)
	}
	if err := errors.Join(reader.Err(), reader.Close()); err != nil {
		t.Fatal(err)
	}
	if len(handles) < 2 || store.(*memory.QuerySpillStore).Used() == 0 {
		t.Fatalf("expected several on-disk expression batches, handles=%d", len(handles))
	}
	var got []int64
	for _, handle := range handles {
		restored, err := store.Restore(ctx, handle, fingerprint, budget.Operator("DecimalTestRestore", memory.MemorySpillBuffer))
		if err != nil {
			t.Fatal(err)
		}
		decoded, decodeErr := memory.DecodeColumnarRun(ctx, outputSchema, restored, budget.Operator("DecimalTestDecode", memory.MemoryWorkspace))
		closeErr := restored.Close()
		if err := errors.Join(decodeErr, closeErr); err != nil {
			if decoded != nil {
				_ = decoded.Close()
			}
			t.Fatal(err)
		}
		got = append(got, assertDecimalExpressionBatch(t, outputSchema, decoded.Batch, expected)...)
		if err := errors.Join(decoded.Close(), store.Delete(ctx, handle)); err != nil {
			t.Fatal(err)
		}
	}
	var want []int64
	for row := 0; row < decimalExpressionRows; row++ {
		if row%5 != 1 {
			want = append(want, int64(row))
		}
	}
	if !equalI64(got, want) {
		t.Fatalf("restored selected ids=%v want=%v", got, want)
	}
	if budget.Used() != 0 || budget.Snapshot().PeakBytes > budget.Limit() || store.(*memory.QuerySpillStore).Used() != 0 {
		t.Fatalf("spill accounting=%+v", budget.Snapshot())
	}
	if err := budget.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("spill cleanup entries=%v error=%v", entries, err)
	}
}

func TestParquetDecimal128LazyExpressionAliasPushdown(t *testing.T) {
	session := NewSession(decimalExpressionOptions(false))
	defer session.Close()
	scan, err := session.ScanParquet(writeDecimalExpressionParquet(t))
	if err != nil {
		t.Fatal(err)
	}
	original := scan.SelectExprs(Col("id").Alias("id"), Col("amount").Alias("renamed"), Col("fallback").Alias("backup")).Filter(Col("renamed").IsNull()).plan
	optimized, err := optimizer.Optimize(original)
	if err != nil {
		t.Fatal(err)
	}
	optimized.Plan, _, err = applyPushdown(context.Background(), optimized.Plan)
	if err != nil {
		t.Fatal(err)
	}
	pushed := expressionPushdownScan(optimized.Plan)
	if pushed == nil {
		t.Fatal("optimized plan lost Parquet scan")
	}
	wantProjection := []string{"id", "amount", "fallback"}
	wantPredicate := Col("amount").IsNull().String()
	if !reflect.DeepEqual(pushed.Projection, wantProjection) || !reflect.DeepEqual(pushed.Pushdown.Projection, wantProjection) ||
		pushed.PushedPredicate == nil || pushed.PushedPredicate.String() != wantPredicate || pushed.Pushdown.Predicate != wantPredicate {
		t.Fatalf("alias not pushed to original source columns: projection=%v predicate=%v report=%+v", pushed.Projection, pushed.PushedPredicate, pushed.Pushdown)
	}
	pruned := pushed.Source.Schema()
	if len(pruned.Fields) != len(wantProjection) || len(expressionPushdownScan(original).Source.Schema().Fields) != 6 {
		t.Fatalf("source projection did not prune unused fields independently: %+v", pruned)
	}
	for index, name := range wantProjection {
		field := pruned.Fields[index]
		if field.Name != name {
			t.Fatalf("source column %d=%q want=%q", index, field.Name, name)
		}
		if name != "id" && (field.Type != memory.TypeDecimal || field.DecimalPrecision != 38 || field.DecimalScale != 12 || !field.Nullable) {
			t.Fatalf("source projection lost decimal metadata: %+v", field)
		}
	}
	if pushed.Pushdown.PrunedBytes <= 0 || pushed.Pushdown.SelectedBytes >= pushed.Pushdown.TotalBytes {
		t.Fatalf("source did not report eliminated column reads: %+v", pushed.Pushdown)
	}

	// Direct lowering deliberately bypasses optimizer and source pushdown for
	// the oracle. Check native and Arrow coefficients on both execution paths.
	execute := func(plan logical.Plan) (memory.Schema, []int64) {
		t.Helper()
		ctx := context.Background()
		manager := memory.NewBudgetManager(2 << 20)
		kernels := kernel.New(kernel.ModeScalar)
		src, pending, closers, err := lowerSourcePipeline(ctx, plan, manager, kernels, nil)
		defer func() {
			for i := len(closers) - 1; i >= 0; i-- {
				_ = closers[i].Close()
			}
		}()
		if err != nil {
			t.Fatal(err)
		}
		if pending != nil {
			src, err = physical.NewFilterSource(src, pending)
			if err != nil {
				t.Fatal(err)
			}
		}
		stream, err := source.OpenBatchStream(ctx, src, source.ReadOptions{Kernels: kernels, MemoryManager: manager, BatchRows: 65})
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		batch := memory.NewBatch()
		defer batch.ReleaseOwned()
		var ids []int64
		for {
			ok, err := stream.Next(ctx, batch)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			ids = append(ids, assertDecimalExpressionBatch(t, src.Schema(), batch, []decimalExpressionExpected{
				decimalExpressionColumn("renamed", "amount"), decimalExpressionColumn("backup", "fallback"),
			})...)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if used := manager.Used(); used != 0 {
			t.Fatalf("direct execution leaked %d bytes", used)
		}
		return src.Schema(), ids
	}
	originalSchema, originalIDs := execute(original)
	optimizedSchema, optimizedIDs := execute(optimized.Plan)
	var wantIDs []int64
	for row := 0; row < decimalExpressionRows; row++ {
		if row%5 == 1 {
			wantIDs = append(wantIDs, int64(row))
		}
	}
	if !reflect.DeepEqual(originalSchema.Fields, optimizedSchema.Fields) || !equalI64(originalIDs, wantIDs) || !equalI64(optimizedIDs, originalIDs) {
		t.Fatalf("pushdown changed results: original schema=%+v optimized schema=%+v original ids=%v optimized ids=%v want=%v", originalSchema, optimizedSchema, originalIDs, optimizedIDs, wantIDs)
	}
}
