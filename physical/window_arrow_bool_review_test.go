package physical

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
)

// Exercise packed Arrow Boolean partition and peer keys across both record and
// stream-batch boundaries, including non-byte-aligned slices and NULL partitions.
func TestArrowBoolWindowKeysAndDirectionalFillsReview(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, batchRows := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("mode%d/rows%d", mode, batchRows), func(t *testing.T) {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				schema := arrow.NewSchema([]arrow.Field{{Name: "g", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, {Name: "o", Type: arrow.FixedWidthTypes.Boolean}, {Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
				builder := array.NewRecordBuilder(alloc, schema)
				defer builder.Release()
				for i := -1; i < 9; i++ {
					if i < 3 {
						builder.Field(0).(*array.BooleanBuilder).AppendNull()
					} else {
						builder.Field(0).(*array.BooleanBuilder).Append(i >= 6)
					}
					builder.Field(1).(*array.BooleanBuilder).Append(i >= 0 && i%3 == 2)
					if i >= 0 && i%3 == 1 {
						builder.Field(2).(*array.Int64Builder).Append(int64(i + 10))
					} else {
						builder.Field(2).(*array.Int64Builder).AppendNull()
					}
				}
				record := builder.NewRecordBatch()
				defer record.Release()
				first, second := record.NewSlice(1, 5), record.NewSlice(5, 10)
				defer first.Release()
				defer second.Release()
				input, err := arrowrecord.NewSource(first, second)
				if err != nil {
					t.Fatal(err)
				}
				defer input.Close()
				current := WindowBound{Kind: WindowCurrentRow}
				expressions := []WindowExpression{{Function: WindowRank, Alias: "r"}, {Function: WindowForwardFill, Column: "v", Alias: "f", Frame: WindowFrame{Start: current, End: current}}, {Function: WindowBackwardFill, Column: "v", Alias: "b", Frame: WindowFrame{Start: current, End: current}}}
				fields := []memory.Field{{Name: "r", Type: memory.TypeInt64}, {Name: "f", Type: memory.TypeInt64, Nullable: true}, {Name: "b", Type: memory.TypeInt64, Nullable: true}}
				countFrame := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: current}
				expressions = append(expressions, WindowExpression{Function: WindowCountRows, Alias: "g", Frame: countFrame}, WindowExpression{Function: WindowCountValues, Column: "g", Alias: "o", Frame: countFrame})
				fields = append(fields, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "o", Type: memory.TypeInt64})
				manager := memory.NewEngineBudget(4 << 20)
				query, err := manager.BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}})
				if err != nil {
					t.Fatal(err)
				}
				defer query.Close()
				ctx := memory.WithQueryBudget(context.Background(), query)
				win, err := NewWindowSourceWithOptions(input, []string{"g"}, []SortKey{{Column: "o"}}, expressions, fields, manager, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: batchRows})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				defer out.ClearColumns()
				seen := 0
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for row := 0; row < out.Rows; row++ {
						pos := seen + row
						count := int64(pos%3 + 1)
						if got := out.Int64s["g"].Values[row]; got != count {
							t.Fatalf("count rows row%d got%d want%d", pos, got, count)
						}
						if pos < 3 {
							count = 0
						}
						if got := out.Int64s["o"].Values[row]; got != count {
							t.Fatalf("count bool row%d got%d want%d", pos, got, count)
						}
						if out.Uint32s["g"] != nil || out.Uint32s["o"] != nil || out.Opaque["g"] != nil || out.Opaque["o"] != nil {
							t.Fatal("replaced Boolean key retained stale binding")
						}

						rank := int64(1)
						if pos%3 == 2 {
							rank = 3
						}
						if got := out.Int64s["r"].Values[row]; got != rank {
							t.Fatalf("rank row%d got%d want%d", pos, got, rank)
						}
						for _, name := range []string{"f", "b"} {
							valid := pos%3 != 0
							if name == "b" {
								valid = pos%3 != 2
							}
							v := out.Int64s[name]
							want := int64(pos/3*3 + 11)
							if v.IsValid(row) != valid || (valid && v.Values[row] != want) {
								t.Fatalf("%s row%d got(%d,%t) want(%d,%t)", name, pos, v.Values[row], v.IsValid(row), want, valid)
							}
						}
					}
					seen += out.Rows
				}
				if seen != 9 {
					t.Fatalf("rows=%d want9", seen)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 {
					t.Fatalf("retained bytes=%d", manager.Used())
				}
			})
		}
	}
}

// These low-level regressions deliberately bypass stream normalization so the
// underlying Arrow fallback failures cannot be masked by Boolean decoding.
func TestWindowOpaqueAccumulatorContinuesReview(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeInt64, memory.TypeBool, memory.TypeUint64, memory.TypeFloat64, memory.TypeBinary} {
		t.Run(typ.String(), func(t *testing.T) {
			arrowType := map[memory.DataType]arrow.DataType{memory.TypeInt32: arrow.PrimitiveTypes.Int32, memory.TypeInt64: arrow.PrimitiveTypes.Int64, memory.TypeBool: arrow.FixedWidthTypes.Boolean, memory.TypeUint64: arrow.PrimitiveTypes.Uint64, memory.TypeFloat64: arrow.PrimitiveTypes.Float64, memory.TypeBinary: arrow.BinaryTypes.Binary}[typ]
			b := array.NewBuilder(arrowmem.DefaultAllocator, arrowType)
			b.AppendNull()
			arr := b.NewArray()
			b.Release()
			defer arr.Release()
			input := memory.NewBatch()
			input.Rows = 1
			input.Opaque["payload"] = &memory.OpaqueVector{Rows: 1, Type: typ, Value: arr}
			input.Int64s["id"] = &memory.Int64Vector{Values: []int64{42}}
			acc := newBatchAccumulator(memory.NewSchema(memory.Field{Name: "payload", Type: typ}, memory.Field{Name: "id", Type: memory.TypeInt64}))
			if err := acc.appendRow(input, 0); err != nil {
				t.Fatal(err)
			}
			result, err := acc.finish()
			if err != nil {
				t.Fatal(err)
			}
			defer result.ReleaseOwned()
			if result.Rows != 1 || result.Int64s["id"] == nil || len(result.Int64s["id"].Values) != 1 {
				t.Fatalf("opaque fallback skipped row or later column: rows=%d id=%v", result.Rows, result.Int64s["id"])
			}
		})
	}
}

func TestWindowOpaqueAccumulatorPropagatesLaterErrorReview(t *testing.T) {
	b := array.NewBooleanBuilder(arrowmem.DefaultAllocator)
	b.Append(true)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()
	input := memory.NewBatch()
	input.Rows = 1
	input.Opaque["payload"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeBool, Value: arr}
	acc := newBatchAccumulator(memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeBool}, memory.Field{Name: "missing", Type: memory.TypeInt64}))
	err := acc.appendRow(input, 0)
	// Finish only to release the accumulated Arrow references on both versions.
	result, finishErr := acc.finish()
	if finishErr != nil {
		t.Fatal(finishErr)
	}
	defer result.ReleaseOwned()
	if err == nil {
		t.Fatal("opaque fallback suppressed missing later column error")
	}
}

func TestWindowFutureArrowMetadataTypedNilReview(t *testing.T) {
	b := array.NewBooleanBuilder(arrowmem.DefaultAllocator)
	b.Append(true)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()
	t.Run("primitive_has_no_dictionary", func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("metadata panicked on Arrow primitive with typed-nil dictionary: %v", p)
			}
		}()
		if n, err := futureArrowCloneMetadata(arr.Data(), 0); err != nil || n <= 0 {
			t.Fatalf("bytes=%d err=%v", n, err)
		}
	})
	t.Run("typed_nil_data_returns_error", func(t *testing.T) {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("metadata panicked on typed-nil input: %v", p)
			}
		}()
		var data *array.Data
		if _, err := futureArrowCloneMetadata(data, 0); err == nil {
			t.Fatal("typed nil accepted")
		}
	})
}

type windowOpaqueErrorReviewStream struct {
	batch   *memory.Batch
	next    bool
	failure error
}

func (s *windowOpaqueErrorReviewStream) Next(_ context.Context, dst *memory.Batch) (bool, error) {
	if s.next {
		return false, s.failure
	}
	s.next = true
	bindAll(dst, s.batch)
	dst.Rows = s.batch.Rows
	return true, nil
}
func (s *windowOpaqueErrorReviewStream) Close() error { return nil }
func TestWindowMaterializeOpaqueErrorCleanupReview(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
			builder.Append([]byte("payload"))
			arr := builder.NewArray()
			builder.Release()
			defer arr.Release()
			batch := memory.NewBatch()
			batch.Rows = 1
			batch.Opaque["payload"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeBinary, Value: arr}
			fields := []memory.Field{{Name: "payload", Type: memory.TypeBinary}}
			if missing {
				fields = append(fields, memory.Field{Name: "missing", Type: memory.TypeInt64})
			}
			failure := errors.New("child read failed")
			stream := &windowOpaqueErrorReviewStream{batch: batch, failure: failure}
			scratch := memory.NewBatch()
			defer scratch.ClearColumns()
			result, err := materializeStream(context.Background(), memory.NewSchema(fields...), stream, scratch)
			if result != nil {
				result.ReleaseOwned()
				t.Fatal("unexpected result on error")
			}
			if err == nil || (!missing && !errors.Is(err, failure)) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestWindowFutureArrowDictionaryMetadataReview(t *testing.T) {
	indicesBuilder := array.NewInt8Builder(arrowmem.DefaultAllocator)
	indicesBuilder.Append(0)
	indices := indicesBuilder.NewArray()
	indicesBuilder.Release()
	defer indices.Release()
	valuesBuilder := array.NewStringBuilder(arrowmem.DefaultAllocator)
	valuesBuilder.Append("value")
	values := valuesBuilder.NewArray()
	valuesBuilder.Release()
	defer values.Release()
	dictionary := array.NewDictionaryArray(&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, indices, values)
	defer dictionary.Release()
	got, err := futureArrowCloneMetadata(dictionary.Data(), 0)
	if err != nil {
		t.Fatal(err)
	}
	dictionaryBytes, err := futureArrowCloneMetadata(values.Data(), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(1024+len(dictionary.Data().Buffers())*64) + dictionaryBytes
	if got != want {
		t.Fatalf("dictionary accounting got%d want%d", got, want)
	}
}

func TestWindowAccumulatorConcatenateErrorCleanupReview(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	firstBuilder := array.NewInt64Builder(alloc)
	firstBuilder.Append(1)
	first := firstBuilder.NewArray()
	firstBuilder.Release()
	defer first.Release()
	secondBuilder := array.NewBooleanBuilder(alloc)
	secondBuilder.Append(true)
	second := secondBuilder.NewArray()
	secondBuilder.Release()
	defer second.Release()
	acc := newBatchAccumulator(memory.NewSchema(memory.Field{Name: "mixed", Type: memory.TypeArrow}, memory.Field{Name: "other", Type: memory.TypeArrow}))
	for _, arr := range []arrow.Array{first, second} {
		batch := memory.NewBatch()
		batch.Rows = 1
		batch.Opaque["mixed"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: arr}
		batch.Opaque["other"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: first}
		if err := acc.appendRow(batch, 0); err != nil {
			t.Fatal(err)
		}
	}
	result, err := acc.finish()
	if result != nil {
		result.ReleaseOwned()
		t.Fatal("unexpected mixed-type output")
	}
	if err == nil {
		t.Fatal("expected concatenation error")
	}
}
