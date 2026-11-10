package lazy

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

const date32CastDayNS int64 = 86400000000000

func date32CastFixture(t *testing.T) (memory.Schema, *memory.Batch) {
	t.Helper()
	tokens := []string{"1970-01-01", "1969-12-31", "2000-02-29", "0000-01-01", "-0001-01-01", "+10000-01-01", "1900-02-29", "2024-2-01", "invalid null payload"}
	dates := []int32{0, -1, 11016, -719528, -719893, 2932897, math.MinInt32, math.MaxInt32, math.MaxInt32}
	stamps := []int64{0, -1, -date32CastDayNS, -date32CastDayNS - 1, date32CastDayNS - 1, date32CastDayNS, math.MinInt64, math.MaxInt64, math.MaxInt64}
	b := memory.NewBatch()
	b.Rows = len(tokens)
	valid := memory.EnsureBitmap(nil, b.Rows)
	text := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView, Validity: valid}
	ids := make([]int64, b.Rows)
	for i, token := range tokens {
		ids[i] = int64(i)
		text.Data = append(text.Data, token...)
		text.Offsets = append(text.Offsets, uint32(len(text.Data)))
		if i != len(tokens)-1 {
			memory.Set(valid, i)
		}
	}
	b.Strings["text"] = text
	b.Int32s["date"] = &memory.Int32Vector{Values: dates, Validity: valid}
	b.Int64s["stamp"] = &memory.Int64Vector{Values: stamps, Validity: valid}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	return memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "text", Type: memory.TypeString, Nullable: true},
		memory.Field{Name: "date", Type: memory.TypeDate32, Nullable: true},
		memory.Field{Name: "stamp", Type: memory.TypeTimestamp, Nullable: true},
	), b
}

func date32CastQuery(scan LazyFrame) LazyFrame {
	return scan.SelectExprs(
		Named(expr.TryCastTo(Col("text"), memory.TypeDate32), "parsed"),
		Named(expr.CastTo(Col("date"), memory.TypeString), "formatted"),
		Named(expr.CastTo(expr.CastTo(Col("date"), memory.TypeString), memory.TypeDate32), "roundtrip"),
		Named(expr.CastTo(expr.CastTo(Col("date"), memory.TypeInt32), memory.TypeString), "physical"),
		Named(expr.TryCastTo(Col("date"), memory.TypeTimestamp), "midnight"),
		Named(expr.CastTo(Col("stamp"), memory.TypeDate32), "floor"),
		Named(expr.CastTo(expr.Null(memory.TypeString), memory.TypeDate32), "null"),
	)
}

func date32CastCheck(t *testing.T, column func(string) (Series, error), offset int) int {
	t.Helper()
	expected := []struct {
		name   string
		typ    memory.DataType
		values []any
	}{
		{"parsed", memory.TypeDate32, []any{int32(0), int32(-1), int32(11016), int32(-719528), int32(-719893), int32(2932897), nil, nil, nil}},
		{"formatted", memory.TypeString, []any{"1970-01-01", "1969-12-31", "2000-02-29", "0000-01-01", "-0001-01-01", "+10000-01-01", "-5877641-06-23", "+5881580-07-11", nil}},
		{"roundtrip", memory.TypeDate32, []any{int32(0), int32(-1), int32(11016), int32(-719528), int32(-719893), int32(2932897), int32(math.MinInt32), int32(math.MaxInt32), nil}},
		{"physical", memory.TypeString, []any{"0", "-1", "11016", "-719528", "-719893", "2932897", "-2147483648", "2147483647", nil}},
		{"midnight", memory.TypeTimestamp, []any{int64(0), -date32CastDayNS, int64(11016) * date32CastDayNS, nil, nil, nil, nil, nil, nil}},
		{"floor", memory.TypeDate32, []any{int32(0), int32(-1), int32(-1), int32(-2), int32(0), int32(1), int32(-106752), int32(106751), nil}},
		{"null", memory.TypeDate32, []any{nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	}
	count := -1
	for _, want := range expected {
		s, err := column(want.name)
		if err != nil {
			t.Fatal(err)
		}
		if s.Type() != want.typ {
			t.Fatalf("%s type=%v want=%v", want.name, s.Type(), want.typ)
		}
		values, err := s.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		if count < 0 {
			count = len(values)
		}
		if len(values) != count || offset+count > len(want.values) || !reflect.DeepEqual(values, want.values[offset:offset+count]) {
			t.Fatalf("%s offset=%d got=%v want=%v", want.name, offset, values, want.values)
		}
		// Inspect native logical chunks too: ToAnySlice alone does not prove that
		// output dtypes and validity survive source materialization and streaming.
		if want.typ == memory.TypeDate32 {
			chunks, err := s.Date32Chunks()
			if err != nil {
				t.Fatal(err)
			}
			row := offset
			for _, chunk := range chunks {
				for i := 0; i < chunk.Len(); i++ {
					v, valid, err := chunk.Value(i)
					w := want.values[row]
					if err != nil || valid != (w != nil) || valid && v != w.(int32) {
						t.Fatalf("%s native row=%d value=%d valid=%v err=%v", want.name, row, v, valid, err)
					}
					row++
				}
			}
			if row != offset+count {
				t.Fatal("native length", row)
			}
		}
	}
	return count
}

func TestDate32CastSourceAndExecutionPaths(t *testing.T) {
	schema, batch := date32CastFixture(t)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	record, err := memory.ArrowRecord(arrowmem.DefaultAllocator, schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	defer record.Release()
	for _, bounded := range []bool{false, true} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("bounded=%v/mode=%v", bounded, mode), func(t *testing.T) {
				opts := decimalExpressionOptions(bounded)
				opts.KernelMode = mode
				session := NewSession(opts)
				defer session.Close()
				scan := session.FromSource(src)
				arrowScan, err := session.ScanArrow(record)
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "source.parquet")
				if _, err := scan.SinkParquet(context.Background(), path, ParquetSinkOptions{}); err != nil {
					t.Fatal(err)
				}
				disk, err := session.ScanParquet(path)
				if err != nil {
					t.Fatal(err)
				}
				for name, input := range map[string]LazyFrame{"memory": scan, "arrow": arrowScan, "parquet": disk} {
					t.Run(name, func(t *testing.T) {
						q := date32CastQuery(input)
						frame, err := q.Collect(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						if n := date32CastCheck(t, frame.Column, 0); n != 9 {
							t.Fatal("collect rows", n)
						}
						frame.Close()
						reader, err := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 128, MaxBatchBytes: 4096})
						if err != nil {
							t.Fatal(err)
						}
						n := 0
						for reader.Next(context.Background()) {
							n += date32CastCheck(t, reader.Batch().Column, n)
						}
						if err := reader.Err(); err != nil {
							t.Fatal(err)
						}
						reader.Close()
						reader.Close()
						if n != 9 {
							t.Fatal("stream rows", n)
						}
						output := filepath.Join(t.TempDir(), "cast.parquet")
						if _, err := q.SinkParquet(context.Background(), output, ParquetSinkOptions{}); err != nil {
							t.Fatal(err)
						}
						reread, err := session.ScanParquet(output)
						if err != nil {
							t.Fatal(err)
						}
						frame, err = reread.Collect(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						if n := date32CastCheck(t, frame.Column, 0); n != 9 {
							t.Fatal("sink rows", n)
						}
						frame.Close()
						if used := session.Engine().Memory().Used(); used != 0 {
							t.Fatal("memory leaked", used)
						}
					})
				}
			})
		}
	}
}

func TestDate32CastErrorDomains(t *testing.T) {
	schema, batch := date32CastFixture(t)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	for _, bounded := range []bool{false, true} {
		session := NewSession(decimalExpressionOptions(bounded))
		defer session.Close()
		scan := session.FromSource(src)
		for name, bad := range map[string]expr.Expr{
			"syntax":   expr.CastTo(expr.Str("1900-02-29"), memory.TypeDate32),
			"overflow": expr.CastTo(expr.CastTo(expr.I64(math.MaxInt32), memory.TypeDate32), memory.TypeTimestamp),
		} {
			predicate := expr.IsNotNull(bad)
			for label, q := range map[string]LazyFrame{
				"earlier_filter": scan.Filter(Col("id").Lt(Lit(0))).Filter(predicate),
				"case":           scan.SelectExprs(Named(expr.If(expr.Bool(false), bad, expr.Null(map[string]memory.DataType{"syntax": memory.TypeDate32, "overflow": memory.TypeTimestamp}[name])), "safe")),
				"dead":           scan.WithColumns(Named(bad, "unused")).Select("id"),
			} {
				t.Run(fmt.Sprintf("safe/%s/%s/bounded=%v", name, label, bounded), func(t *testing.T) {
					frame, err := q.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					frame.Close()
					if session.Engine().Memory().Used() != 0 {
						t.Fatal("safe leaked")
					}
				})
			}
			for label, q := range map[string]LazyFrame{
				"demanded":     scan.SelectExprs(Named(bad, "bad")),
				"later_filter": scan.Filter(predicate).Filter(Col("id").Lt(Lit(0))),
				"eager_and":    scan.Filter(expr.And(expr.Bool(false), predicate)),
			} {
				t.Run(fmt.Sprintf("error/%s/%s/bounded=%v", name, label, bounded), func(t *testing.T) {
					frame, err := q.Collect(context.Background())
					frame.Close()
					if err == nil {
						t.Fatal("strict error suppressed")
					}
					reader, err := q.CollectBatches(context.Background(), BatchOptions{})
					if err == nil {
						for reader.Next(context.Background()) {
						}
						err = reader.Err()
						reader.Close()
					}
					if err == nil {
						t.Fatal("stream error suppressed")
					}
					if session.Engine().Memory().Used() != 0 {
						t.Fatal("error leaked")
					}
				})
			}
		}
		// A poison payload in a SQL NULL is never parsed by a strict cast.
		q := scan.Filter(Col("id").Eq(Lit(8))).SelectExprs(Named(expr.CastTo(Col("text"), memory.TypeDate32), "date"))
		frame, err := q.Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		s, err := frame.Column("date")
		if err != nil {
			t.Fatal(err)
		}
		values, err := s.ToAnySlice()
		if err != nil || !reflect.DeepEqual(values, []any{nil}) {
			t.Fatal(values, err)
		}
		frame.Close()
		if session.Engine().Memory().Used() != 0 {
			t.Fatal("null leaked")
		}
	}
}

func TestDate32CastIntegerBounds(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 6
	b.Int64s["days"] = &memory.Int64Vector{Values: []int64{math.MinInt32, math.MaxInt32, math.MinInt32 - 1, math.MaxInt32 + 1, 0, -1}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "days", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, bounded := range []bool{false, true} {
		session := NewSession(decimalExpressionOptions(bounded))
		frame, err := session.FromSource(src).SelectExprs(Named(expr.TryCastTo(Col("days"), memory.TypeDate32), "date")).Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		s, err := frame.Column("date")
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.ToAnySlice()
		want := []any{int32(math.MinInt32), int32(math.MaxInt32), nil, nil, int32(0), int32(-1)}
		if err != nil || s.Type() != memory.TypeDate32 || !reflect.DeepEqual(got, want) {
			t.Fatal("integer date bounds", got, err)
		}
		frame.Close()
		if session.Engine().Memory().Used() != 0 {
			t.Fatal("integer bounds leaked")
		}
		session.Close()
	}
}

func TestDate32CastStreamLifecycle(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		for _, mode := range []string{"late_error", "early_close", "cancel"} {
			t.Run(fmt.Sprintf("%s/bounded=%v", mode, bounded), func(t *testing.T) {
				b := memory.NewBatch()
				b.Rows = 140
				v := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
				for i := 0; i < b.Rows; i++ {
					token := "1969-12-31"
					if i == 100 && mode == "late_error" {
						token = "1900-02-29"
					}
					v.Data = append(v.Data, token...)
					v.Offsets = append(v.Offsets, uint32(len(v.Data)))
				}
				b.Strings["text"] = v
				src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "text", Type: memory.TypeString}), b)
				if err != nil {
					t.Fatal(err)
				}
				session := NewSession(decimalExpressionOptions(bounded))
				defer session.Close()
				q := session.FromSource(src).SelectExprs(Named(expr.CastTo(Col("text"), memory.TypeDate32), "date"))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reader, err := q.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 256, MaxBatchBytes: 1024})
				if err != nil {
					t.Fatal(err)
				}
				rows := 0
				for reader.Next(ctx) {
					s, err := reader.Batch().Column("date")
					if err != nil {
						t.Fatal(err)
					}
					values, err := s.ToAnySlice()
					if err != nil {
						t.Fatal(err)
					}
					for _, value := range values {
						if value != int32(-1) {
							t.Fatal("early value", value)
						}
						rows++
					}
					if mode == "early_close" {
						break
					}
					if mode == "cancel" {
						cancel()
					}
				}
				if mode == "late_error" && (rows == 0 || rows >= b.Rows || reader.Err() == nil) {
					t.Fatal("late error contract", rows, reader.Err())
				}
				if mode == "early_close" && reader.Err() != nil {
					t.Fatal(reader.Err())
				}
				if mode == "cancel" && reader.Err() == nil {
					t.Fatal("cancellation suppressed")
				}
				reader.Close()
				reader.Close()
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("stream lifecycle leaked")
				}
			})
		}
	}
}
