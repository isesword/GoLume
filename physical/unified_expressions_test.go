package physical

import (
	"context"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"reflect"
	"testing"
)

func TestUnifiedProjectedPredicatesRoundTripThroughFilter(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, dictionary := range []bool{false, true} {
			t.Run(mode.String()+map[bool]string{false: "/plain", true: "/dictionary"}[dictionary], func(t *testing.T) {
				encoding := memory.EncodingView
				if dictionary {
					encoding = memory.EncodingDictionary
				}
				schema := memory.NewSchema(
					memory.Field{Name: "x", Type: memory.TypeInt64},
					memory.Field{Name: "keep", Type: memory.TypeBool, Nullable: true},
					memory.Field{Name: "u", Type: memory.TypeUint64},
					memory.Field{Name: "f", Type: memory.TypeFloat32},
					memory.Field{Name: "s", Type: memory.TypeString, Encoding: encoding},
				)
				batch := memory.NewBatch()
				batch.Rows = 4
				batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 300, 3, 400}}
				batch.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1, 0}, Validity: memory.Bitmap{7}}
				batch.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{1 << 63, math.MaxUint64, 1<<63 + 1, 0}}
				batch.Float32s["f"] = &memory.Float32Vector{Values: []float32{1, math.Nextafter32(1, 2), 1, float32(math.NaN())}}
				if dictionary {
					batch.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0, 1}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Encoding: encoding, DictionaryDomain: "unified-test"}
				} else {
					batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3, 4}, Data: []byte("abab"), Encoding: encoding}
				}
				base, err := inmemory.New(schema, batch)
				if err != nil {
					t.Fatal(err)
				}
				named := []expr.NamedExpr{
					expr.Named(expr.GT(expr.Col("u"), expr.I64(math.MaxInt64)), "large"),
					expr.Named(expr.EQ(expr.Col("f"), expr.F64(1.00000003)), "rounded"),
					expr.Named(expr.If(expr.Col("keep"), expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0)), "checked"),
					expr.Named(expr.Case([]expr.WhenThen{{When: expr.Col("keep"), Then: expr.CastTo(expr.Col("x"), memory.TypeInt8)}}, expr.I64(-1)), "case_checked"),
					expr.Named(expr.Coalesce(expr.Col("keep"), expr.Bool(true)), "known"),
					expr.Named(expr.In(expr.Col("s"), expr.Str("a"), expr.Null(memory.TypeString)), "member"),
				}
				project, err := NewProjectSource(base, named, true)
				if err != nil {
					t.Fatal(err)
				}
				logicalSchema, err := logical.InferSchema(&logical.ExprProjection{Input: &logical.Scan{Source: base}, Exprs: named, IncludeInput: true})
				if err != nil || !reflect.DeepEqual(logicalSchema, project.Schema()) {
					t.Fatalf("logical/physical schema mismatch: %+v %+v err=%v", logicalSchema, project.Schema(), err)
				}
				manager := memory.NewBudgetManager(1 << 20)
				opts := source.ReadOptions{Kernels: kernel.New(mode), MemoryManager: manager}
				stream, err := project.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				out := memory.NewBatch()
				if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
					t.Fatalf("projection ok=%v err=%v", ok, err)
				}
				if !reflect.DeepEqual(out.Int64s["case_checked"].Values, []int64{1, -1, 3, -1}) || !reflect.DeepEqual(out.Int64s["checked"].Values, []int64{1, 0, 3, 0}) || !reflect.DeepEqual(out.Uint32s["large"].Values, []uint32{1, 1, 1, 0}) || !reflect.DeepEqual(out.Uint32s["rounded"].Values, []uint32{0, 0, 0, 0}) {
					t.Fatal("conditional or exact numeric comparison result mismatch")
				}
				if !reflect.DeepEqual(out.Uint32s["known"].Values, []uint32{1, 0, 1, 1}) || out.Uint32s["member"].IsValid(1) || out.Uint32s["member"].IsValid(3) {
					t.Fatal("COALESCE/IN null semantics mismatch")
				}
				if err := stream.Close(); err != nil || manager.Used() != 0 {
					t.Fatalf("projection close err=%v retained=%d", err, manager.Used())
				}
				filtered, err := NewFilterSource(project, expr.And(expr.Col("large"), expr.Col("member")))
				if err != nil {
					t.Fatal(err)
				}
				stream, err = filtered.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
					t.Fatalf("filter ok=%v err=%v", ok, err)
				}
				if out.Rows != 2 || !reflect.DeepEqual(out.Uint64s["u"].Values, []uint64{1 << 63, 1<<63 + 1}) || !reflect.DeepEqual(out.Int64s["checked"].Values, []int64{1, 3}) {
					t.Fatal("projected boolean filter did not compact exact values")
				}
				if dictionary {
					if !reflect.DeepEqual(out.DictStrings["s"].Indices, []uint32{0, 0}) {
						t.Fatal("dictionary binding changed during compaction")
					}
				} else if string(out.Strings["s"].ValueBytes(0)) != "a" || string(out.Strings["s"].ValueBytes(1)) != "a" {
					t.Fatal("plain string values changed during compaction")
				}
				if err := stream.Close(); err != nil || manager.Used() != 0 {
					t.Fatalf("filter close err=%v retained=%d", err, manager.Used())
				}
			})
		}
	}
}

func TestUnifiedNestedFieldSchemasUseBinder(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	base, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []expr.Expr{
		expr.Split(expr.Col("s"), expr.Str(",")),
		expr.RegexExtractAll(expr.Col("s"), expr.Str("(a)?"), expr.I64(1)),
		expr.JSONDecode(expr.Col("s"), memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true})),
	} {
		named := []expr.NamedExpr{expr.Named(value, "nested")}
		want, err := expr.Bind(schema, value)
		if err != nil {
			t.Fatal(err)
		}
		project, err := NewProjectSource(base, named, false)
		if err != nil {
			t.Fatal(err)
		}
		logicalSchema, err := logical.InferSchema(&logical.ExprProjection{Input: &logical.Scan{Source: base}, Exprs: named})
		if err != nil {
			t.Fatal(err)
		}
		field := want.OutputField("nested")
		if len(field.Children) == 0 || !reflect.DeepEqual(project.Schema().Fields[0], field) || !reflect.DeepEqual(logicalSchema.Fields[0], field) {
			t.Fatalf("lost nested schema for %s: bound=%+v logical=%+v physical=%+v", value, field, logicalSchema, project.Schema())
		}
	}
	bad := expr.JSONDecode(expr.I64(1), memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}))
	if _, err := logical.InferSchema(&logical.ExprProjection{Input: &logical.Scan{Source: base}, Exprs: []expr.NamedExpr{expr.Named(bad, "bad")}}); err == nil {
		t.Fatal("logical JSON schema inference skipped input validation")
	}
}

func TestSequentialFiltersExcludeNullRowsBeforeFalliblePredicate(t *testing.T) {
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 300, 400}}
	batch.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{1, 0, 0}, Validity: memory.Bitmap{3}}
	base, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "keep", Type: memory.TypeBool, Nullable: true}), batch)
	fallible := expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0))
	first, err := NewFilterSource(base, expr.Col("keep"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFilterSource(first, fallible)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := second.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil || out.Rows != 1 {
		t.Fatalf("sequential filters ok=%v rows=%d err=%v", ok, out.Rows, err)
	}
	stream.Close()
	merged, err := NewFilterSource(base, expr.And(expr.Col("keep"), fallible))
	if err != nil {
		t.Fatal(err)
	}
	stream, err = merged.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if ok, err := stream.Next(context.Background(), out); ok || err == nil {
		t.Fatalf("SQL AND must demand the right side for NULL left rows: ok=%v err=%v", ok, err)
	}
}

func TestRetainedProjectionErrorPrecedesOuterFalseFilter(t *testing.T) {
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{300}}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	project, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked")}, true)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(project, expr.Bool(false))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(1 << 20)
	opts := testReadOptions()
	opts.MemoryManager = manager
	stream, err := filtered.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); ok || err == nil {
		t.Fatalf("outer filter hid retained projection error: ok=%v err=%v", ok, err)
	}
	if err := stream.Close(); err != nil || manager.Used() != 0 {
		t.Fatalf("failed projection retained resources: err=%v bytes=%d", err, manager.Used())
	}
}
