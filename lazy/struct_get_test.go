package lazy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

type structGetCase struct {
	typ    memory.DataType
	vector string
	value  any
}

func structGetCases() []structGetCase {
	return []structGetCase{
		{memory.TypeInt8, "Int8s", int8(3)}, {memory.TypeInt16, "Int16s", int16(3)}, {memory.TypeInt32, "Int32s", int32(3)}, {memory.TypeInt64, "Int64s", int64(3)},
		{memory.TypeUint8, "Uint8s", uint8(3)}, {memory.TypeUint16, "Uint16s", uint16(3)}, {memory.TypeUint32, "Uint32s", uint32(3)}, {memory.TypeUint64, "Uint64s", uint64(3)},
		{memory.TypeFloat32, "Float32s", float32(3.5)}, {memory.TypeFloat64, "Float64s", float64(3.5)}, {memory.TypeBool, "Uint32s", uint32(1)},
		{memory.TypeDate32, "Int32s", int32(3)}, {memory.TypeDate64, "Int64s", int64(86400000)}, {memory.TypeTimestamp, "Int64s", int64(3000000)},
		{memory.TypeTime32, "Int32s", int32(3)}, {memory.TypeTime64, "Int64s", int64(3000000)}, {memory.TypeDuration, "Int64s", int64(3000000)},
		{memory.TypeString, "", "中.value"}, {memory.TypeList, "string", nil}, {memory.TypeList, "int64", nil},
	}
}

// Fixtures and oracles share only literal input values, never a StructGet result.
func structGetFixture(tc structGetCase, n int) (memory.Schema, *memory.Batch, []any, []string) {
	child := memory.Field{Name: "a.b:中", Type: tc.typ, Nullable: true}
	if tc.typ == memory.TypeString {
		child.Encoding = memory.EncodingView
	}
	if tc.typ == memory.TypeList {
		child = memory.ListStringField(child.Name, true, true)
		if tc.vector == "int64" {
			child = memory.ListInt64Field(child.Name, true, true)
		}
	}
	sibling := memory.Field{Name: "sibling", Type: memory.TypeInt64}
	parent := memory.StructField("root", true, child, sibling)
	children := memory.NewBatch()
	children.Reset(n)
	parentBits, childBits := memory.EnsureBitmap(nil, n), memory.EnsureBitmap(nil, n)
	wants := make([]any, n)
	payloads := make([]string, n)
	siblings := make([]int64, n)
	for i := 0; i < n; i++ {
		if i%7 != 0 {
			memory.Set(parentBits, i)
		}
		if i%5 != 0 {
			memory.Set(childBits, i)
		}
		siblings[i] = int64(i + 10)
	}
	if tc.vector != "" && tc.typ != memory.TypeList {
		m := reflect.ValueOf(children).Elem().FieldByName(tc.vector)
		v := reflect.New(m.Type().Elem().Elem())
		vals := reflect.MakeSlice(v.Elem().FieldByName("Values").Type(), n, n)
		for i := 0; i < n; i++ {
			vals.Index(i).Set(reflect.ValueOf(tc.value))
		}
		v.Elem().FieldByName("Values").Set(vals)
		v.Elem().FieldByName("Validity").Set(reflect.ValueOf(childBits))
		m.SetMapIndex(reflect.ValueOf(child.Name), v)
	} else if tc.typ == memory.TypeString {
		v := &memory.StringViewVector{Offsets: []uint32{0}, Validity: childBits, Encoding: memory.EncodingView}
		for i := 0; i < n; i++ {
			v.Data = append(v.Data, tc.value.(string)...)
			v.Offsets = append(v.Offsets, uint32(len(v.Data)))
		}
		children.Strings[child.Name] = v
	} else {
		offsets := make([]uint32, n+1)
		elements := memory.EnsureBitmap(nil, n*2)
		for i := 0; i < n; i++ {
			offsets[i+1] = uint32((i + 1) * 2)
			memory.Set(elements, i*2)
		}
		if tc.vector == "int64" {
			vals := make([]int64, n*2)
			for i := 0; i < n; i++ {
				vals[i*2] = int64(i + 1)
			}
			children.Int64Lists[child.Name] = &memory.Int64ListVector{Offsets: offsets, Validity: childBits, Values: &memory.Int64Vector{Values: vals, Validity: elements}, Encoding: memory.EncodingView}
		} else {
			v := &memory.StringViewVector{Offsets: []uint32{0}, Validity: elements, Encoding: memory.EncodingView}
			for i := 0; i < n; i++ {
				v.Data = append(v.Data, "中"...)
				v.Offsets = append(v.Offsets, uint32(len(v.Data)), uint32(len(v.Data)))
			}
			children.StringLists[child.Name] = &memory.StringListVector{Offsets: offsets, Validity: childBits, Values: v, Encoding: memory.EncodingView}
		}
	}
	children.Int64s["sibling"] = &memory.Int64Vector{Values: siblings}
	for i := 0; i < n; i++ {
		var value any = tc.value
		if tc.typ == memory.TypeBool {
			value = true
		}
		if tc.typ == memory.TypeList {
			value = []any{"中", nil}
			if tc.vector == "int64" {
				value = []any{int64(i + 1), nil}
			}
		}
		if i%5 == 0 {
			value = nil
		}
		if i%7 != 0 {
			wants[i] = value
			raw, _ := json.Marshal(map[string]any{child.Name: value, "sibling": siblings[i]})
			payloads[i] = string(raw)
		} else {
			payloads[i] = "null"
		}
	}
	b := memory.NewBatch()
	b.Reset(n)
	b.Structs["root"] = &memory.StructVector{Rows: n, Fields: parent.Children, Values: children, Validity: parentBits, Encoding: memory.EncodingView}
	return memory.NewSchema(parent), b, wants, payloads
}
func structGetCheck(t *testing.T, df DataFrame, name string, want []any) {
	t.Helper()
	s, e := df.Column(name)
	if e != nil {
		t.Fatal(e)
	}
	got, e := s.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s got=%#v want=%#v", name, got, want)
	}
}

func TestStructGetEntrypointOracle(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, entry := range []string{"memory", "arrow", "json"} {
			for ci, tc := range structGetCases() {
				t.Run(fmt.Sprintf("%d/%s/%d_%s", mode, entry, ci, tc.typ), func(t *testing.T) {
					schema, b, want, payloads := structGetFixture(tc, 65)
					opts := engine.DefaultOptions()
					opts.KernelMode = mode
					opts.PreparedCacheBytes = 0
					s := NewSession(opts)
					defer func() {
						s.Close()
						if s.Engine().Memory().Used() != 0 {
							t.Fatal("budget leak")
						}
					}()
					var q LazyFrame
					var parent expr.Expr = expr.Col("root")
					switch entry {
					case "memory":
						src, e := inmemory.New(schema, b)
						if e != nil {
							t.Fatal(e)
						}
						q = s.FromSource(src)
					case "arrow":
						rec, e := memory.ArrowRecord(arrowmem.DefaultAllocator, schema, b)
						if e != nil {
							t.Fatal(e)
						}
						sliced := rec.NewSlice(1, 65)
						rec.Release()
						q, e = s.ScanArrow(sliced)
						sliced.Release()
						if e != nil {
							t.Fatal(e)
						}
						want = want[1:]
					case "json":
						ids := make([]int64, len(payloads))
						src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView}), structGetJSONInputBatch(ids, payloads, nil))
						if e != nil {
							t.Fatal(e)
						}
						q = s.FromSource(src)
						parent = expr.JSONDecode(expr.Col("payload"), memory.NewSchema(schema.Fields[0].Children...))
					}
					expectedChild := schema.Fields[0].Children[0]
					if entry == "arrow" {
						inputSchema, err := q.Schema()
						if err != nil {
							t.Fatal(err)
						}
						expectedChild = inputSchema.Fields[0].Children[0]
					}
					q = q.SelectExprs(expr.StructGet(parent, "a.b:中").Alias("out"), expr.StructGet(parent, "a.b:中").Alias("again"), expr.StructGet(parent, "sibling").Alias("sib"))
					outSchema, e := q.Schema()
					if e != nil {
						t.Fatal(e)
					}
					f, _ := outSchema.Field("out")
					if f.Type != tc.typ || !f.Nullable {
						t.Fatal(f)
					}
					if tc.typ == memory.TypeList && !reflect.DeepEqual(f.Children, expectedChild.Children) {
						t.Fatal("list metadata", f)
					}
					for repeat := 0; repeat < 2; repeat++ {
						df, e := q.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						structGetCheck(t, df, "out", want)
						structGetCheck(t, df, "again", want)
						sw := make([]any, len(want))
						for i := range sw {
							id := i
							if entry == "arrow" {
								id++
							}
							if id%7 != 0 {
								sw[i] = int64(id + 10)
							}
						}
						structGetCheck(t, df, "sib", sw)
						df.Close()
					}
					path := filepath.Join(t.TempDir(), "out.arrow")
					if _, e := q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
						t.Fatal(e)
					}
					file, e := os.Open(path)
					if e != nil {
						t.Fatal(e)
					}
					reader, e := ipc.NewFileReader(file)
					if e != nil {
						t.Fatal(e)
					}
					var records []arrow.RecordBatch
					for i := 0; i < reader.NumRecords(); i++ {
						r, e := reader.RecordBatchAt(i)
						if e != nil {
							t.Fatal(e)
						}
						records = append(records, r)
					}
					round, e := s.ScanArrow(records...)
					for _, r := range records {
						r.Release()
					}
					reader.Close()
					file.Close()
					if e != nil {
						t.Fatal(e)
					}
					df, e := round.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					structGetCheck(t, df, "out", want)
					df.Close()
				})
			}
		}
	}
}

func TestStructGetPipelineErrorsLifetime(t *testing.T) {
	schema, b, want, _ := structGetFixture(structGetCase{memory.TypeInt64, "Int64s", int64(3)}, 130)
	src, e := inmemory.New(schema, b)
	if e != nil {
		t.Fatal(e)
	}
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	q := s.FromSource(src).SelectExprs(StructGet(Col("root"), "a.b:中").Alias("value"), expr.Add(expr.StructGet(expr.Col("root"), "a.b:中"), expr.I64(2)).Alias("plus"))
	df, e := q.Filter(expr.GT(expr.Col("plus"), expr.I64(4))).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	var filtered []any
	for _, v := range want {
		if v != nil {
			filtered = append(filtered, int64(5))
		}
	}
	structGetCheck(t, df, "plus", filtered)
	df.Close()
	path := filepath.Join(t.TempDir(), "scalar.parquet")
	if _, e = q.SinkParquet(context.Background(), path, ParquetSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	round, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	df, e = round.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	structGetCheck(t, df, "value", want)
	df.Close()
	stream, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
	if e != nil {
		t.Fatal(e)
	}
	if !stream.Next(context.Background()) {
		t.Fatal(stream.Err())
	}
	stream.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("early close leak")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	df, e = q.Collect(ctx)
	if !errors.Is(e, context.Canceled) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("cancel leak")
	}
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	defer tiny.Close()
	df, e = tiny.FromSource(src).SelectExprs(expr.StructGet(expr.Col("root"), "a.b:中").Alias("out")).Collect(context.Background())
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	if tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget fail leak")
	}
}

func TestStructGetArrowTemporalNormalization(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	fields := []arrow.Field{{Name: "seconds", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, Nullable: true}, {Name: "time", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true}, {Name: "duration", Type: &arrow.DurationType{Unit: arrow.Microsecond}, Nullable: true}}
	rb := array.NewRecordBuilder(alloc, arrow.NewSchema([]arrow.Field{{Name: "root", Type: arrow.StructOf(fields...), Nullable: true}}, nil))
	sb := rb.Field(0).(*array.StructBuilder)
	for i := 0; i < 5; i++ {
		sb.Append(true)
		sb.FieldBuilder(0).(*array.TimestampBuilder).Append(arrow.Timestamp(i + 1))
		sb.FieldBuilder(1).(*array.Time32Builder).Append(arrow.Time32(i + 1))
		sb.FieldBuilder(2).(*array.DurationBuilder).Append(arrow.Duration(i + 1))
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	slice := rec.NewSlice(1, 5)
	rec.Release()
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	q, e := s.ScanArrow(slice)
	slice.Release()
	if e != nil {
		t.Fatal(e)
	}
	q = q.SelectExprs(expr.StructGet(expr.Col("root"), "seconds").Alias("ts"), expr.StructGet(expr.Col("root"), "time").Alias("tm"), expr.StructGet(expr.Col("root"), "duration").Alias("dur"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	structGetCheck(t, df, "ts", []any{int64(2000000000), int64(3000000000), int64(4000000000), int64(5000000000)})
	structGetCheck(t, df, "tm", []any{int32(2), int32(3), int32(4), int32(5)})
	structGetCheck(t, df, "dur", []any{int64(2000), int64(3000), int64(4000), int64(5000)})
	// Exported metadata must match normalized physical values, not original seconds.
	schema, e := q.Schema()
	if e != nil {
		t.Fatal(e)
	}
	a, e := memory.ArrowSchema(schema)
	if e != nil {
		t.Fatal(e)
	}
	if a.Field(0).Type.(*arrow.TimestampType).Unit != arrow.Nanosecond || a.Field(1).Type.(*arrow.Time32Type).Unit != arrow.Millisecond || a.Field(2).Type.(*arrow.DurationType).Unit != arrow.Nanosecond {
		t.Fatal(a)
	}
}

func TestStructGetBatchSizes(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 65, 130} {
		for _, ci := range []int{3, 17, 18, 19} {
			t.Run(fmt.Sprintf("n%d/type%d", n, ci), func(t *testing.T) {
				schema, b, want, _ := structGetFixture(structGetCases()[ci], n)
				src, e := inmemory.New(schema, b)
				if e != nil {
					t.Fatal(e)
				}
				opts := engine.DefaultOptions()
				opts.PreparedCacheBytes = 0
				s := NewSession(opts)
				defer func() {
					s.Close()
					if s.Engine().Memory().Used() != 0 {
						t.Fatal("leak")
					}
				}()
				q := s.FromSource(src).SelectExprs(expr.StructGet(expr.Col("root"), "a.b:中").Alias("out"))
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer df.Close()
				structGetCheck(t, df, "out", want)
			})
		}
	}
}

// Input fixture copied from json_struct_test.go; no expression results are reused.
func structGetJSONInputBatch(ids []int64, payloads []string, validity []bool) *memory.Batch {
	b := memory.NewBatch()
	b.Int64s["id"] = &memory.Int64Vector{Values: append([]int64(nil), ids...)}
	v := &memory.StringViewVector{Offsets: make([]uint32, len(payloads)+1), Encoding: memory.EncodingView}
	if validity != nil {
		v.Validity = memory.EnsureBitmap(nil, len(payloads))
	}
	for i, payload := range payloads {
		if validity == nil || validity[i] {
			v.Data = append(v.Data, payload...)
			if v.Validity != nil {
				memory.Set(v.Validity, i)
			}
		}
		v.Offsets[i+1] = uint32(len(v.Data))
	}
	b.Strings["payload"] = v
	b.Reset(len(ids))
	return b
}
