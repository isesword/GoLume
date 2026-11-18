package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type listGetIndexCase struct {
	typ    memory.DataType
	vector string
	sample any
}

func listGetIndices() []listGetIndexCase {
	return []listGetIndexCase{{memory.TypeInt8, "Int8s", int8(0)}, {memory.TypeInt16, "Int16s", int16(0)}, {memory.TypeInt32, "Int32s", int32(0)}, {memory.TypeInt64, "Int64s", int64(0)}, {memory.TypeUint8, "Uint8s", uint8(0)}, {memory.TypeUint16, "Uint16s", uint16(0)}, {memory.TypeUint32, "Uint32s", uint32(0)}, {memory.TypeUint64, "Uint64s", uint64(0)}}
}
func listGetIndex(row int, unsigned bool) uint64 {
	if row%6 == 4 {
		if unsigned {
			return math.MaxUint64
		}
		return math.MaxUint64
	}
	return uint64(row % 6)
}
func listGetWant(row int, strings bool, index uint64, indexValid bool) any {
	if row%7 == 0 || row%5 == 0 || !indexValid || index >= 3 || index == 1 {
		return nil
	}
	if strings {
		if index == 2 {
			return ""
		}
		return fmt.Sprintf("中%d", row)
	}
	if index == 2 {
		return -int64(row + 1)
	}
	return int64(row + 1)
}
func listGetFixture(tc listGetIndexCase, stringList bool, start, n int) (memory.Schema, *memory.Batch) {
	listField := memory.ListInt64Field("xs", true, true)
	if stringList {
		listField = memory.ListStringField("xs", true, true)
	}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "ix", Type: tc.typ, Nullable: true}, listField)
	b := memory.NewBatch()
	b.Reset(n)
	ids := make([]int64, n)
	offsets := []uint32{0}
	parent := memory.EnsureBitmap(nil, n)
	indexBits := memory.EnsureBitmap(nil, n)
	var values []int64
	sv := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
	elementBits := memory.EnsureBitmap(nil, n*3)
	typed := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(tc.sample)), n, n)
	unsigned := reflect.TypeOf(tc.sample).Kind() >= reflect.Uint && reflect.TypeOf(tc.sample).Kind() <= reflect.Uint64
	for i := 0; i < n; i++ {
		r := start + i
		ids[i] = int64(r)
		if r%7 != 0 {
			memory.Set(parent, i)
		}
		if r%11 != 0 {
			memory.Set(indexBits, i)
		}
		ix := listGetIndex(r, unsigned)
		if unsigned {
			typed.Index(i).SetUint(ix)
		} else {
			typed.Index(i).SetInt(int64(ix))
		}
		if r%5 != 0 {
			base := len(values)
			values = append(values, int64(r+1), 999, -int64(r+1))
			memory.Set(elementBits, base)
			memory.Set(elementBits, base+2)
			text := fmt.Sprintf("中%d", r)
			sv.Data = append(sv.Data, text...)
			sv.Offsets = append(sv.Offsets, uint32(len(sv.Data)), uint32(len(sv.Data)), uint32(len(sv.Data)))
		}
		offsets = append(offsets, uint32(len(values)))
	}
	m := reflect.ValueOf(b).Elem().FieldByName(tc.vector)
	v := reflect.New(m.Type().Elem().Elem())
	v.Elem().FieldByName("Values").Set(typed)
	v.Elem().FieldByName("Validity").Set(reflect.ValueOf(indexBits))
	m.SetMapIndex(reflect.ValueOf("ix"), v)
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	if stringList {
		sv.Validity = elementBits
		b.StringLists["xs"] = &memory.StringListVector{Offsets: offsets, Values: sv, Validity: parent, Encoding: memory.EncodingView}
	} else {
		b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: offsets, Values: &memory.Int64Vector{Values: values, Validity: elementBits}, Validity: parent, Encoding: memory.EncodingView}
	}
	return schema, b
}
func listGetCheck(t *testing.T, column func(string) (Series, error), rows []int, stringList, unsigned bool) {
	t.Helper()
	for _, name := range []string{"dynamic", "copy", "first", "last", "huge"} {
		c, e := column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(rows))
		for i, r := range rows {
			ix, valid := listGetIndex(r, unsigned), r%11 != 0
			switch name {
			case "first":
				ix, valid = 0, true
			case "last":
				ix, valid = 2, true
			case "huge":
				ix, valid = math.MaxUint64, true
			}
			want[i] = listGetWant(r, stringList, ix, valid)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got=%#v want=%#v", name, got, want)
		}
	}
}
func TestListGetEntrypointOracle(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, entry := range []string{"memory", "arrow"} {
			for _, stringList := range []bool{false, true} {
				for _, tc := range listGetIndices() {
					t.Run(fmt.Sprintf("%d/%s/string%v/%s", mode, entry, stringList, tc.typ), func(t *testing.T) {
						opts := engine.DefaultOptions()
						opts.KernelMode = mode
						opts.PreparedCacheBytes = 0
						s := NewSession(opts)
						defer func() {
							s.Close()
							if s.Engine().Memory().Used() != 0 {
								t.Error("leak")
							}
						}()
						schema, b := listGetFixture(tc, stringList, 0, 132)
						var q LazyFrame
						var rows []int
						if entry == "memory" {
							var batches []*memory.Batch
							for start := 0; start < 130; start += 31 {
								_, batch := listGetFixture(tc, stringList, start, min(31, 130-start))
								batches = append(batches, batch)
							}
							src, e := inmemory.New(schema, batches...)
							if e != nil {
								t.Fatal(e)
							}
							q = s.FromSource(src)
							for i := 0; i < 130; i++ {
								rows = append(rows, i)
							}
						} else {
							alloc := am.NewCheckedAllocator(am.DefaultAllocator)
							defer func() { s.Close(); alloc.AssertSize(t, 0) }()
							rec, e := memory.ArrowRecord(alloc, schema, b)
							if e != nil {
								t.Fatal(e)
							}
							slice := rec.NewSlice(1, 131)
							rec.Release()
							q, e = s.ScanArrow(slice)
							slice.Release()
							if e != nil {
								t.Fatal(e)
							}
							for i := 1; i < 131; i++ {
								rows = append(rows, i)
							}
						}
						// Keep legacy collected-view error semantics separate from the new expression.
						if !stringList && entry == "memory" && tc.typ == memory.TypeInt64 {
							df, e := q.Collect(context.Background())
							if e != nil {
								t.Fatal(e)
							}
							c, e := df.Column("xs")
							if e != nil {
								t.Fatal(e)
							}
							chunks, e := c.Int64ListChunks()
							if e != nil {
								t.Fatal(e)
							}
							value, valid, e := chunks[0].Value(1)
							if e != nil || !valid {
								t.Fatal("fixture")
							}
							if _, _, e := value.Element(3); e == nil {
								t.Fatal("existing Element out-of-range must error")
							}
							df.Close()
						}
						q = q.SelectExprs(expr.Col("id").Alias("id"), ListGet(Col("xs"), Col("ix")).Alias("dynamic"), expr.ListGet(expr.Col("xs"), expr.Col("ix")).Alias("copy"), expr.ListGet(expr.Col("xs"), expr.I64(0)).Alias("first"), expr.ListGet(expr.Col("xs"), expr.I64(2)).Alias("last"), expr.ListGet(expr.Col("xs"), expr.I64(math.MaxInt64)).Alias("huge"))
						out, e := q.Schema()
						if e != nil {
							t.Fatal(e)
						}
						for _, f := range out.Fields[1:] {
							want := memory.TypeInt64
							if stringList {
								want = memory.TypeString
							}
							if f.Type != want || !f.Nullable {
								t.Fatal(f)
							}
						}
						unsigned := reflect.TypeOf(tc.sample).Kind() >= reflect.Uint
						df, e := q.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						listGetCheck(t, df.Column, rows, stringList, unsigned)
						df.Close()
						if tc.typ != memory.TypeInt64 {
							return
						}
						filtered := q.Filter(expr.Not(expr.IsNull(expr.Col("dynamic"))))
						var selected []int
						for _, r := range rows {
							if listGetWant(r, stringList, listGetIndex(r, unsigned), r%11 != 0) != nil {
								selected = append(selected, r)
							}
						}
						df, e = filtered.Collect(context.Background())
						if e != nil {
							t.Fatal(e)
						}
						listGetCheck(t, df.Column, selected, stringList, unsigned)
						df.Close()
						stream, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
						if e != nil {
							t.Fatal(e)
						}
						seen := 0
						for stream.Next(context.Background()) {
							batch := stream.Batch()
							ids, e := batch.Column("id")
							if e != nil {
								t.Fatal(e)
							}
							n := int(ids.Len())
							listGetCheck(t, batch.Column, rows[seen:seen+n], stringList, unsigned)
							seen += n
						}
						if e := stream.Err(); e != nil {
							t.Fatal(e)
						}
						stream.Close()
						if seen != len(rows) {
							t.Fatal(seen)
						}
						for _, format := range []string{"ipc", "parquet"} {
							p := filepath.Join(t.TempDir(), "out."+format)
							var round LazyFrame
							if format == "parquet" {
								if _, e := q.SinkParquet(context.Background(), p, ParquetSinkOptions{}); e != nil {
									t.Fatal(e)
								}
								round, e = s.ScanParquet(p)
							} else {
								if _, e := q.SinkIPC(context.Background(), p, IPCSinkOptions{}); e != nil {
									t.Fatal(e)
								}
								f, err := os.Open(p)
								if err != nil {
									t.Fatal(err)
								}
								reader, err := ipc.NewFileReader(f)
								if err != nil {
									t.Fatal(err)
								}
								var records []arrow.RecordBatch
								for i := 0; i < reader.NumRecords(); i++ {
									r, err := reader.RecordBatchAt(i)
									if err != nil {
										t.Fatal(err)
									}
									records = append(records, r)
								}
								round, e = s.ScanArrow(records...)
								for _, r := range records {
									r.Release()
								}
								reader.Close()
								f.Close()
							}
							if e != nil {
								t.Fatal(e)
							}
							df, e = round.Collect(context.Background())
							if e != nil {
								t.Fatal(e)
							}
							listGetCheck(t, df.Column, rows, stringList, unsigned)
							df.Close()
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
						early, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
						if e != nil {
							t.Fatal(e)
						}
						if !early.Next(context.Background()) {
							t.Fatal(early.Err())
						}
						early.Close()
					})
				}
			}
		}
	}
}
func TestListGetStringProducersAndBudget(t *testing.T) {
	b := memory.NewBatch()
	b.Reset(3)
	b.Strings["text"] = &memory.StringViewVector{Offsets: []uint32{0, 5, 5, 8}, Data: []byte("ab,cdx,y"), Validity: memory.Bitmap{5}, Encoding: memory.EncodingView}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "text", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}), b)
	if e != nil {
		t.Fatal(e)
	}
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	q := s.FromSource(src).SelectExprs(expr.ListGet(expr.Split(expr.Col("text"), expr.Str(",")), expr.I64(1)).Alias("split"), expr.ListGet(expr.RegexExtractAll(expr.Col("text"), expr.Str("[a-z]+"), expr.I64(0)), expr.I64(0)).Alias("regex"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for name, want := range map[string][]any{"split": {"cd", nil, "y"}, "regex": {"ab", nil, "x"}} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(name, got, want, e)
		}
	}
	df.Close()
	opts.MemoryLimitBytes = 1
	tiny := NewSession(opts)
	defer tiny.Close()
	df, e = tiny.FromSource(src).SelectExprs(expr.ListGet(expr.Split(expr.Col("text"), expr.Str(",")), expr.I64(0)).Alias("out")).Collect(context.Background())
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	if tiny.Engine().Memory().Used() != 0 || s.Engine().Memory().Used() != 0 {
		t.Fatal("budget leak")
	}
}
