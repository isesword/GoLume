package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func recursiveBFixture(t *testing.T, depth, n int) (memory.Schema, *memory.Batch) {
	t.Helper()
	leaf := memory.NewBatch()
	leaf.Rows = n
	vals := make([]int64, n)
	validity := make(memory.Bitmap, memory.WordsFor(n))
	offsets := make([]uint32, n+1)
	var data []byte
	for i := 0; i < n; i++ {
		vals[i] = int64(1000 + i)
		if i%5 != 0 {
			memory.Set(validity, i)
		}
		data = append(data, []byte(fmt.Sprintf("中-%d", i))...)
		offsets[i+1] = uint32(len(data))
	}
	leaf.Int64s["v"] = &memory.Int64Vector{Values: vals}
	leaf.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: validity}
	listOffsets := make([]uint32, n+1)
	elemOffsets := make([]uint32, 2*n+1)
	nums := make([]int64, 2*n)
	elemValidity := make(memory.Bitmap, memory.WordsFor(2*n))
	var words []byte
	for row := 0; row < n; row++ {
		listOffsets[row+1] = uint32(2 * (row + 1))
		for j := 0; j < 2; j++ {
			i := 2*row + j
			nums[i] = int64(row*10 + j)
			words = append(words, []byte(fmt.Sprintf("e%d", i))...)
			elemOffsets[i+1] = uint32(len(words))
			if j == 0 {
				memory.Set(elemValidity, i)
			}
		}
	}
	leaf.Int64Lists["ints"] = &memory.Int64ListVector{Offsets: listOffsets, Validity: validity, Values: &memory.Int64Vector{Values: nums, Validity: elemValidity}}
	leaf.StringLists["words"] = &memory.StringListVector{Offsets: listOffsets, Validity: validity, Values: &memory.StringViewVector{Offsets: elemOffsets, Data: words, Validity: elemValidity}}
	fields := []memory.Field{{Name: "v", Type: memory.TypeInt64}, {Name: "s", Type: memory.TypeString, Nullable: true}, memory.ListInt64Field("ints", true, true), memory.ListStringField("words", true, true)}
	var field memory.Field
	var node *memory.StructVector
	for level := depth; level >= 1; level-- {
		nullable := level%2 == 1
		var mask memory.Bitmap
		if nullable {
			mask = make(memory.Bitmap, memory.WordsFor(n))
			for i := 0; i < n; i++ {
				if i%(level+3) != 0 {
					memory.Set(mask, i)
				}
			}
		}
		name := "node.中"
		if level == 1 {
			name = "root"
		}
		field = memory.StructField(name, nullable, fields...)
		node = &memory.StructVector{Rows: n, Fields: field.Children, Values: leaf, Validity: mask, Encoding: memory.EncodingView}
		leaf = memory.NewBatch()
		leaf.Rows = n
		leaf.Structs[name] = node
		fields = []memory.Field{field}
	}
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	leaf.Int64s["id"] = &memory.Int64Vector{Values: ids}
	schema, err := memory.NewSchemaChecked(field, memory.Field{Name: "id", Type: memory.TypeInt64})
	if err != nil {
		t.Fatal(err)
	}
	return schema, leaf
}
func recursiveBValid(depth, row int) bool {
	for level := 1; level <= depth; level += 2 {
		if row%(level+3) == 0 {
			return false
		}
	}
	return true
}

func recursiveBLeaf(depth int, name string) expr.Expr {
	var e expr.Expr = Col("root")
	for i := 1; i < depth; i++ {
		e = expr.StructGet(e, "node.中")
	}
	return expr.StructGet(e, name)
}
func recursiveBCheck(t *testing.T, df DataFrame, depth int) {
	recursiveBCheckRange(t, df, depth, 0, 65)
}
func recursiveBCheckRange(t *testing.T, df DataFrame, depth, start, count int) {
	t.Helper()
	for _, name := range []string{"v", "s", "head", "length"} {
		c, err := df.Column(name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		want := make([]any, count)
		for j := range want {
			i := start + j
			if recursiveBValid(depth, i) {
				if name == "v" {
					want[j] = int64(1000 + i)
				} else if i%5 != 0 {
					switch name {
					case "s":
						want[j] = fmt.Sprintf("中-%d", i)
					case "head":
						want[j] = fmt.Sprintf("e%d", 2*i)
					case "length":
						want[j] = int64(2)
					}
				}
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s got%v want%v", name, got, want)
		}
	}
}
func TestRecursiveStructBLazy(t *testing.T) {
	for _, depth := range []int{2, 8} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("d%d/m%d", depth, mode), func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.KernelMode = mode
				opts.PreparedCacheBytes = 0
				s := NewSession(opts)
				defer s.Close()
				schema, b := recursiveBFixture(t, depth, 65)
				src, err := inmemory.New(schema, b)
				if err != nil {
					t.Fatal(err)
				}
				q := s.FromSource(src).WithColumns(Named(recursiveBLeaf(depth, "v"), "v"), Named(recursiveBLeaf(depth, "s"), "s"), expr.ListGet(recursiveBLeaf(depth, "words"), expr.I64(0)).Alias("head"), expr.ListLength(recursiveBLeaf(depth, "ints")).Alias("length"))
				df, err := q.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				recursiveBCheck(t, df, depth)
				df.Close()
				path := filepath.Join(t.TempDir(), "recursive.arrow")
				if _, err = q.SinkIPC(context.Background(), path, IPCSinkOptions{}); err != nil {
					t.Fatal(err)
				}
				f, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				rd, err := ipc.NewFileReader(f)
				if err != nil {
					t.Fatal(err)
				}
				var records []arrow.RecordBatch
				for i := 0; i < rd.NumRecords(); i++ {
					r, e := rd.RecordBatchAt(i)
					if e != nil {
						t.Fatal(e)
					}
					records = append(records, r)
				}
				if _, e := s.ScanArrow(records...); e == nil {
					t.Fatal("default Arrow accepted recursive Struct")
				}
				if len(records) != 1 {
					t.Fatalf("fixture requires one IPC batch, got%d", len(records))
				}
				db := array.NewDecimal128Builder(am.DefaultAllocator, &arrow.Decimal128Type{Precision: 20, Scale: 2})
				for i := 0; i < 65; i++ {
					db.Append(decimal128.FromI64(int64(12300 + i)))
				}
				da := db.NewArray()
				db.Release()
				fs := append([]arrow.Field(nil), records[0].Schema().Fields()...)
				fs = append(fs, arrow.Field{Name: "dec", Type: da.DataType()})
				cols := append([]arrow.Array(nil), records[0].Columns()...)
				cols = append(cols, da)
				mixed := array.NewRecordBatch(arrow.NewSchema(fs, nil), cols, 65)
				da.Release()
				mq, me := s.ScanArrowWithOptions(ArrowScanOptions{NativeRecursiveStruct: true, NativeDecimal128: true}, mixed)
				mixed.Release()
				if me != nil {
					t.Fatal(me)
				}
				md, me := mq.SelectExprs(expr.RoundDecimal128(Col("dec"), 20, 0, expr.DecimalRoundTowardZero).Alias("dec"), Named(recursiveBLeaf(depth, "v"), "v")).Collect(context.Background())
				if me != nil {
					t.Fatal(me)
				}
				mc, me := md.Column("dec")
				if me != nil {
					t.Fatal(me)
				}
				chunks, me := mc.Decimal128Chunks()
				if me != nil {
					t.Fatal(me)
				}
				seen := 0
				for _, c := range chunks {
					if c.Precision() != 20 || c.Scale() != 0 {
						t.Fatal("decimal metadata")
					}
					for i := 0; i < c.Len(); i++ {
						v, ok, e := c.Value(i)
						if e != nil || !ok || v.BigInt().Int64() != 123 {
							t.Fatal("decimal coexist coefficient", v, ok, e)
						}
						seen++
					}
				}
				if seen != 65 {
					t.Fatal("decimal rowcount")
				}
				md.Close()
				dt := &arrow.Decimal128Type{Precision: 20, Scale: 2}
				innerType := arrow.StructOf(arrow.Field{Name: "d", Type: dt})
				outerType := arrow.StructOf(arrow.Field{Name: "node", Type: innerType})
				nb := array.NewStructBuilder(am.DefaultAllocator, outerType)
				nb.Append(true)
				ib := nb.FieldBuilder(0).(*array.StructBuilder)
				ib.Append(true)
				ib.FieldBuilder(0).(*array.Decimal128Builder).Append(decimal128.FromI64(1))
				na := nb.NewArray()
				nb.Release()
				nr := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "root", Type: outerType}}, nil), []arrow.Array{na}, 1)
				na.Release()
				_, ne := s.ScanArrowWithOptions(ArrowScanOptions{NativeRecursiveStruct: true, NativeDecimal128: true}, nr)
				nr.Release()
				if ne == nil {
					t.Fatal("nested Decimal silently expanded")
				}
				sliced := records[0].NewSlice(1, 65)
				sq, se := s.ScanArrowWithOptions(ArrowScanOptions{NativeRecursiveStruct: true}, sliced)
				sliced.Release()
				if se != nil {
					t.Fatal(se)
				}
				sq = sq.SelectExprs(Named(recursiveBLeaf(depth, "v"), "v"), Named(recursiveBLeaf(depth, "s"), "s"), expr.ListGet(recursiveBLeaf(depth, "words"), expr.I64(0)).Alias("head"), expr.ListLength(recursiveBLeaf(depth, "ints")).Alias("length"))
				round, err := s.ScanArrowWithOptions(ArrowScanOptions{NativeRecursiveStruct: true}, records...)
				for _, r := range records {
					r.Release()
				}
				rd.Close()
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
				sd, se := sq.Collect(context.Background())
				if se != nil {
					t.Fatal(se)
				}
				recursiveBCheckRange(t, sd, depth, 1, 64)
				sd.Close()
				// Re-extract from the transported recursive root, not just the parallel scalar columns.
				round = round.SelectExprs(Named(recursiveBLeaf(depth, "v"), "v"), Named(recursiveBLeaf(depth, "s"), "s"), expr.ListGet(recursiveBLeaf(depth, "words"), expr.I64(0)).Alias("head"), expr.ListLength(recursiveBLeaf(depth, "ints")).Alias("length"))
				tinyOpts := opts
				tinyOpts.MemoryLimitBytes = 1
				tiny := NewSession(tinyOpts)
				td, te := tiny.FromSource(src).Collect(context.Background())
				if te == nil {
					td.Close()
				}
				tiny.Close()
				if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
					t.Fatalf("budget %v", te)
				}
				df, err = round.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				recursiveBCheck(t, df, depth)
				df.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				df, err = q.Collect(ctx)
				if !errors.Is(err, context.Canceled) {
					if err == nil {
						df.Close()
					}
					t.Fatal(err)
				}
				df, err = q.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				s.Close()
				rc, re := df.Column("root")
				if re != nil {
					t.Fatal(re)
				}
				raw, re := rc.ToAnySlice()
				if re != nil {
					t.Fatal(re)
				}
				wantRaw := make([]any, 65)
				for i := range wantRaw {
					wantRaw[i] = recursiveBRaw(depth, 1, i)
				}
				if !reflect.DeepEqual(raw, wantRaw) {
					t.Fatal("owned recursive result after session close", raw, wantRaw)
				}
				recursiveBCheck(t, df, depth)
				df.Close()
			})
		}
	}
}

func recursiveBRaw(depth, level, row int) any {
	if level%2 == 1 && row%(level+3) == 0 {
		return nil
	}
	if level < depth {
		return map[string]any{"node.中": recursiveBRaw(depth, level+1, row)}
	}
	m := map[string]any{"v": int64(1000 + row), "s": nil, "ints": nil, "words": nil}
	if row%5 != 0 {
		m["s"] = fmt.Sprintf("中-%d", row)
		m["ints"] = []any{int64(row * 10), nil}
		m["words"] = []any{fmt.Sprintf("e%d", 2*row), nil}
	}
	return m
}
