package parquet

import (
	"context"
	"errors"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type forestTestLeaf struct {
	parent, name string
	kind         int
	A, L, E      bool
	shift        int
}

// kind 0=fixed, 1=scalar String, 2=Int64 List, 3=String List.
func forestTestFixture(t *testing.T, leaves []forestTestLeaf, groups []int, corrupt int, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	fields := schema.FieldList{}
	parents := map[string]schema.FieldList{}
	order := []string{}
	for _, k := range leaves {
		rep := req
		if k.L {
			rep = opt
		}
		var node schema.Node
		var e error
		typ := arrowparquet.Types.Int64
		var logical schema.LogicalType = schema.NewIntLogicalType(64, true)
		if k.kind == 1 || k.kind == 3 {
			typ = arrowparquet.Types.ByteArray
			logical = schema.StringLogicalType{}
		}
		if k.kind < 2 {
			node, e = schema.NewPrimitiveNodeLogical(k.name, rep, logical, typ, -1, -1)
		} else {
			er := req
			if k.E {
				er = opt
			}
			el, err := schema.NewPrimitiveNodeLogical("element", er, logical, typ, -1, -1)
			if err != nil {
				t.Fatal(err)
			}
			mid, err := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
			if err != nil {
				t.Fatal(err)
			}
			node, e = schema.NewGroupNodeLogical(k.name, rep, schema.FieldList{mid}, schema.ListLogicalType{}, -1)
		}
		if e != nil {
			t.Fatal(e)
		}
		if k.parent == "" {
			fields = append(fields, node)
		} else {
			if _, ok := parents[k.parent]; !ok {
				order = append(order, k.parent)
			}
			parents[k.parent] = append(parents[k.parent], node)
		}
	}
	// Fixture leaf order is top leaves followed by contiguous parent children.
	for _, p := range order {
		rep := req
		for _, k := range leaves {
			if k.parent == p && k.A {
				rep = opt
			}
		}
		n, e := schema.NewGroupNode(p, rep, parents[p], -1)
		if e != nil {
			t.Fatal(e)
		}
		fields = append(fields, n)
	}
	root, e := schema.NewGroupNode("schema", req, fields, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "forest.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(128), arrowparquet.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	for _, n := range groups {
		rg := w.AppendRowGroup()
		for col, k := range leaves {
			c, e := rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			var iv []int64
			var sv []arrowparquet.ByteArray
			var defs, reps []int16
			ad, ld, ed := int16(0), int16(0), int16(0)
			if k.A {
				ad = 1
			}
			if k.L {
				ld = 1
			}
			if k.E {
				ed = 1
			}
			for i := 0; i < n; i++ {
				row := base + i
				pv := !k.A || (row+k.shift)%11 != 0
				if col == corrupt && row == 4 {
					pv = !pv
				}
				if !pv {
					defs = append(defs, 0)
					if k.kind >= 2 {
						reps = append(reps, 0)
					}
					continue
				}
				if k.kind < 2 {
					valid := !k.L || (row+k.shift)%3 != 1
					d := ad
					if valid {
						d += ld
						if k.kind == 0 {
							iv = append(iv, int64(row+k.shift))
						} else {
							sv = append(sv, arrowparquet.ByteArray(forestTestText(k.name, row+k.shift)))
						}
					}
					defs = append(defs, d)
					continue
				}
				valid := false
				var bits []bool
				var ints []int64
				var texts []string
				if k.kind == 2 {
					r := listNativeExpected(row+k.shift, 129, k.L, k.E)
					valid, bits, ints = r.valid, r.valids, r.values
				} else {
					r := listStringExpected(row+k.shift, 129, 0, k.L, k.E)
					valid, bits, texts = r.valid, r.valids, r.values
				}
				if !valid {
					defs = append(defs, ad)
					reps = append(reps, 0)
					continue
				}
				if len(bits) == 0 {
					defs = append(defs, ad+ld)
					reps = append(reps, 0)
					continue
				}
				for j, valid := range bits {
					d, r := ad+ld+1, int16(0)
					if j > 0 {
						r = 1
					}
					if valid {
						d += ed
						if k.kind == 2 {
							iv = append(iv, ints[j])
						} else {
							sv = append(sv, arrowparquet.ByteArray(texts[j]))
						}
					}
					defs = append(defs, d)
					reps = append(reps, r)
				}
			}
			if k.kind == 0 || k.kind == 2 {
				_, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(iv, defs, reps)
			} else {
				_, e = c.(*file.ByteArrayColumnChunkWriter).WriteBatch(sv, defs, reps)
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = c.Close(); e != nil {
				t.Fatal(e)
			}
		}
		if e = rg.Close(); e != nil {
			t.Fatal(e)
		}
		base += n
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}
func forestTestCheck(t *testing.T, b *memory.Batch, leaves []forestTestLeaf, start int) {
	t.Helper()
	for _, k := range leaves {
		child := b
		if k.parent != "" {
			v := b.Structs[k.parent]
			if v == nil || v.Rows != b.Rows || v.Values.Rows != b.Rows {
				t.Fatal("parent crop", k.parent)
			}
			child = v.Values
		}
		for i := 0; i < b.Rows; i++ {
			row := start + i
			pv := !k.A || (row+k.shift)%11 != 0
			if k.parent != "" && b.Structs[k.parent].IsValid(i) != pv {
				t.Fatal("parent NULL", k.parent, row)
			}
			if k.kind == 0 {
				v := child.Int64s[k.name]
				valid := pv && (!k.L || (row+k.shift)%3 != 1)
				if v.IsValid(i) != valid || valid && v.Values[i] != int64(row+k.shift) {
					t.Fatal("fixed", k, row)
				}
				continue
			}
			if k.kind == 1 {
				v := child.Strings[k.name]
				valid := pv && (!k.L || (row+k.shift)%3 != 1)
				if v.IsValid(i) != valid || valid && string(v.ValueBytes(i)) != forestTestText(k.name, row+k.shift) {
					t.Fatal("scalar string", k, row)
				}
				continue
			}
			if k.kind == 2 {
				r := listNativeExpected(row+k.shift, 129, k.L, k.E)
				if !pv {
					r = listNativeRow{}
				}
				v := child.Int64Lists[k.name]
				if v == nil || len(v.Offsets) != b.Rows+1 || v.IsValid(i) != r.valid {
					t.Fatal("Int list shape/NULL")
				}
				lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
				if hi-lo != len(r.values) {
					t.Fatal("Int length")
				}
				for j, x := range r.values {
					if v.Values.IsValid(lo+j) != r.valids[j] || r.valids[j] && v.Values.Values[lo+j] != x {
						t.Fatal("Int item")
					}
				}
			} else {
				r := listStringExpected(row+k.shift, 129, 0, k.L, k.E)
				if !pv {
					r = listStringRow{}
				}
				v := child.StringLists[k.name]
				if v == nil || len(v.Offsets) != b.Rows+1 || v.IsValid(i) != r.valid {
					t.Fatal("String list shape/NULL")
				}
				lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
				if hi-lo != len(r.values) {
					t.Fatal("String length")
				}
				for j, x := range r.values {
					if v.Values.IsValid(lo+j) != r.valids[j] || r.valids[j] && string(v.Values.ValueBytes(lo+j)) != x {
						t.Fatal("String item")
					}
				}
			}
		}
	}
}
func TestNativeNestedForestIndependentLevels(t *testing.T) {
	for mask := 0; mask < 8; mask++ {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			t.Run(fmt.Sprintf("nullable%d/page%d", mask, version), func(t *testing.T) {
				A, L, E := mask&1 != 0, mask&2 != 0, mask&4 != 0
				leaves := []forestTestLeaf{{"", "id", 0, false, false, false, 0}, {"", "a.same", 1, false, true, false, 1}, {"", "toplist", 3, false, true, true, 2}, {"a", "same", 1, A, L, false, 0}, {"a", "items", 2, A, L, E, 0}, {"a", "text.items", 3, A, L, E, 0}, {"b", "same", 0, true, true, false, 3}, {"b", "items", 3, true, true, true, 3}}
				path := forestTestFixture(t, leaves, []int{0, 17, 66}, -1, arrowparquet.WithDataPageVersion(version), arrowparquet.WithDictionaryDefault(mask%2 == 0))
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				opts := testReadOptions()
				opts.BatchRows = 7
				opts.BatchBytes = 256 << 10
				m := memory.NewBudgetManager(0)
				opts.MemoryManager = m
				st, e := source.OpenBatchStream(context.Background(), src, opts)
				if e != nil {
					t.Fatal(e)
				}
				defer st.Close()
				cursor := 0
				for {
					b := memory.NewBatch()
					ok, e := st.Next(context.Background(), b)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					forestTestCheck(t, b, leaves, cursor)
					cursor += b.Rows
				}
				st.Close()
				if cursor != 83 || m.Used() != 0 {
					t.Fatal("rows/leak", cursor, m.Used())
				}
			})
		}
	}
}

func TestNativeNestedForestParentFailure(t *testing.T) {
	leaves := []forestTestLeaf{{"", "id", 0, false, false, false, 0}, {"a", "fixed", 0, true, false, false, 0}, {"a", "text", 1, true, true, false, 0}, {"a", "ints", 2, true, true, true, 0}, {"a", "strings", 3, true, true, true, 0}}
	for _, bad := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			path := forestTestFixture(t, leaves, []int{17}, bad)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 17
			opts.BatchBytes = 128 << 10
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			found := false
			for {
				ok, err := st.Next(context.Background(), memory.NewBatch())
				if err != nil {
					if !strings.Contains(err.Error(), "parent") {
						t.Fatal("wrong error", err)
					}
					found = true
					break
				}
				if !ok {
					break
				}
			}
			st.Close()
			if !found || m.Used() != 0 {
				t.Fatal("parent consensus/cleanup", found, m.Used())
			}
		})
	}
}
func TestNativeNestedForestPartialParentResume(t *testing.T) {
	leaves := []forestTestLeaf{{"", "id", 0, false, false, false, 0}, {"a", "ints", 2, true, true, true, 0}, {"a", "strings", 3, true, true, true, 0}, {"b", "strings", 3, true, true, true, 3}}
	path := forestTestFixture(t, leaves, []int{17}, -1)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 17
	opts.BatchBytes = 256 << 10
	m := &nestedTestFaultManager{Manager: memory.NewBudgetManager(0), stage: 1}
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	m.native = st.(*nestedListStream)
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil || !m.hit || b.Rows != 4 {
		t.Fatal("partial prefix", ok, e, m.hit, b.Rows)
	}
	forestTestCheck(t, b, leaves, 0)
	c := m.native.cursors[1]
	if !c.ancestorValid || c.complete || !c.tuple.present || c.stageCount() != m.count || string(c.bytes[0]) != m.pending {
		t.Fatal("partial ancestor/borrowed state")
	}
	cursor := b.Rows
	for {
		b = memory.NewBatch()
		ok, e = st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		forestTestCheck(t, b, leaves, cursor)
		cursor += b.Rows
	}
	st.Close()
	if cursor != 17 || m.Used() != 0 {
		t.Fatal("resume lost rows/leak", cursor, m.Used())
	}
}

func forestTestText(name string, row int) string {
	if row == 1000 {
		return "xx" + strings.Repeat("界", 44000) + "z"
	}
	if row%5 == 2 {
		return ""
	}
	return fmt.Sprintf("%s/%d/界", name, row)
}
func TestNativeNestedForestScalarFinalAndFixedOnly(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		for _, n := range []int{3, 4, 5} {
			t.Run(fmt.Sprintf("fixed%v/rows%d", fixed, n), func(t *testing.T) {
				kind := 1
				if fixed {
					kind = 0
				}
				leaves := []forestTestLeaf{{"", "id", 0, false, false, false, 0}, {"a", "value", kind, true, true, false, 0}, {"b", "value", 0, true, true, false, 2}}
				path := forestTestFixture(t, leaves, []int{n}, -1)
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				opts := testReadOptions()
				opts.BatchRows = 2
				opts.BatchBytes = 64 << 10
				m := memory.NewBudgetManager(0)
				opts.MemoryManager = m
				st, e := source.OpenBatchStream(context.Background(), src, opts)
				if e != nil {
					t.Fatal(e)
				}
				cursor := 0
				for {
					b := memory.NewBatch()
					ok, e := st.Next(context.Background(), b)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					forestTestCheck(t, b, leaves, cursor)
					cursor += b.Rows
				}
				st.Close()
				if cursor != n || m.Used() != 0 {
					t.Fatal("final scalar/fixed-only", cursor, m.Used())
				}
			})
		}
	}
}

func TestNativeNestedForestLateScalarCopyCancel(t *testing.T) {
	leaves := []forestTestLeaf{{"", "id", 0, false, false, false, 0}, {"", "ints", 2, false, false, false, 4}, {"a", "items", 3, false, false, false, 4}, {"a", "scalar", 1, false, false, false, 1000}}
	path := forestTestFixture(t, leaves, []int{1}, -1)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, cancel := range []bool{false, true} {
		opts := testReadOptions()
		opts.BatchRows = 1
		opts.BatchBytes = 1 << 20
		m := newBatchStreamPeakManager(opts.BatchBytes)
		opts.MemoryManager = m
		st, e := source.OpenBatchStream(context.Background(), src, opts)
		if e != nil {
			t.Fatal(e)
		}
		ctx := &forestTestCopyContext{Context: context.Background(), native: st.(*nestedListStream)}
		var runctx context.Context = context.Background()
		if cancel {
			runctx = ctx
		}
		b := memory.NewBatch()
		ok, e := st.Next(runctx, b)
		if cancel {
			if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || b.Rows != 0 {
				t.Fatal("scalar copy cancel", ok, e, ctx.checks)
			}
		} else {
			if !ok || e != nil {
				t.Fatal(e)
			}
			forestTestCheck(t, b, leaves, 0)
		}
		st.Close()
		if m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
			t.Fatal("scalar copy cleanup/peak")
		}
	}
}

type forestTestCopyContext struct {
	context.Context
	native *nestedListStream
	checks int
}

func (c *forestTestCopyContext) Err() error {
	s := c.native
	if len(s.cursors[2].bytes[0]) > 64<<10 && s.cursors[0].complete && s.cursors[1].complete {
		var pcs [32]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, ".copyBytes") {
				c.checks++
				if c.checks == 2 {
					return context.Canceled
				}
				break
			}
			if !more {
				break
			}
		}
	}
	return c.Context.Err()
}

func TestNativeNestedForestRejectRecursive(t *testing.T) {
	for _, kind := range []string{"recursive_struct", "list_struct", "list_list_list"} {
		t.Run(kind, func(t *testing.T) {
			req := arrowparquet.Repetitions.Required
			leaf, e := schema.NewPrimitiveNodeLogical("n", req, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
			if e != nil {
				t.Fatal(e)
			}
			var node schema.Node
			if kind == "recursive_struct" {
				node = leaf
				// NativeNested now supports depth 2..8; reject depth 9 instead.
				for depth := 0; depth < 9; depth++ {
					n, err := schema.NewGroupNode("inner", req, schema.FieldList{node}, -1)
					if err != nil {
						t.Fatal(err)
					}
					node = n
				}
			} else {
				if kind == "list_struct" {
					// NativeNested now permits a flat item Struct; nested item Struct remains outside scope.
					nested, err := schema.NewGroupNode("nested", req, schema.FieldList{leaf}, -1)
					if err != nil {
						t.Fatal(err)
					}
					element, err := schema.NewGroupNode("element", req, schema.FieldList{nested}, -1)
					if err != nil {
						t.Fatal(err)
					}
					middle, err := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{element}, -1)
					if err != nil {
						t.Fatal(err)
					}
					node, err = schema.NewGroupNodeLogical("items", req, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					element, e := schema.NewGroupNode("element", req, schema.FieldList{leaf}, -1)
					if e != nil {
						t.Fatal(e)
					}
					if kind == "list_list_list" {
						inner, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{leaf}, -1)
						if e != nil {
							t.Fatal(e)
						}
						element, e = schema.NewGroupNodeLogical("element", req, schema.FieldList{inner}, schema.ListLogicalType{}, -1)
						if e != nil {
							t.Fatal(e)
						}
					}
					if kind == "list_list_list" {
						wrapper, err := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{element}, -1)
						if err != nil {
							t.Fatal(err)
						}
						element, e = schema.NewGroupNodeLogical("element", req, schema.FieldList{wrapper}, schema.ListLogicalType{}, -1)
						if e != nil {
							t.Fatal(e)
						}
					}
					middle, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{element}, -1)
					if e != nil {
						t.Fatal(e)
					}
					node, e = schema.NewGroupNodeLogical("items", req, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
					if e != nil {
						t.Fatal(e)
					}
				}
			}
			root, e := schema.NewGroupNode("schema", req, schema.FieldList{node}, -1)
			if e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(t.TempDir(), "unsupported.parquet")
			f, e := os.Create(path)
			if e != nil {
				t.Fatal(e)
			}
			w := file.NewParquetWriter(f, root)
			if e = w.Close(); e != nil {
				t.Fatal(e)
			}
			if _, e = OpenFileWithOptions(path, Options{NativeNested: true}); e == nil {
				t.Fatal("recursive scope accepted")
			}
		})
	}
}
