package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"gocolumn/source"
)

type recursiveReaderLeaf struct {
	level, kind int
	name        string
}

// kind: 0 fixed witness, 1 String, 2 Int64 List, 3 String List.
// Every Struct has its own witness, so a deeper leaf cannot silently determine
// its ancestors without agreement with all other descendants.
func recursiveReaderFixture(t *testing.T, optional []bool, groups []int, variable bool, badLeaf int, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	primitive := func(name string, kind int, nullable bool) schema.Node {
		rep := req
		if nullable {
			rep = opt
		}
		typ := arrowparquet.Types.Int64
		var logical schema.LogicalType = schema.NewIntLogicalType(64, true)
		if kind == 1 || kind == 3 {
			typ = arrowparquet.Types.ByteArray
			logical = schema.StringLogicalType{}
		}
		if kind < 2 {
			n, e := schema.NewPrimitiveNodeLogical(name, rep, logical, typ, -1, -1)
			if e != nil {
				t.Fatal(e)
			}
			return n
		}
		el, e := schema.NewPrimitiveNodeLogical("element", opt, logical, typ, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		mid, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
		if e != nil {
			t.Fatal(e)
		}
		n, e := schema.NewGroupNodeLogical(name, rep, schema.FieldList{mid}, schema.ListLogicalType{}, -1)
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	leaves := []recursiveReaderLeaf{{-1, 0, "root.dot.n"}}
	for level := range optional {
		leaves = append(leaves, recursiveReaderLeaf{level, 0, "n"})
	}
	if variable {
		for k, name := range []string{"text", "ints", "strings"} {
			leaves = append(leaves, recursiveReaderLeaf{len(optional) - 1, k + 1, name})
		}
	}
	var inner schema.Node
	for level := len(optional) - 1; level >= 0; level-- {
		children := schema.FieldList{primitive("n", 0, false)}
		if inner != nil {
			children = append(children, inner)
		} else if variable {
			children = append(children, primitive("text", 1, true), primitive("ints", 2, true), primitive("strings", 3, true))
		}
		rep := req
		if optional[level] {
			rep = opt
		}
		name := "inner"
		if level == 0 {
			name = "root.dot"
		}
		n, e := schema.NewGroupNode(name, rep, children, -1)
		if e != nil {
			t.Fatal(e)
		}
		inner = n
	}
	root, e := schema.NewGroupNode("schema", req, schema.FieldList{primitive("root.dot.n", 0, false), inner}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "recursive.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(128), arrowparquet.WithBatchSize(5)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	for _, count := range groups {
		rg := w.AppendRowGroup()
		for col, leaf := range leaves {
			c, e := rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			var iv []int64
			var sv []arrowparquet.ByteArray
			var defs, reps []int16
			for row := base; row < base+count; row++ {
				ancestorDef, visible := recursiveReaderDomain(optional, leaf.level, row)
				if col == badLeaf && row == len(optional)+3 {
					ancestorDef = int16(leaf.level) // all-optional corruption fixtures: deepest ancestor alone is NULL
					visible = false
				}
				appendLevel := func(d, r int16) {
					defs = append(defs, d)
					if leaf.kind >= 2 {
						reps = append(reps, r)
					}
				}
				if !visible {
					appendLevel(ancestorDef, 0)
					continue
				}
				if leaf.kind == 0 {
					appendLevel(ancestorDef, 0)
					iv = append(iv, int64(row*100+leaf.level+1))
					continue
				}
				if row%5 == 0 {
					appendLevel(ancestorDef, 0)
					continue
				} // NULL scalar/list
				if leaf.kind == 1 {
					appendLevel(ancestorDef+1, 0)
					sv = append(sv, arrowparquet.ByteArray(recursiveReaderText(row, 1)))
					continue
				}
				n := recursiveReaderListLen(row)
				if n == 0 {
					appendLevel(ancestorDef+1, 0)
					continue
				}
				for j := 0; j < n; j++ {
					r := int16(0)
					if j > 0 {
						r = 1
					}
					d := ancestorDef + 3
					if j%7 == 3 {
						d--
					}
					if badLeaf == -2 && leaf.kind == 3 && row == 11 {
						if j == 0 {
							r = 1
						}
						if j == 1 {
							r = 0
						}
					}
					malformed := badLeaf == -3 && leaf.kind == 3 && row == 11 && j == 2
					if malformed {
						d = ancestorDef
					}
					appendLevel(d, r)
					if malformed {
						continue
					}
					if j%7 == 3 {
						continue
					}
					if leaf.kind == 2 {
						iv = append(iv, int64(row*10000+j))
					} else {
						sv = append(sv, arrowparquet.ByteArray(recursiveReaderText(row, j)))
					}
				}
			}
			if leaf.kind == 0 || leaf.kind == 2 {
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
		base += count
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}
func recursiveReaderDomain(optional []bool, level, row int) (int16, bool) {
	var d int16
	// Rows 0..depth-1 each hide one optional ancestor; other rows are visible.
	hidden := row % (len(optional) + 4)
	for i := 0; i <= level; i++ {
		if optional[i] {
			if hidden == i {
				return d, false
			}
			d++
		}
	}
	return d, true
}
func recursiveReaderText(row, item int) string {
	if row == 11 && item == 1 {
		return "xx" + strings.Repeat("界", 44000) + "z"
	}
	if item%5 == 0 {
		return ""
	}
	return fmt.Sprintf("界/%d/%d", row, item)
}
func recursiveReaderListLen(row int) int {
	if row%4 == 0 {
		return 0
	}
	if row%13 == 11 {
		return 4097
	}
	return row%6 + 1
}
func recursiveReaderCheck(t *testing.T, b *memory.Batch, optional []bool, variable bool, start int) {
	t.Helper()
	for r := 0; r < b.Rows; r++ {
		if b.Int64s["root.dot.n"].Values[r] != int64((start+r)*100) {
			t.Fatal("top dotted name collision")
		}
	}
	node := b.Structs["root.dot"]
	for level := range optional {
		if node == nil || node.Rows != b.Rows || node.Values.Rows != b.Rows {
			t.Fatal("recursive row shape", level)
		}
		for r := 0; r < b.Rows; r++ {
			_, valid := recursiveReaderDomain(optional, level, start+r)
			if node.IsValid(r) != valid {
				t.Fatalf("ancestor level=%d row=%d validity got=%v want=%v", level, start+r, node.IsValid(r), valid)
			}
			if valid && node.Values.Int64s["n"].Values[r] != int64((start+r)*100+level+1) {
				t.Fatal("witness", level, start+r)
			}
		}
		if level+1 < len(optional) {
			node = node.Values.Structs["inner"]
		}
	}
	if !variable {
		return
	}
	for r := 0; r < b.Rows; r++ {
		row := start + r
		_, visible := recursiveReaderDomain(optional, len(optional)-1, row)
		valid := visible && row%5 != 0
		text := node.Values.Strings["text"]
		if text.IsValid(r) != valid {
			t.Fatal("String NULL", row)
		}
		if valid && string(text.ValueBytes(r)) != recursiveReaderText(row, 1) {
			t.Fatal("String value", row)
		}
		ints, ss := node.Values.Int64Lists["ints"], node.Values.StringLists["strings"]
		if ints.IsValid(r) != valid || ss.IsValid(r) != valid {
			t.Fatal("List NULL", row)
		}
		a, z, _ := ints.ValueRange(r)
		x, y, _ := ss.ValueRange(r)
		n := 0
		if valid {
			n = recursiveReaderListLen(row)
		}
		if z-a != n || y-x != n {
			t.Fatal("List length", row, z-a, y-x, n)
		}
		for j := 0; j < n; j++ {
			itemValid := j%7 != 3
			if ints.Values.IsValid(a+j) != itemValid || ss.Values.IsValid(x+j) != itemValid {
				t.Fatal("element NULL", row, j)
			}
			if itemValid && (ints.Values.Values[a+j] != int64(row*10000+j) || string(ss.Values.ValueBytes(x+j)) != recursiveReaderText(row, j)) {
				t.Fatal("element value", row, j)
			}
		}
	}
}
func TestNativeNestedRecursiveReaderMatrix(t *testing.T) {
	for _, depth := range []int{1, 2, 8} {
		for _, pattern := range []int{0, 1, 2} {
			for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
				t.Run(fmt.Sprintf("depth%d/pattern%d/page%d", depth, pattern, version), func(t *testing.T) {
					optional := make([]bool, depth)
					for i := range optional {
						optional[i] = pattern == 1 || pattern == 2 && i%2 == 0
					}
					path := recursiveReaderFixture(t, optional, []int{13, 0, 24}, true, -1, arrowparquet.WithDataPageVersion(version), arrowparquet.WithDictionaryDefault(pattern != 0))
					src, e := OpenFileWithOptions(path, Options{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					m := memory.NewBudgetManager(0)
					opts := testReadOptions()
					opts.MemoryManager = m
					opts.BatchRows = 5
					opts.BatchBytes = 2 << 20
					st, e := source.OpenBatchStream(context.Background(), src, opts)
					if e != nil {
						t.Fatal(e)
					}
					defer st.Close()
					pos := 0
					for {
						b := memory.NewBatch()
						ok, e := st.Next(context.Background(), b)
						if e != nil {
							t.Fatal(e)
						}
						if !ok {
							break
						}
						recursiveReaderCheck(t, b, optional, true, pos)
						if e = b.Structs["root.dot"].ValidateRecursiveWithContext(src.Schema().Fields[1], b.Rows, nil); e != nil {
							t.Fatal(e)
						}
						pos += b.Rows
					}
					st.Close()
					if pos != 37 || m.Used() != 0 {
						t.Fatal("rows/cleanup", pos, m.Used())
					}
				})
			}
		}
	}
}
func TestNativeNestedRecursiveReaderScopeAndConsensus(t *testing.T) {
	for _, depth := range []int{2, 8, 9} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			optional := make([]bool, depth)
			for i := range optional {
				optional[i] = true
			}
			path := recursiveReaderFixture(t, optional, []int{17}, false, -1)
			for _, old := range []Options{{}} {
				if _, e := OpenFileWithOptions(path, old); e == nil {
					t.Fatal("legacy flag scope expanded", old)
				}
			}
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if depth == 9 {
				if e == nil {
					t.Fatal("depth9 accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			pos := 0
			for {
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				recursiveReaderCheck(t, b, optional, false, pos)
				pos += b.Rows
			}
			st.Close()
			if pos != 17 || m.Used() != 0 {
				t.Fatal("fixed-only", pos, m.Used())
			}
		})
	}
	for _, bad := range []int{2, 8, 9, 10, 11} {
		t.Run(fmt.Sprintf("badleaf%d", bad), func(t *testing.T) {
			optional := []bool{true, true, true, true, true, true, true, true}
			path := recursiveReaderFixture(t, optional, []int{17}, true, bad)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			var found error
			for {
				ok, e := st.Next(context.Background(), memory.NewBatch())
				if e != nil {
					found = e
					break
				}
				if !ok {
					break
				}
			}
			st.Close()
			if found == nil || !strings.Contains(found.Error(), "parent") || m.Used() != 0 {
				t.Fatal("ancestor consensus/cleanup", found, m.Used())
			}
		})
	}
}

func TestNativeNestedRecursiveReaderBudgetFailuresAndCancellation(t *testing.T) {
	optional := []bool{true, false, true, false, true, false, true, false}
	path := recursiveReaderFixture(t, optional, []int{17}, true, -1)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	run := func(m memory.Manager) error {
		opts := testReadOptions()
		opts.MemoryManager = m
		opts.BatchRows = 5
		opts.BatchBytes = 2 << 20
		st, e := source.OpenBatchStream(context.Background(), src, opts)
		if e != nil {
			return e
		}
		defer st.Close()
		pos := 0
		for {
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				return e
			}
			if !ok {
				break
			}
			recursiveReaderCheck(t, b, optional, true, pos)
			pos += b.Rows
		}
		if pos != 17 {
			t.Fatalf("rows after prefix recovery %d", pos)
		}
		return nil
	}
	probe := &fixedTestFailManager{Manager: memory.NewBudgetManager(0)}
	if e = run(probe); e != nil {
		t.Fatal(e)
	}
	for fail := 1; fail <= probe.calls; fail++ {
		m := &fixedTestFailManager{Manager: memory.NewBudgetManager(0), fail: fail}
		e := run(m)
		// Resource denial after a prefix may publish it and retry the paid pending row.
		// Either terminal refusal or complete exact recovery is valid, never data loss.
		if e != nil && !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("wrong admission error", fail, e)
		}
		if m.calls < fail || m.Used() != 0 {
			t.Fatal("injection/cleanup", fail, m.calls, m.Used())
		}
		m.fail = 0
		if e = run(m); e != nil || m.Used() != 0 {
			t.Fatal("fresh retry", fail, e, m.Used())
		}
	}
	opts := testReadOptions()
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	ctx := &fixedTestCancelContext{Context: context.Background()}
	st, e := source.OpenBatchStream(ctx, src, opts)
	if e != nil {
		t.Fatal(e)
	}
	ctx.armed = true
	ok, e := st.Next(ctx, memory.NewBatch())
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || ctx.checks < 40 || m.Used() != 0 {
		t.Fatal("running cancellation", ok, e, ctx.checks, m.Used())
	}
}

func TestNativeNestedRecursiveReaderExactNodeBudget(t *testing.T) {
	var previous int64
	for _, depth := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			optional := make([]bool, depth)
			for i := range optional {
				optional[i] = true
			}
			path := recursiveReaderFixture(t, optional, []int{1}, false, -1)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			run := func(m memory.Manager) error {
				opts := testReadOptions()
				opts.BatchRows = 1
				opts.BatchBytes = 1 << 20
				opts.MemoryManager = m
				st, e := source.OpenBatchStream(context.Background(), src, opts)
				if e != nil {
					return e
				}
				defer st.Close()
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					return e
				}
				if !ok {
					t.Fatal("no first row")
				}
				recursiveReaderCheck(t, b, optional, false, 0)
				return nil
			}
			peak := newBatchStreamPeakManager(0)
			if e = run(peak); e != nil {
				t.Fatal(e)
			}
			charge := peak.peak.Load()
			if charge <= previous || peak.Used() != 0 {
				t.Fatal("each extra recursive node must be charged", depth, charge, previous)
			}
			previous = charge
			exact := memory.NewBudgetManager(charge)
			if e = run(exact); e != nil || exact.Used() != 0 {
				t.Fatal("exact node budget", charge, e, exact.Used())
			}
			short := memory.NewBudgetManager(charge - 1)
			if e = run(short); !errors.Is(e, memory.ErrBudgetExceeded) || short.Used() != 0 {
				t.Fatal("node budget -1", charge, e, short.Used())
			}
			t.Logf("depth=%d measured exact manager peak=%d", depth, charge)
		})
	}
}

func TestNativeNestedRecursiveReaderSchemaProjectionAndPartitions(t *testing.T) {
	optional := []bool{true, false, true, true, false, true, false, true}
	path := recursiveReaderFixture(t, optional, []int{3, 4, 5}, true, -1)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	// Source schema snapshots cannot alias plan metadata at any depth.
	snapshot := src.Schema()
	f := &snapshot.Fields[1]
	for level := 0; level < 8; level++ {
		f.Name = "poison"
		f.Children[0].Name = "poison"
		if level < 7 {
			f = &f.Children[1]
		}
	}
	parts, e := src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for _, names := range [][]string{{"root.dot", "root.dot.n"}, {"root.dot.n"}} {
		projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: names})
		if e != nil {
			t.Fatal(e)
		}
		opts := testReadOptions()
		opts.BatchRows = 2
		opts.BatchBytes = 1 << 20
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), projected, opts, []source.Partition{parts[2], parts[0]})
		if e != nil {
			t.Fatal(e)
		}
		expected := []int{7, 8, 9, 10, 11, 0, 1, 2}
		pos := 0
		for {
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				t.Fatal(e)
			}
			if !ok {
				break
			}
			for row := 0; row < b.Rows; row++ {
				if b.Int64s["root.dot.n"].Values[row] != int64(expected[pos+row]*100) {
					t.Fatal("partition order")
				}
			}
			if len(names) == 2 {
				recursiveReaderCheck(t, b, optional, true, expected[pos])
			} else if len(b.Structs) != 0 {
				t.Fatal("projection retained unselected tree")
			}
			pos += b.Rows
		}
		st.Close()
		if pos != len(expected) || m.Used() != 0 {
			t.Fatal("projection/partition cleanup", pos, m.Used())
		}
	}
}

func recursiveReaderFixedFixture(t *testing.T, depth int, groups []int, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	optional, inconsistent := true, false
	kinds := fixedTestKinds()
	children := schema.FieldList{}
	for j, k := range kinds {
		rep := arrowparquet.Repetitions.Required
		if j%2 == 0 {
			rep = arrowparquet.Repetitions.Optional
		}
		node, e := schema.NewPrimitiveNodeLogical(k.name, rep, k.logical, k.physical, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		children = append(children, node)
	}
	rep := arrowparquet.Repetitions.Required
	if optional {
		rep = arrowparquet.Repetitions.Optional
	}
	parent, e := schema.NewGroupNode("record", rep, children, -1)
	if e != nil {
		t.Fatal(e)
	}
	id, e := schema.NewPrimitiveNodeLogical("id", arrowparquet.Repetitions.Required, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	var wrapped schema.Node = parent
	for level := 1; level < depth; level++ {
		n, err := schema.NewGroupNode("record", arrowparquet.Repetitions.Required, schema.FieldList{wrapped}, -1)
		if err != nil {
			t.Fatal(err)
		}
		wrapped = n
	}
	root, e := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{id, wrapped}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "fixed.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(64), arrowparquet.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	for _, n := range groups {
		rg := w.AppendRowGroup()
		c, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		ids := make([]int64, n)
		for r := range ids {
			ids[r] = int64(base + r)
		}
		if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
		for j := range kinds {
			c, e = rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			defs := make([]int16, n)
			var i32 []int32
			var i64 []int64
			var f32 []float32
			var f64 []float64
			var bs []bool
			for r := 0; r < n; r++ {
				row := base + r
				pv := !optional || row%5 != 0
				if inconsistent && j == 1 && row == 0 {
					pv = true
				}
				cv := j%2 != 0 || row%3 != 1
				if !pv {
					continue
				}
				if optional {
					defs[r]++
				}
				if !cv {
					continue
				}
				if j%2 == 0 {
					defs[r]++
				}
				v := int64(row%50 + 1)
				i32 = append(i32, int32(v))
				i64 = append(i64, v)
				f32 = append(f32, float32(v)+0.25)
				f64 = append(f64, float64(v)+0.25)
				bs = append(bs, row%2 == 0)
			}
			switch col := c.(type) {
			case *file.Int32ColumnChunkWriter:
				_, e = col.WriteBatch(i32, defs, nil)
			case *file.Int64ColumnChunkWriter:
				_, e = col.WriteBatch(i64, defs, nil)
			case *file.Float32ColumnChunkWriter:
				_, e = col.WriteBatch(f32, defs, nil)
			case *file.Float64ColumnChunkWriter:
				_, e = col.WriteBatch(f64, defs, nil)
			case *file.BooleanColumnChunkWriter:
				_, e = col.WriteBatch(bs, defs, nil)
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
func TestNativeNestedRecursiveReaderAllFixedKinds(t *testing.T) {
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			path := recursiveReaderFixedFixture(t, depth, []int{7, 11})
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 3
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			pos := 0
			for {
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				v := b.Structs["record"]
				if e = v.ValidateRecursiveWithContext(src.Schema().Fields[1], b.Rows, nil); e != nil {
					t.Fatal(e)
				}
				for level := 1; level < depth; level++ {
					for row := 0; row < b.Rows; row++ {
						if !v.IsValid(row) {
							t.Fatal("required wrapper NULL")
						}
					}
					v = v.Values.Structs["record"]
				}
				// The shared checker is an independent numeric/null oracle, not another decoder.
				flat := memory.NewBatch()
				flat.Rows = b.Rows
				flat.Int64s["id"] = b.Int64s["id"]
				flat.Structs["record"] = v
				ids := make([]int, b.Rows)
				for i := range ids {
					ids[i] = pos + i
				}
				fixedTestCheck(t, flat, true, ids)
				pos += b.Rows
			}
			st.Close()
			if pos != 18 || m.Used() != 0 {
				t.Fatal("fixed kinds cleanup", pos, m.Used())
			}
		})
	}
}

func TestNativeNestedRecursiveReaderPartialAncestorLookahead(t *testing.T) {
	optional := []bool{true, true, true, true, true, true, true, true}
	path := recursiveReaderFixture(t, optional, []int{17}, true, -1)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := &nestedTestFaultManager{Manager: memory.NewBudgetManager(0), stage: 2}
	opts := testReadOptions()
	opts.MemoryManager = m
	opts.BatchRows = 17
	opts.BatchBytes = 2 << 20
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	m.native = st.(*nestedListStream)
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if e != nil || !ok || !m.hit || b.Rows != 11 {
		t.Fatal("deep partial prefix", ok, e, m.hit, b.Rows)
	}
	recursiveReaderCheck(t, b, optional, true, 0)
	ready, partial := m.native.cursors[1], m.native.cursors[2]
	if !ready.complete || !ready.tuple.present || ready.tuple.rep[0] != 0 || ready.tuple.def[0] != 0 {
		t.Fatal("missing next-row NULL ancestor lookahead")
	}
	if partial.complete || !partial.tuple.present || partial.stageCount() != m.count || string(partial.bytes[0]) != m.pending {
		t.Fatal("partial paid byte view lost")
	}
	for level := 0; level < 8; level++ {
		if !ready.witness[level] || !partial.witness[level] {
			t.Fatal("lookahead/partial overwrote current ancestor", level)
		}
	}
	pos := b.Rows
	for {
		b = memory.NewBatch()
		ok, e = st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		recursiveReaderCheck(t, b, optional, true, pos)
		pos += b.Rows
	}
	st.Close()
	if pos != 17 || m.Used() != 0 {
		t.Fatal("partial rows/cleanup", pos, m.Used())
	}
}

type recursiveReaderCopyContext struct {
	context.Context
	stream   *nestedListStream
	checks   int
	canceled bool
}

func (c *recursiveReaderCopyContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	s := c.stream
	if s.current == 2 && len(s.cursors[2].strings.stageData) > 64<<10 && len(s.cursors[2].bytes[0]) > 64<<10 {
		var pcs [16]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, ".copyBytes") {
				c.checks++
				if c.checks == 2 {
					c.canceled = true
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
func TestNativeNestedRecursiveReaderLongCopyCancel(t *testing.T) {
	optional := []bool{true, true, true, true, true, true, true, true}
	path := recursiveReaderFixture(t, optional, []int{11, 1}, true, -1)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	parts, e := src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 1
	opts.BatchBytes = 2 << 20
	m := newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = m
	st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
	if e != nil {
		t.Fatal(e)
	}
	ctx := &recursiveReaderCopyContext{Context: context.Background(), stream: st.(*nestedListStream)}
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || b.Rows != 0 || m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
		t.Fatal("deep long-copy cancellation", ok, e, ctx.checks, b.Rows, m.Used(), m.peak.Load())
	}
	st, e = source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
	if e != nil {
		t.Fatal(e)
	}
	ok, e = st.Next(context.Background(), b)
	if !ok || e != nil {
		t.Fatal("copy retry", ok, e)
	}
	recursiveReaderCheck(t, b, optional, true, 11)
	st.Close()
	if m.Used() != 0 {
		t.Fatal("retry cleanup")
	}
}

func TestNativeNestedRecursiveReaderCorruptFooter(t *testing.T) {
	for _, col := range []int{8, 10, 11} {
		for _, delta := range []int64{-1, 1} {
			t.Run(fmt.Sprintf("column%d/delta%d", col, delta), func(t *testing.T) {
				optional := []bool{true, true, true, true, true, true, true, true}
				path := recursiveReaderFixture(t, optional, []int{17}, true, -1)
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				src.meta.RowGroups[0].Columns[col].MetaData.NumValues += delta
				footer, e := src.meta.Serialize(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				raw, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				old := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
				raw = append(raw[:len(raw)-8-old], footer...)
				var size [4]byte
				binary.LittleEndian.PutUint32(size[:], uint32(len(footer)))
				raw = append(raw, size[:]...)
				raw = append(raw, []byte("PAR1")...)
				if e = os.WriteFile(path, raw, 0600); e != nil {
					t.Fatal(e)
				}
				src, e = OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					return
				}
				m := memory.NewBudgetManager(0)
				opts := testReadOptions()
				opts.MemoryManager = m
				st, e := source.OpenBatchStream(context.Background(), src, opts)
				if e == nil {
					for {
						ok, err := st.Next(context.Background(), memory.NewBatch())
						if err != nil {
							e = err
							break
						}
						if !ok {
							break
						}
					}
					st.Close()
				}
				if e == nil || m.Used() != 0 {
					t.Fatal("corrupt footer accepted/leaked", e, m.Used())
				}
			})
		}
	}
}

func TestNativeNestedRecursiveReaderMalformedLevels(t *testing.T) {
	for _, bad := range []int{-2, -3} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			optional := []bool{true, true, true, true, true, true, true, true}
			path := recursiveReaderFixture(t, optional, []int{17}, true, bad)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			var found error
			for {
				ok, err := st.Next(context.Background(), memory.NewBatch())
				if err != nil {
					found = err
					break
				}
				if !ok {
					break
				}
			}
			st.Close()
			if found == nil || (!strings.Contains(found.Error(), "continuation") && !strings.Contains(found.Error(), "row start")) || m.Used() != 0 {
				t.Fatal("malformed rep/def", found, m.Used())
			}
		})
	}
}

func TestNativeNestedRecursiveReaderBranchNamespace(t *testing.T) {
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	leaf := func() schema.Node {
		n, e := schema.NewPrimitiveNodeLogical("x", req, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	group := func(name string, children schema.FieldList) schema.Node {
		n, e := schema.NewGroupNode(name, opt, children, -1)
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	// Literal branch.dot and branch/dot must never share a path-derived identity.
	rootNode := group("root", schema.FieldList{group("branch.dot", schema.FieldList{leaf()}), group("branch", schema.FieldList{group("dot", schema.FieldList{leaf()})})})
	root, e := schema.NewGroupNode("schema", req, schema.FieldList{rootNode}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "branch.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := file.NewParquetWriter(f, root)
	rg := w.AppendRowGroup()
	for i := 0; i < 2; i++ {
		c, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		defs := []int16{0, 1, 2, 2}
		values := []int64{102, 103}
		if i == 1 {
			defs = []int16{0, 3, 1, 3}
			values = []int64{201, 203}
		}
		if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(values, defs, nil); e != nil {
			t.Fatal(e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if e = rg.Close(); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 4
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if e != nil || !ok || b.Rows != 4 {
		t.Fatal(ok, e, b.Rows)
	}
	r := b.Structs["root"]
	left, right := r.Values.Structs["branch.dot"], r.Values.Structs["branch"].Values.Structs["dot"]
	for row := 0; row < 4; row++ {
		if r.IsValid(row) != (row != 0) || left.IsValid(row) != (row == 2 || row == 3) || right.IsValid(row) != (row == 1 || row == 3) {
			t.Fatal("branch identities/ancestor masks", row)
		}
	}
	if left.Values.Int64s["x"].Values[2] != 102 || left.Values.Int64s["x"].Values[3] != 103 || right.Values.Int64s["x"].Values[1] != 201 || right.Values.Int64s["x"].Values[3] != 203 {
		t.Fatal("branch values aliased")
	}
	if e = r.ValidateRecursiveWithContext(src.Schema().Fields[0], 4, nil); e != nil {
		t.Fatal(e)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("branch cleanup")
	}
}

func TestNativeNestedRecursiveReaderProjectedBranchRemap(t *testing.T) {
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	group := func(name string, children schema.FieldList) schema.Node {
		n, e := schema.NewGroupNode(name, opt, children, -1)
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	tree := func(name string) schema.Node {
		branches := schema.FieldList{}
		for _, branch := range []string{"left", "right"} {
			x, e := schema.NewPrimitiveNodeLogical("x", req, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
			if e != nil {
				t.Fatal(e)
			}
			branches = append(branches, group(branch, schema.FieldList{group("same", schema.FieldList{x})}))
		}
		return group(name, branches)
	}
	root, e := schema.NewGroupNode("schema", req, schema.FieldList{tree("earlier"), tree("retained")}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "branch-remap.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := file.NewParquetWriter(f, root)
	rg := w.AppendRowGroup()
	defs := [][]int16{{0, 1, 3, 3}, {0, 3, 2, 3}, {3, 2, 1, 0}, {2, 3, 3, 0}}
	values := [][]int64{{102, 103}, {201, 203}, {300}, {401, 402}}
	for col := range defs {
		c, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(values[col], defs[col], nil); e != nil {
			t.Fatal(e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if e = rg.Close(); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, names := range [][]string{{"earlier", "retained"}, {"retained"}} {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: names})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 2
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), projected, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			pos := 0
			for {
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				if len(b.Structs) != len(names) {
					t.Fatal("unselected top Struct leaked")
				}
				for _, name := range names {
					base := 0
					if name == "retained" {
						base = 2
					}
					r := b.Structs[name]
					for j, branch := range []string{"left", "right"} {
						v := r.Values.Structs[branch]
						inner := v.Values.Structs["same"]
						for row := 0; row < b.Rows; row++ {
							global := pos + row
							d := defs[base+j][global]
							if r.IsValid(row) != (d >= 1) || v.IsValid(row) != (d >= 2) || inner.IsValid(row) != (d >= 3) {
								t.Fatalf("remap NULL mismatch tree=%s branch=%s row=%d", name, branch, global)
							}
							if d == 3 && inner.Values.Int64s["x"].Values[row] != int64((base+j+1)*100+global) {
								t.Fatal("branch values mixed", name, branch, global)
							}
						}
					}
					field, ok := projected.Schema().Field(name)
					if !ok {
						t.Fatal("projected field missing")
					}
					if e = r.ValidateRecursiveWithContext(field, b.Rows, nil); e != nil {
						t.Fatal(e)
					}
				}
				pos += b.Rows
			}
			st.Close()
			if pos != 4 || m.Used() != 0 {
				t.Fatal("remap cleanup", pos, m.Used())
			}
		})
	}
}
