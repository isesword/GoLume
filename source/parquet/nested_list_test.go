package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

type nestedReaderConfig struct {
	longBytes         int
	text, O, I, E     bool
	multi, emptyLarge bool
	depth, large      int
	bad               string
}

func nestedReaderValue(c nestedReaderConfig, row int) any {
	val := func(i int) any {
		if c.text {
			if c.longBytes > 0 && row%8 == 6 && i == 0 {
				return strings.Repeat("界", c.longBytes/3) + "z"
			}
			return []string{"", "界\x00", fmt.Sprintf("r%d/e%d", row, i)}[i%3]
		}
		return int64(row*100 + i - 700)
	}
	switch row % 8 {
	case 0:
		if c.O {
			return nil
		}
		return []any{}
	case 1:
		return []any{}
	case 2:
		if c.I {
			return []any{nil}
		}
		return []any{[]any{}}
	case 3:
		return []any{[]any{}}
	case 4:
		if c.E {
			return []any{[]any{nil}}
		}
		return []any{[]any{val(1)}}
	case 5:
		return []any{[]any{val(0), val(1)}, []any{}, []any{val(2)}}
	case 6:
		if c.emptyLarge {
			a := make([]any, c.large)
			for i := range a {
				a[i] = []any{}
			}
			if c.longBytes > 0 {
				a = append(a, []any{val(0)})
			}
			return a
		}
		a := []any{}
		for i := 0; i < c.large; i++ {
			v := val(i)
			if c.E && i%7 == 1 {
				v = nil
			}
			a = append(a, v)
		}
		return []any{a, []any{}, []any{val(2)}}
	default:
		return []any{[]any{val(2)}, []any{val(1), val(0)}}
	}
}
func nestedReaderDomain(c nestedReaderConfig, row int) (int16, bool) {
	for i := 0; i < c.depth; i++ {
		if row%19 == i {
			return int16(i), false
		}
	}
	return int16(c.depth), true
}
func nestedReaderWrite(t *testing.T, c nestedReaderConfig, groups []int, props ...pq.WriterProperty) string {
	t.Helper()
	req, opt := pq.Repetitions.Required, pq.Repetitions.Optional
	rep := func(b bool) pq.Repetition {
		if b {
			return opt
		}
		return req
	}
	makeNode := func(name string, text bool) schema.Node {
		kind := pq.Types.Int64
		var logical schema.LogicalType = schema.NoLogicalType{}
		if text {
			kind = pq.Types.ByteArray
			logical = schema.StringLogicalType{}
		}
		leaf, e := schema.NewPrimitiveNodeLogical("element", rep(c.E), logical, kind, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		wrap, e := schema.NewGroupNode("list", pq.Repetitions.Repeated, schema.FieldList{leaf}, -1)
		if e != nil {
			t.Fatal(e)
		}
		inner, e := schema.NewGroupNodeLogical("element", rep(c.I), schema.FieldList{wrap}, schema.ListLogicalType{}, -1)
		if e != nil {
			t.Fatal(e)
		}
		outerWrap, e := schema.NewGroupNode("list", pq.Repetitions.Repeated, schema.FieldList{inner}, -1)
		if e != nil {
			t.Fatal(e)
		}
		outer, e := schema.NewGroupNodeLogical(name, rep(c.O), schema.FieldList{outerWrap}, schema.ListLogicalType{}, -1)
		if e != nil {
			t.Fatal(e)
		}
		var node schema.Node = outer
		for d := c.depth - 1; d >= 0; d-- {
			node, e = schema.NewGroupNode(fmt.Sprintf("%s.node%d", name, d), opt, schema.FieldList{node}, -1)
			if e != nil {
				t.Fatal(e)
			}
		}
		return node
	}
	node := makeNode("items", c.text)
	id, e := schema.NewPrimitiveNodeLogical("id", req, schema.NoLogicalType{}, pq.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	top := schema.FieldList{id, node}
	if c.multi {
		top = append(top, makeNode("other.items", !c.text))
	}
	root, e := schema.NewGroupNode("schema", req, top, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "nested.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]pq.WriterProperty{pq.WithDataPageSize(128), pq.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(pq.NewWriterProperties(props...)))
	base := 0
	for _, n := range groups {
		rg := w.AppendRowGroup()
		col, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = int64(base + i)
		}
		if _, e = col.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e = col.Close(); e != nil {
			t.Fatal(e)
		}
		columns := 1
		if c.multi {
			columns = 2
		}
		for column := 0; column < columns; column++ {
			cc := c
			if column == 1 {
				cc.text = !cc.text
			}
			col, e = rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			defs, reps := []int16{}, []int16{}
			iv := []int64{}
			sv := []pq.ByteArray{}
			emit := func(d, r int16, v any) {
				defs = append(defs, d)
				reps = append(reps, r)
				if v != nil {
					if cc.text {
						sv = append(sv, pq.ByteArray(v.(string)))
					} else {
						iv = append(iv, v.(int64))
					}
				}
			}
			for row := base + column; row < base+n+column; row++ {
				d, visible := nestedReaderDomain(c, row)
				if !visible {
					emit(d, 0, nil)
					continue
				}
				value := nestedReaderValue(cc, row)
				if value == nil {
					emit(d, 0, nil)
					continue
				}
				if c.O {
					d++
				}
				inners := value.([]any)
				if len(inners) == 0 {
					emit(d, 0, nil)
					continue
				}
				d++
				for j, in := range inners {
					r := int16(0)
					if j > 0 {
						r = 1
					}
					if in == nil {
						emit(d, r, nil)
						continue
					}
					innerD := d
					if c.I {
						innerD++
					}
					vs := in.([]any)
					if len(vs) == 0 {
						emit(innerD, r, nil)
						continue
					}
					innerD++
					for k, v := range vs {
						rr := r
						if k > 0 {
							rr = 2
						}
						dd := innerD
						if v != nil && c.E {
							dd++
						}
						emit(dd, rr, v)
					}
				}
			}
			if c.bad == "first_rep" && len(reps) > 0 {
				reps[0] = 2
				for j := 1; j < len(reps); j++ {
					if reps[j] == 2 {
						reps[j] = 0
						break
					}
				}
			}
			if c.bad == "null_inner_rep2" || c.bad == "empty_inner_rep2" {
				at := 3
				prior := int16(2)
				if c.bad == "empty_inner_rep2" {
					at = 4
					prior = 3
				}
				if c.depth != 0 || !c.O || !c.I || !c.E || len(defs) < 8 || defs[at-1] != prior || reps[at-1] != 0 {
					t.Fatal("bad continuation fixture precondition")
				}
				// Move a real valid-value event after the placeholder. Preserve
				// the physical value count and total rep0 parent count.
				found := -1
				for j := at + 1; j < len(defs); j++ {
					if defs[j] == 5 {
						found = j
						break
					}
				}
				if found < 0 {
					t.Fatal("missing valid continuation event")
				}
				defs[at], defs[found] = defs[found], defs[at]
				reps[at] = 2
				balanced := false
				for j := at + 1; j < len(reps); j++ {
					if reps[j] != 0 {
						reps[j] = 0
						balanced = true
						break
					}
				}
				if !balanced || defs[at] != 5 {
					t.Fatal("continuation fixture not balanced")
				}
			}
			if cc.text {
				_, e = col.(*file.ByteArrayColumnChunkWriter).WriteBatch(sv, defs, reps)
			} else {
				_, e = col.(*file.Int64ColumnChunkWriter).WriteBatch(iv, defs, reps)
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = col.Close(); e != nil {
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
func nestedReaderNodeValue(v *memory.ArrayNode, row int) any {
	if !v.IsValid(row) {
		return nil
	}
	if v.Kind == memory.TypeList {
		a := make([]any, v.Offsets[row+1]-v.Offsets[row])
		for i := v.Offsets[row]; i < v.Offsets[row+1]; i++ {
			a[i-v.Offsets[row]] = nestedReaderNodeValue(v.Child, int(i))
		}
		return a
	}
	if v.Kind == memory.TypeInt64 {
		return v.Int64.Values[row]
	}
	return string(v.Strings.ValueBytes(row))
}
func nestedReaderCheck(t *testing.T, b *memory.Batch, c nestedReaderConfig, base int) {
	t.Helper()
	columns := 1
	if c.multi {
		columns = 2
	}
	for column := 0; column < columns; column++ {
		name := "items"
		if column == 1 {
			name = "other.items"
		}
		cur := b
		for d := 0; d < c.depth; d++ {
			v := cur.Structs[fmt.Sprintf("%s.node%d", name, d)]
			if v == nil || v.Rows != b.Rows {
				t.Fatal("ancestor shape")
			}
			cur = v.Values
		}
		a := cur.Arrays[name]
		if a == nil || len(a.Offsets) != b.Rows+1 {
			t.Fatal("missing R0")
		}
		for i := 0; i < b.Rows; i++ {
			row := base + i
			if b.Int64s["id"].Values[i] != int64(row) {
				t.Fatal("id sync")
			}
			cc := c
			if column == 1 {
				cc.text = !cc.text
			}
			want := nestedReaderValue(cc, row+column)
			_, visible := nestedReaderDomain(c, row+column)
			if !visible {
				want = nil
			}
			if got := nestedReaderNodeValue(a, i); !reflect.DeepEqual(got, want) {
				t.Fatalf("row%d got%#v want%#v", row, got, want)
			}
		}
	}
}
func TestNativeNestedTwoListsDecodeMatrix(t *testing.T) {
	for _, text := range []bool{false, true} {
		for bits := 0; bits < 8; bits++ {
			for _, depth := range []int{0, 8} {
				c := nestedReaderConfig{text: text, O: bits&1 != 0, I: bits&2 != 0, E: bits&4 != 0, depth: depth, large: 65}
				t.Run(fmt.Sprint(text, bits, depth), func(t *testing.T) {
					path := nestedReaderWrite(t, c, []int{0, 19, 17})
					src, e := OpenFileWithOptions(path, Options{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					opts := testReadOptions()
					opts.BatchRows = 3
					opts.BatchBytes = 2 << 20
					m := memory.NewBudgetManager(0)
					opts.MemoryManager = m
					st, e := source.OpenBatchStream(context.Background(), src, opts)
					if e != nil {
						t.Fatal(e)
					}
					defer st.Close()
					base := 0
					for {
						b := memory.NewBatch()
						ok, e := st.Next(context.Background(), b)
						if e != nil {
							t.Fatal(e)
						}
						if !ok {
							break
						}
						nestedReaderCheck(t, b, c, base)
						base += b.Rows
					}
					if base != 36 {
						t.Fatal("lost parents", base)
					}
					st.Close()
					if m.Used() != 0 {
						t.Fatal("leak", m.Used())
					}
				})
			}
		}
	}
}

func nestedReaderDrain(t *testing.T, src *Source, c nestedReaderConfig, opts source.ReadOptions) (int, error) {
	t.Helper()
	st, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		return 0, e
	}
	defer st.Close()
	seen := 0
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			return seen, e
		}
		if !ok {
			return seen, nil
		}
		if err := memory.ValidateNestedListsBatch(b, src.Schema(), nil); err != nil {
			t.Fatal("full output validation", err)
		}
		nestedReaderCheck(t, b, c, seen)
		seen += b.Rows
	}
}
func TestNativeNestedTwoListsPagesAndMetadata(t *testing.T) {
	for _, text := range []bool{false, true} {
		for _, version := range []pq.DataPageVersion{pq.DataPageV1, pq.DataPageV2} {
			for _, dict := range []bool{false, true} {
				t.Run(fmt.Sprint(text, version, dict), func(t *testing.T) {
					c := nestedReaderConfig{text: text, O: true, I: true, E: true, large: 4097}
					path := nestedReaderWrite(t, c, []int{17}, pq.WithDataPageVersion(version), pq.WithDictionaryDefault(dict))
					r, e := file.OpenParquetFile(path, false)
					if e != nil {
						t.Fatal(e)
					}
					defer r.Close()
					desc := r.MetaData().Schema.Column(1)
					if desc.MaxRepetitionLevel() != 2 || desc.MaxDefinitionLevel() != 5 {
						t.Fatal("physical levels", desc.MaxRepetitionLevel(), desc.MaxDefinitionLevel())
					}
					pages, e := r.RowGroup(0).GetColumnPageReader(1)
					if e != nil {
						t.Fatal(e)
					}
					count, max := 0, int32(0)
					for pages.Next() {
						p := pages.Page()
						if p.Type() == file.PageTypeDataPage || p.Type() == file.PageTypeDataPageV2 {
							count++
							if p.NumValues() > max {
								max = p.NumValues()
							}
						}
					}
					if e = pages.Err(); e != nil {
						t.Fatal(e)
					}
					pages.Close()
					t.Logf("actual version=%v dict=%v dataPages=%d maxPageLevels=%d; giant inner leaf slots=4097", version, dict, count, max)
					if count < 2 {
						t.Fatal("not multipage")
					}
					if version == pq.DataPageV1 && max >= 4097 {
						t.Fatal("V1 no proven giant row crossing")
					}
					if version == pq.DataPageV2 && max < 4097 {
						t.Fatal("unexpected V2 row split")
					}
					src, e := OpenFileWithOptions(path, Options{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					opts := testReadOptions()
					opts.BatchRows = 3
					opts.BatchBytes = 4 << 20
					m := memory.NewBudgetManager(0)
					opts.MemoryManager = m
					if n, e := nestedReaderDrain(t, src, c, opts); e != nil || n != 17 || m.Used() != 0 {
						t.Fatal("decode", n, e, m.Used())
					}
				})
			}
		}
	}
}
func TestNativeNestedTwoListsEveryReserve(t *testing.T) {
	c := nestedReaderConfig{text: true, O: true, I: true, E: true, large: 65}
	path := nestedReaderWrite(t, c, []int{9})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	run := func(m *fixedTestFailManager) (int, error) {
		opts := testReadOptions()
		opts.BatchRows = 4
		opts.BatchBytes = 2 << 20
		opts.MemoryManager = m
		return nestedReaderDrain(t, src, c, opts)
	}
	probe := &fixedTestFailManager{Manager: memory.NewBudgetManager(0)}
	n, e := run(probe)
	if e != nil || n != 9 || probe.Used() != 0 {
		t.Fatal("trace", n, e)
	}
	t.Logf("successful actual Reserve calls=%d", probe.calls)
	for fail := 1; fail <= probe.calls; fail++ {
		m := &fixedTestFailManager{Manager: memory.NewBudgetManager(0), fail: fail}
		n, e := run(m)
		if m.calls < fail || m.Used() != 0 || (e != nil && !errors.Is(e, memory.ErrBudgetExceeded)) || (e == nil && n != 9) {
			t.Fatal("injected failure/recovery", fail, n, e, m.Used())
		}
	}
}
func TestNativeNestedTwoListsMalformedFirstRep(t *testing.T) {
	c := nestedReaderConfig{O: true, I: true, E: true, large: 3, bad: "first_rep"}
	path := nestedReaderWrite(t, c, []int{9})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		return
	}
	opts := testReadOptions()
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	_, e = nestedReaderDrain(t, src, c, opts)
	if e == nil || m.Used() != 0 {
		t.Fatal("malformed accepted/leaked", e, m.Used())
	}
}

type nestedReaderFaultManager struct {
	memory.Manager
	native         *nestedListStream
	candidate, hit bool
	count          int
	pending        []byte
}

func (m *nestedReaderFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		ns := m.native
		if !m.candidate && ns.current == 1 {
			c := ns.cursors[1]
			g := c.array
			if g != nil {
				slots, _, _ := g.stage.counts()
				out, _, _ := g.output.counts()
				if slots >= 8 && out > 0 && !c.complete {
					m.hit = true
					m.count = slots
					m.pending = c.bytes[0]
					return nil, memory.ErrBudgetExceeded
				}
			}
		}
		if m.candidate && ns.current == len(ns.cursors) && ns.cursors[0].replacementLease != nil {
			m.hit = true
			return nil, memory.ErrBudgetExceeded
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeNestedTwoListsPartialAndCandidate(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		t.Run(fmt.Sprint(candidate), func(t *testing.T) {
			c := nestedReaderConfig{text: true, O: true, I: true, E: true, multi: true, emptyLarge: true, large: 4097}
			path := nestedReaderWrite(t, c, []int{17}, pq.WithDataPageVersion(pq.DataPageV1))
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			m := &nestedReaderFaultManager{Manager: memory.NewBudgetManager(0), candidate: candidate}
			opts := testReadOptions()
			opts.BatchRows = 20
			opts.BatchBytes = 8 << 20
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			ns := st.(*nestedListStream)
			m.native = ns
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil || !ok || !m.hit || b.Rows == 17 {
				t.Fatal("recoverable prefix", ok, e, m.hit, b.Rows)
			}
			nestedReaderCheck(t, b, c, 0)
			seen := b.Rows
			if candidate {
				for _, cur := range ns.cursors {
					if cur.replacementLease != nil || cur.array.candidate.node != nil || cur.array.candidate.lease != nil {
						t.Fatal("replacement rollback")
					}
				}
			} else {
				cur := ns.cursors[1]
				slots, _, _ := cur.array.stage.counts()
				if slots != m.count || !ns.cursors[0].complete || !cur.tuple.present || !bytes.Equal(cur.bytes[0], m.pending) {
					t.Fatal("partial pending identity", slots, m.count, ns.cursors[0].complete, cur.tuple.present, cur.bytes[0], m.pending, ns.current)
				}
				if len(m.pending) > 0 && &cur.bytes[0][0] != &m.pending[0] {
					t.Fatal("borrowed tuple pointer changed")
				}
			}
			for {
				b = memory.NewBatch()
				ok, e = st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				nestedReaderCheck(t, b, c, seen)
				seen += b.Rows
			}
			st.Close()
			if seen != 17 || m.Used() != 0 {
				t.Fatal("resume count/release", seen, m.Used())
			}
		})
	}
}

type nestedReaderTransferContext struct {
	context.Context
	native *nestedListStream
	nodes  []*memory.ArrayNode
	leases []memory.Reservation
}

func (c *nestedReaderTransferContext) Err() error {
	if c.native == nil || len(c.nodes) > 0 {
		return c.Context.Err()
	}
	var pcs [32]uintptr
	fs := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	commit := false
	for {
		f, more := fs.Next()
		if strings.Contains(f.Function, "nestedListStream).commit") {
			commit = true
			break
		}
		if !more {
			break
		}
	}
	if !commit {
		return c.Context.Err()
	}
	for _, cur := range c.native.cursors {
		g := cur.array
		if g == nil || !cur.complete || g.stage.node == nil || g.stage.lease == nil {
			return c.Context.Err()
		}
		n, _, _ := g.stage.counts()
		o, _, _ := g.output.counts()
		if n == 0 || o != 0 {
			return c.Context.Err()
		}
	}
	for _, cur := range c.native.cursors {
		c.nodes = append(c.nodes, cur.array.stage.node)
		c.leases = append(c.leases, cur.array.stage.lease)
	}
	return c.Context.Err()
}
func TestNativeNestedTwoListsBundleTransfer(t *testing.T) {
	c := nestedReaderConfig{text: true, O: true, I: true, E: true, multi: true, large: 3}
	path := nestedReaderWrite(t, c, []int{8})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 1
	opts.MemoryManager = m
	ctx := &nestedReaderTransferContext{Context: context.Background()}
	st, e := src.NewBatchStream(ctx, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	ctx.native = st.(*nestedListStream)
	for row := 0; row < 8; row++ {
		b := memory.NewBatch()
		ok, e := st.Next(ctx, b)
		if e != nil || !ok {
			t.Fatal(ok, e)
		}
		nestedReaderCheck(t, b, c, row)
		if len(ctx.nodes) > 0 {
			for i, cur := range ctx.native.cursors {
				g := cur.array
				if g.output.node != ctx.nodes[i] || g.output.lease != ctx.leases[i] || g.vector.Child != ctx.nodes[i] || g.stage.node != nil || g.stage.lease != nil {
					t.Fatal("bundle transfer identity", i)
				}
			}
			break
		}
	}
	if len(ctx.nodes) != 2 {
		t.Fatal("no whole bundle transfer observed")
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("transfer leak")
	}
}

func nestedReaderArrowAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch b := b.(type) {
	case *array.ListBuilder:
		b.Append(true)
		for _, x := range v.([]any) {
			nestedReaderArrowAppend(b.ValueBuilder(), x)
		}
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.StringBuilder:
		b.Append(v.(string))
	default:
		panic("unexpected builder")
	}
}
func TestNativeNestedTwoListsIndependentArrowWriter(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprint(text), func(t *testing.T) {
			c := nestedReaderConfig{text: text, O: true, I: true, E: true, large: 65}
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			var dt arrow.DataType = arrow.PrimitiveTypes.Int64
			if text {
				dt = arrow.BinaryTypes.String
			}
			dt = arrow.ListOf(arrow.ListOf(dt))
			lb := array.NewListBuilder(alloc, dt.(*arrow.ListType).Elem())
			defer lb.Release()
			ib := array.NewInt64Builder(alloc)
			defer ib.Release()
			for row := 0; row < 17; row++ {
				ib.Append(int64(row))
				nestedReaderArrowAppend(lb, nestedReaderValue(c, row))
			}
			lists, ids := lb.NewArray(), ib.NewArray()
			defer lists.Release()
			defer ids.Release()
			sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "items", Type: dt, Nullable: true}}, nil)
			rec := array.NewRecordBatch(sc, []arrow.Array{ids, lists}, 17)
			defer rec.Release()
			path := filepath.Join(t.TempDir(), "arrow.parquet")
			f, e := os.Create(path)
			if e != nil {
				t.Fatal(e)
			}
			w, e := pqarrow.NewFileWriter(sc, f, pq.NewWriterProperties(), pqarrow.DefaultWriterProps())
			if e != nil {
				t.Fatal(e)
			}
			if e = w.Write(rec); e != nil {
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
			opts.BatchRows = 3
			opts.BatchBytes = 2 << 20
			if n, e := nestedReaderDrain(t, src, c, opts); e != nil || n != 17 {
				t.Fatal(n, e)
			}
		})
	}
}

type nestedReaderCancelContext struct {
	stageBytes, pendingBytes int
	onlyPendingCopy          bool
	context.Context
	native   *nestedListStream
	target   string
	checks   int
	charge   int64
	canceled bool
}

func (c *nestedReaderCancelContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	if c.native == nil {
		return c.Context.Err()
	}
	cur := c.native.cursors[0]
	if len(cur.bytes[0]) < 65536 {
		return c.Context.Err()
	}
	if c.onlyPendingCopy {
		_, _, n := cur.array.stage.counts()
		if n < len(cur.bytes[0]) {
			return c.Context.Err()
		}
	}

	var pcs [32]uintptr
	fs := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		f, more := fs.Next()
		if strings.HasSuffix(f.Function, c.target) {
			c.checks++
			if c.checks == 2 {
				c.canceled = true
				c.charge = c.native.base.opts.MemoryManager.Used()
				_, _, c.stageBytes = cur.array.stage.counts()
				c.pendingBytes = len(cur.bytes[0])
				return context.Canceled
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestNativeNestedTwoListsLongCopyCancellation(t *testing.T) {
	c := nestedReaderConfig{text: true, O: true, I: true, E: true, large: 3, longBytes: 132000}
	path := nestedReaderWrite(t, c, []int{8})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, target := range []string{".validateUTF8", ".copyBytes"} {
		t.Run(target, func(t *testing.T) {
			m := memory.NewBudgetManager(0)
			opts := testReadOptions()
			opts.BatchRows = 8
			opts.BatchBytes = 2 << 20
			opts.MemoryManager = m
			ctx := &nestedReaderCancelContext{Context: context.Background(), target: target, onlyPendingCopy: target == ".copyBytes"}
			st, e := src.NewBatchStream(ctx, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			ctx.native = st.(*nestedListStream)
			b := memory.NewBatch()
			_, e = st.Next(ctx, b)
			if !errors.Is(e, context.Canceled) || ctx.checks != 2 || ctx.charge <= 0 {
				t.Fatal("running cancellation", e, ctx.checks, ctx.charge)
			}
			st.Close()
			if m.Used() != 0 {
				t.Fatal("cancel leak")
			}
		})
	}
}

func TestNativeNestedTwoListsMetadataExactAndDefaultScope(t *testing.T) {
	for _, depth := range []int{0, 8} {
		for _, multi := range []bool{false, true} {
			c := nestedReaderConfig{O: true, I: true, E: true, depth: depth, multi: multi}
			path := nestedReaderWrite(t, c, []int{0})
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			extra, e := src.arrayMetadata(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			want := int64(16384)
			if multi {
				want *= 2
			}
			if extra != want {
				t.Fatal("adapter envelope", extra, want)
			}
			opts := testReadOptions()
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			charge := m.Used()
			st.Close()
			if charge < extra || m.Used() != 0 {
				t.Fatal("metadata charge", charge, extra)
			}
			for _, delta := range []int64{0, -1} {
				mm := memory.NewBudgetManager(charge + delta)
				opts.MemoryManager = mm
				st, e = src.NewBatchStream(context.Background(), opts)
				if delta == 0 {
					if e != nil {
						t.Fatal("exact", e)
					}
					st.Close()
				} else {
					if e == nil {
						st.Close()
						t.Fatal("-1 admitted")
					}
					if !errors.Is(e, memory.ErrBudgetExceeded) {
						t.Fatal(e)
					}
				}
				if mm.Used() != 0 {
					t.Fatal("metadata leak")
				}
			}
			for _, opt := range []Options{{}} {
				if _, e := OpenFileWithOptions(path, opt); e == nil {
					t.Fatal("default scope expanded", opt)
				}
			}
		}
	}
}
func TestNativeNestedTwoListsDeepProjection(t *testing.T) {
	c := nestedReaderConfig{text: true, O: true, I: true, E: true, multi: true, depth: 8, large: 65}
	path := nestedReaderWrite(t, c, []int{17, 19})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	scan, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id", "other.items.node0"}})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 5
	opts.BatchBytes = 2 << 20
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), scan, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	seen := 0
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if len(b.Structs) != 1 || b.Structs["other.items.node0"] == nil {
			t.Fatal("wrong remapped root")
		}
		cur := b
		for d := 0; d < 8; d++ {
			v := cur.Structs[fmt.Sprintf("other.items.node%d", d)]
			if v == nil || v.Rows != b.Rows {
				t.Fatal("deep layout")
			}
			cur = v.Values
		}
		for i := 0; i < b.Rows; i++ {
			row := seen + i + 1
			cc := c
			cc.text = !cc.text
			want := nestedReaderValue(cc, row)
			_, visible := nestedReaderDomain(c, row)
			if !visible {
				want = nil
			}
			if got := nestedReaderNodeValue(cur.Arrays["other.items"], i); !reflect.DeepEqual(got, want) {
				t.Fatal("remap oracle", row, got, want)
			}
		}
		seen += b.Rows
	}
	st.Close()
	if seen != 36 || m.Used() != 0 {
		t.Fatal("projection cleanup", seen, m.Used())
	}
}

func TestNativeNestedTwoListsFooterCounts(t *testing.T) {
	for _, delta := range []int64{-1, 1} {
		c := nestedReaderConfig{O: true, I: true, E: true, large: 3}
		path := nestedReaderWrite(t, c, []int{9})
		src, e := OpenFileWithOptions(path, Options{NativeNested: true})
		if e != nil {
			t.Fatal(e)
		}
		src.meta.RowGroups[0].Columns[1].MetaData.NumValues += delta
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
		var n [4]byte
		binary.LittleEndian.PutUint32(n[:], uint32(len(footer)))
		raw = append(raw, n[:]...)
		raw = append(raw, []byte("PAR1")...)
		if e = os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
		src, e = OpenFileWithOptions(path, Options{NativeNested: true})
		if e != nil {
			continue
		}
		m := memory.NewBudgetManager(0)
		opts := testReadOptions()
		opts.BatchRows = 20
		opts.MemoryManager = m
		_, e = nestedReaderDrain(t, src, c, opts)
		if e == nil || m.Used() != 0 {
			t.Fatal("footer count admitted/leaked", delta, e, m.Used())
		}
	}
}
func TestNativeNestedTwoListsOversizeBeforeCopy(t *testing.T) {
	c := nestedReaderConfig{text: true, O: true, I: true, E: true, large: 1, longBytes: 300000}
	path := nestedReaderWrite(t, c, []int{8})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 8
	opts.BatchBytes = 128 << 10
	opts.MemoryManager = m
	ctx := &nestedReaderCancelContext{Context: context.Background(), target: ".copyBytes", onlyPendingCopy: true}
	st, e := src.NewBatchStream(ctx, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	ctx.native = st.(*nestedListStream)
	for {
		b := memory.NewBatch()
		ok, err := st.Next(ctx, b)
		if err != nil {
			e = err
			break
		}
		if !ok {
			t.Fatal("oversize unexpectedly fit")
		}
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) || ctx.checks != 0 {
		t.Fatal("oversize copied before rejection", e, ctx.checks, ctx.stageBytes, ctx.pendingBytes, ctx.charge)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("oversize cleanup")
	}
}

func TestNativeNestedTwoListsEmptyR1ThenStringGrowth(t *testing.T) {
	c := nestedReaderConfig{text: true, O: true, I: true, E: true, large: 4097, emptyLarge: true, longBytes: 132000}
	path := nestedReaderWrite(t, c, []int{8}, pq.WithDataPageVersion(pq.DataPageV1))
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 1
	opts.BatchBytes = 230 << 10
	opts.MemoryManager = m
	n, e := nestedReaderDrain(t, src, c, opts)
	if e != nil || n != 8 || m.Used() != 0 {
		t.Fatal("independent R1 empty then R2 bytes", n, e, m.Used())
	}
}

func TestNativeNestedTwoListsRejectInnerPlaceholderContinuation(t *testing.T) {
	for _, text := range []bool{false, true} {
		for _, bad := range []string{"null_inner_rep2", "empty_inner_rep2"} {
			t.Run(fmt.Sprint(text, bad), func(t *testing.T) {
				c := nestedReaderConfig{text: text, O: true, I: true, E: true, large: 3, bad: bad}
				path := nestedReaderWrite(t, c, []int{9}, pq.WithDataPageVersion(pq.DataPageV1))
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal("fixture must reach decoder", e)
				}
				opts := testReadOptions()
				opts.BatchRows = 20
				opts.BatchBytes = 2 << 20
				m := memory.NewBudgetManager(0)
				opts.MemoryManager = m
				st, e := src.NewBatchStream(context.Background(), opts)
				if e != nil {
					t.Fatal(e)
				}
				defer st.Close()
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e == nil || ok || !strings.Contains(e.Error(), "array rep2 after inner placeholder") {
					t.Fatal("expected precise placeholder-state rejection", ok, e)
				}
				st.Close()
				if m.Used() != 0 {
					t.Fatal("invalid continuation leaked", m.Used())
				}
			})
		}
	}
}
