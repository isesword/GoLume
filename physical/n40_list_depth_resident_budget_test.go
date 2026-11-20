package physical

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
)

type n40Source struct {
	schema         memory.Schema
	text           bool
	opened, closed int
	live           int64
	data           *memory.Batch
}

func n40Input(text bool) *n40Source {
	leaf := memory.Field{Name: "item", Type: memory.TypeInt64, Nullable: true, Encoding: memory.EncodingView}
	if text {
		leaf.Type = memory.TypeString
	}
	f := leaf
	for i := 0; i < 3; i++ {
		f = memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{f}}
	}
	f.Name = "items"
	return &n40Source{schema: memory.Schema{Fields: []memory.Field{{Name: "key", Type: memory.TypeInt64}, {Name: "id", Type: memory.TypeInt64}, f}}, text: text}
}
func (s *n40Source) Schema() memory.Schema { return s.schema }
func (s *n40Source) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: 8, Partitions: 1}, nil
}
func (s *n40Source) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: 8}}, nil
}
func (s *n40Source) NewBatchStream(_ context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opened++
	return &n40Stream{s: s, m: o.MemoryManager}, nil
}

type n40Stream struct {
	s      *n40Source
	m      memory.Manager
	lease  memory.Reservation
	page   int
	closed bool
}

func n40Leaf(text bool, id int64) any {
	if text {
		return fmt.Sprintf("%03d:", id) + strings.Repeat("z", 508)
	}
	return id*17 - 9
}
func (s *n40Stream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if s.page == 2 {
		return false, nil
	}
	if s.lease == nil {
		// Exact fixed payload capacities plus conservative batch/vector/field descriptors.
		charge := int64(16384 + 64 + 3*20 + 4*8 + 32)
		if s.s.text {
			charge = 16384 + 64 + 3*20 + 4*8 + 20 + 4*512
		}
		var e error
		s.lease, e = s.m.Reserve(ctx, charge)
		if e != nil {
			return false, e
		}
		s.s.live = charge
		b := memory.NewBatch()
		b.Rows = 4
		b.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1, 0, 1}}
		b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 4)}
		leaf := &memory.ArrayNode{Kind: memory.TypeInt64, Encoding: memory.EncodingView, Int64: &memory.Int64Vector{Values: make([]int64, 4), Validity: memory.Bitmap{9}, Encoding: memory.EncodingView}}
		if s.s.text {
			leaf = &memory.ArrayNode{Kind: memory.TypeString, Encoding: memory.EncodingView, Strings: &memory.StringViewVector{Offsets: []uint32{0, 512, 1024, 1536, 2048}, Data: make([]byte, 2048), Validity: memory.Bitmap{9}, Encoding: memory.EncodingView}}
		}
		for i := 0; i < 2; i++ {
			leaf = &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 2, 3, 4}, Validity: memory.Bitmap{15}, Child: leaf, Encoding: memory.EncodingView}
		}
		b.Arrays["items"] = &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 1, 2, 4}, Validity: memory.Bitmap{11}, Child: leaf, Encoding: memory.EncodingView}
		s.s.data = b
	}
	b := s.s.data
	leaf := b.Arrays["items"].Child.Child.Child
	// Same backing arrays are overwritten on every page: earlier append cannot borrow them.
	for i := 0; i < 4; i++ {
		id := int64(s.page*4 + i)
		b.Int64s["id"].Values[i] = id
		if s.s.text {
			copy(leaf.Strings.Data[i*512:(i+1)*512], n40Leaf(true, id).(string))
		} else {
			leaf.Int64.Values[i] = n40Leaf(false, id).(int64)
		}
	}
	s.page++
	dst.Rows = 4
	dst.Int64s["key"] = b.Int64s["key"]
	dst.Int64s["id"] = b.Int64s["id"]
	dst.Arrays["items"] = b.Arrays["items"]
	return true, nil
}
func (s *n40Stream) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closed++
		if s.s.data != nil {
			v := s.s.data.Arrays["items"].Child.Child.Child
			if v.Int64 != nil {
				for i := range v.Int64.Values {
					v.Int64.Values[i] = -777
				}
			} else {
				for i := range v.Strings.Data {
					v.Strings.Data[i] = '!'
				}
			}
		}
		if s.lease != nil {
			s.lease.Release()
			s.lease = nil
		}
		s.s.live = 0
	}
	return nil
}
func n40Read(v *memory.ArrayNode, r int) any {
	if !v.IsValid(r) {
		return nil
	}
	if v.Kind == memory.TypeInt64 {
		return v.Int64.Values[r]
	}
	if v.Kind == memory.TypeString {
		return string(v.Strings.ValueBytes(r))
	}
	out := []any{}
	for i := v.Offsets[r]; i < v.Offsets[r+1]; i++ {
		out = append(out, n40Read(v.Child, int(i)))
	}
	return out
}
func n40Want(text bool, id int64) any {
	switch id % 4 {
	case 0:
		return []any{[]any{[]any{n40Leaf(text, id)}}}
	case 1:
		return []any{}
	case 2:
		return nil
	default:
		return []any{[]any{[]any{nil}}, []any{[]any{n40Leaf(text, id)}}}
	}
}

type n40Probe struct {
	releases, releaseAt int
	memory.Manager
	src                   *n40Source
	calls, fail, cancelAt int
	cancel                context.CancelFunc
	peak, sourceOverlap   int64
	trace                 []string
}

func (m *n40Probe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	before := m.Used()
	if m.fail == m.calls {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	m.trace = append(m.trace, fmt.Sprintf("%d:used%d+new%d source%d after%d", m.calls, before, n, m.src.live, m.Used()))
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	if m.src.live > 0 && before >= m.src.live && m.Used() > m.sourceOverlap {
		m.sourceOverlap = m.Used()
	}
	if e == nil && m.cancelAt == m.calls {
		m.cancel()
	}
	if e == nil {
		return &n40Lease{Reservation: r, m: m}, nil
	}
	return r, e
}
func n40Run(t *testing.T, text bool, limit int64, fail, cancelAt int, early bool, want error) (int64, int) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	src := n40Input(text)
	m := &n40Probe{Manager: memory.NewBudgetManager(limit), src: src, fail: fail, cancelAt: cancelAt, cancel: stop}
	op, e := NewSortSource(src, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(op.Schema(), src.Schema()) {
		t.Fatal("schema changed")
	}
	opts := n27Opts(m)
	opts.BatchRows = 4
	st, e := source.OpenBatchStream(ctx, op, opts)
	rows := 0
	var held *memory.OwnedNestedList
	var heldWant []any
	if e == nil {
		if _, ok := st.(*residentSortStream); !ok {
			t.Fatalf("not resident: %T", st)
		}
		b := memory.NewBatch()
		order := []int64{2, 6, 1, 3, 5, 7, 0, 4}
		for {
			var ok bool
			ok, e = st.Next(ctx, b)
			if e != nil || !ok {
				break
			}
			for i := 0; i < b.Rows; i++ {
				id := order[rows+i]
				if b.Int64s["id"].Values[i] != id || !reflect.DeepEqual(n40Read(b.Arrays["items"], i), n40Want(text, id)) {
					t.Fatalf("independent stable oracle row%d", rows+i)
				}
			}
			if held != nil {
				for i, w := range heldWant {
					if !reflect.DeepEqual(n40Read(held.Vector, i), w) {
						t.Fatal("owned clone poisoned by next output")
					}
				}
				held.Close()
				held = nil
			}
			f, _ := op.Schema().Field("items")
			before := m.Used()
			if want == nil {
				rs := st.(*residentSortStream)
				a := rs.output
				t.Logf("published owner metadata=%d payloadCapacity=%d ownerToken=%d borrowedView=%d logicalWorkspaceUsed=%d retainedCreditsCapacity=%d parentUsed=%d", a.metadataBytes, a.capacityBytes, a.reservation.Bytes(), a.viewLease.Bytes(), rs.workspace.used, rs.workspace.capacity, before)
			}
			held, e = memory.CloneNestedListRangeOwned(ctx, m, b.Arrays["items"], f, memory.NestedListParentDomain{}, 0, b.Rows)
			if e != nil {
				break
			}
			if m.Used() < before+held.Bytes {
				t.Fatal("output and retained clone not jointly charged")
			}
			heldWant = nil
			for i := 0; i < b.Rows; i++ {
				heldWant = append(heldWant, n40Want(text, order[rows+i]))
			}
			rows += b.Rows
			if early {
				break
			}
		}
	}
	if want != nil {
		if !errors.Is(e, want) {
			t.Fatalf("expected %v got %v calls%d", want, e, m.calls)
		}
	} else if e != nil {
		t.Fatal(e)
	} else if !early && rows != 8 {
		t.Fatal("row count", rows)
	}
	m.fail = 0
	m.cancelAt = 0
	if st != nil {
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if held != nil {
		for i, w := range heldWant {
			if !reflect.DeepEqual(n40Read(held.Vector, i), w) {
				t.Fatal("owner invalidated by stream close")
			}
		}
		if m.Used() != held.Bytes {
			t.Fatalf("remaining nonowner charge%d owner%d", m.Used(), held.Bytes)
		}
		held.Close()
		held.Close()
	}
	if m.Used() != 0 || src.opened != src.closed {
		t.Fatalf("cleanup used%d opens%d closes%d", m.Used(), src.opened, src.closed)
	}
	if want == nil {
		t.Logf("text=%v rows8 BatchRows4 sourceRows4 peak%d sourceOverlap%d calls%d trace%v", text, m.peak, m.sourceOverlap, m.calls, m.trace)
		if m.sourceOverlap <= 16384 {
			t.Fatal("missing full source overlap")
		}
	}
	return m.peak, m.calls
}
func TestN40ListDepthResidentBudget(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			peak, calls := n40Run(t, text, 8<<20, 0, 0, false, nil)
			t.Run("exact", func(t *testing.T) { n40Run(t, text, peak, 0, 0, false, nil) })
			t.Run("minusone", func(t *testing.T) { n40Run(t, text, peak-1, 0, 0, false, memory.ErrBudgetExceeded) })
			t.Run("early", func(t *testing.T) { n40Run(t, text, 8<<20, 0, 0, true, nil) })
			for at := 1; at <= calls; at++ {
				t.Run(fmt.Sprintf("fail%d", at), func(t *testing.T) { n40Run(t, text, 8<<20, at, 0, false, memory.ErrBudgetExceeded) })
				t.Run(fmt.Sprintf("cancel%d", at), func(t *testing.T) { n40Run(t, text, 8<<20, 0, at, false, context.Canceled) })
			}
		})
	}
}

func TestN40ListDepthResidentDescriptorAudit(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			ctx := context.Background()
			m := memory.NewBudgetManager(1 << 20)
			src := n40Input(text)
			st, e := src.NewBatchStream(ctx, source.ReadOptions{MemoryManager: m})
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			b := memory.NewBatch()
			if ok, e := st.Next(ctx, b); !ok || e != nil {
				t.Fatal(e)
			}
			deep, _, e := residentExtendedMetadata(ctx, src.schema.Fields, b, 0)
			if e != nil {
				t.Fatal(e)
			}
			// Compare the same runtime graph with exactly one List axis removed.
			shallow := memory.NewBatch()
			shallow.Rows = b.Rows
			shallow.Int64s = b.Int64s
			root := *b.Arrays["items"]
			root.Child = root.Child.Child
			shallow.Arrays["items"] = &root
			fields := append([]memory.Field(nil), src.schema.Fields...)
			f := fields[2]
			f.Children = append([]memory.Field(nil), f.Children[0].Children...)
			fields[2] = f
			low, _, e := residentExtendedMetadata(ctx, fields, shallow, 0)
			if e != nil {
				t.Fatal(e)
			}
			if deep-low < 1024 {
				t.Fatalf("extra retained node uncharged deep%d shallow%d", deep, low)
			}
			// The range starts after nonzero offsets and at misaligned validity bits.
			all, e := memory.BorrowedRangeMetadataBytesWithCheck(b, 3, 1, ctx.Err)
			if e != nil {
				t.Fatal(e)
			}
			scalar := memory.NewBatch()
			scalar.Rows = b.Rows
			scalar.Int64s = b.Int64s
			base, e := memory.BorrowedRangeMetadataBytesWithCheck(scalar, 3, 1, ctx.Err)
			if e != nil {
				t.Fatal(e)
			}
			// Four node allowances; root/middle/inner offsets 8+12+12; four bitmaps 32.
			expected := int64(4*1024 + 8 + 12 + 12 + 32)
			if text {
				expected += 12
			}
			if all-base != expected {
				t.Fatalf("borrow recursive charge got%d expected%d", all-base, expected)
			}
			headers, e := residentExtendedHeaders(ctx, b, 0)
			if e != nil {
				t.Fatal(e)
			}
			t.Logf("retained metadata depth3=%d depth2=%d borrowedRange=%d arrayRange=%d additionalHeaders=%d", deep, low, all, all-base, headers)
		})
	}
}

// Release hooks observe real parent leases, including deferred input/permutation
// cleanup before prepare returns. Cancellation must not publish a successful page.
type n40Lease struct {
	memory.Reservation
	m    *n40Probe
	done bool
}

func (r *n40Lease) Release() {
	if !r.done {
		r.done = true
		r.Reservation.Release()
		r.m.releases++
		if r.m.releaseAt == r.m.releases {
			r.m.cancel()
		}
	}
}

type n40Checks struct {
	context.Context
	checks, at int
	ready      func() bool
}

func (c *n40Checks) Err() error {
	c.checks++
	if c.ready != nil && c.ready() {
		return context.Canceled
	}
	if c.at > 0 && c.checks >= c.at {
		return context.Canceled
	}
	return c.Context.Err()
}
func n40Publication(t *testing.T, releaseAt, checkAt int) (int, int) {
	t.Helper()
	base, stop := context.WithCancel(context.Background())
	defer stop()
	ctx := &n40Checks{Context: base, at: checkAt}
	src := n40Input(true)
	m := &n40Probe{Manager: memory.NewBudgetManager(8 << 20), src: src, cancel: stop, releaseAt: releaseAt}
	op, e := NewSortSource(src, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	st, e := source.OpenBatchStream(ctx, op, n27Opts(m))
	if e != nil {
		t.Fatal(e)
	}
	if checkAt == -1 {
		rs := st.(*residentSortStream)
		ctx.ready = func() bool { return rs.prepared }
	}
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	releases, checks := m.releases, ctx.checks
	if releaseAt > 0 || checkAt != 0 {
		if ok || !errors.Is(e, context.Canceled) || len(b.Arrays) != 0 || len(b.Int64s) != 0 {
			t.Fatalf("cancel publication ok=%v error=%v arrays%d scalars%d release%d checks%d", ok, e, len(b.Arrays), len(b.Int64s), releases, checks)
		}
	} else if !ok || e != nil {
		t.Fatal("baseline", ok, e)
	}
	m.releaseAt = 0
	ctx.ready = nil
	ctx.at = 0
	if e := st.Close(); e != nil {
		t.Fatal(e)
	}
	if e := st.Close(); e != nil {
		t.Fatal(e)
	}
	if m.Used() != 0 || src.opened != src.closed {
		t.Fatal("publication cleanup")
	}
	return releases, checks
}
func TestN40ListDepthResidentPublicationCancel(t *testing.T) {
	t.Run("after_prepare_deferred_cleanup", func(t *testing.T) { n40Publication(t, 0, -1) })
	releases, checks := n40Publication(t, 0, 0)
	t.Logf("before first publication releasepoints=%d contextchecks=%d", releases, checks)
	for at := 1; at <= releases; at++ {
		t.Run(fmt.Sprintf("release%d", at), func(t *testing.T) { n40Publication(t, at, 0) })
	}
	for at := max(1, checks-31); at <= checks; at++ {
		t.Run(fmt.Sprintf("finalcheck%d", at), func(t *testing.T) { n40Publication(t, 0, at) })
	}
}
