package lazy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/physical"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
)

// Observed sources make replay routing an assertion, not an inferred optimizer choice.
type nestedTopKObserved struct {
	source.Source
	stable               bool
	forbidden            bool
	batchRows            int
	opens, nexts, closes int
}

func (s *nestedTopKObserved) Replayability() source.Replayability {
	if s.stable {
		return source.ReplayStable
	}
	return source.ReplayUnknown
}
func (s *nestedTopKObserved) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: -1}, nil
}
func (s *nestedTopKObserved) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.forbidden || (!s.stable && s.opens > 1) {
		return nil, errors.New("forbidden TopK source open")
	}
	o.BatchRows = s.batchRows
	st, e := source.OpenBatchStream(ctx, s.Source, o)
	if e != nil {
		return nil, e
	}
	return &nestedTopKObservedStream{BatchStream: st, owner: s}, nil
}

type nestedTopKObservedStream struct {
	source.BatchStream
	owner  *nestedTopKObserved
	closed bool
}

func (s *nestedTopKObservedStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.owner.nexts++
	return s.BatchStream.Next(ctx, b)
}
func (s *nestedTopKObservedStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.owner.closes++
	return s.BatchStream.Close()
}
func nestedTopKSources(t *testing.T, stable bool) (*nestedTopKObserved, *nestedTopKObserved, func()) {
	t.Helper()
	var full, keys []arrow.RecordBatch
	for part := 0; part < 3; part++ {
		r := residentNestedRecord(t, part)
		full = append(full, r)
		last := r.NumCols() - 1
		keys = append(keys, array.NewRecordBatch(arrow.NewSchema([]arrow.Field{r.Schema().Field(int(last))}, nil), []arrow.Array{r.Column(int(last))}, r.NumRows()))
	}
	payload, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeNested: true}, full...)
	if e != nil {
		t.Fatal(e)
	}
	key, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{}, keys...)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range full {
		r.Release()
	}
	for _, r := range keys {
		r.Release()
	}
	return &nestedTopKObserved{Source: payload, stable: stable, batchRows: 13}, &nestedTopKObserved{Source: key, stable: true, forbidden: !stable, batchRows: 7}, func() { payload.Close(); key.Close() }
}
func TestNestedTopKReplayAndVolatile(t *testing.T) {
	for _, stable := range []bool{true, false} {
		for _, k := range []int{0, 1, 17, 1000000} {
			t.Run(fmt.Sprintf("stable%v/k%d", stable, k), func(t *testing.T) {
				payload, key, closeSource := nestedTopKSources(t, stable)
				defer closeSource()
				opts := engine.DefaultOptions()
				opts.MemoryLimitBytes = 64 << 20
				opts.PreparedCacheBytes = 0
				s := NewSession(opts)
				defer s.Close()
				top, e := physical.NewTopKSourceWithKeyInput(payload, key, k, []physical.SortKey{{Column: "key", NullsLast: true}}, nil)
				if e != nil {
					t.Fatal(e)
				}
				q := s.FromSource(top)
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer df.Close()
				n := k
				if n > 195 {
					n = 195
				}
				want := residentNestedOrder(false)[:n]
				residentNestedCheck(t, df, want)
				if stable {
					if key.opens > 1 || payload.opens > 1 || (k > 0 && (key.opens != 1 || payload.opens != 1)) {
						t.Fatal("replay routing", key.opens, payload.opens)
					}
				} else {
					if payload.opens > 1 || (k > 0 && payload.opens != 1) || key.opens != 0 {
						t.Fatal("volatile routing", payload.opens, key.opens)
					}
				}
				if k == 0 && (payload.nexts != 0 || key.nexts != 0) {
					t.Fatal("K0 consumed rows", payload.nexts, key.nexts)
				}
				if payload.opens != payload.closes || key.opens != key.closes {
					t.Fatal("source close counts", payload.opens, payload.closes, key.opens, key.closes)
				}
				closeSource()
				s.Close()
				residentNestedCheck(t, df, want)
				df.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("TopK retained owner")
				}
			})
		}
	}
}
func TestNestedTopKTransportAndBorrow(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	q := residentNestedQuery(t, s).TopK(17, Desc("key"))
	want := residentNestedOrder(true)[:17]
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	residentNestedCheck(t, df, want)
	rd, e := q.CollectBatches(context.Background(), BatchOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if !rd.Next(context.Background()) {
		t.Fatal(rd.Err())
	}
	view := rd.Batch()
	rd.Close()
	if view.Valid() {
		t.Fatal("TopK borrow survives Close")
	}
	ad := structNestedIPC(t, s, q, filepath.Join(t.TempDir(), "top.arrow"))
	defer ad.Close()
	residentNestedCheck(t, ad, want)
	path := filepath.Join(t.TempDir(), "top.parquet")
	if _, e = q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 3}); e != nil {
		t.Fatal(e)
	}
	pq, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	pd, e := pq.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer pd.Close()
	residentNestedCheck(t, pd, want)
	s.Close()
	residentNestedCheck(t, df, want)
	residentNestedCheck(t, ad, want)
	residentNestedCheck(t, pd, want)
	df.Close()
	ad.Close()
	pd.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("TopK transport leak")
	}
}
func TestNestedTopKBudgetAndCancel(t *testing.T) {
	for _, stable := range []bool{true, false} {
		t.Run(fmt.Sprint(stable), func(t *testing.T) {
			p, k, closeSource := nestedTopKSources(t, stable)
			defer closeSource()
			opts := engine.DefaultOptions()
			opts.MemoryLimitBytes = 1
			s := NewSession(opts)
			defer s.Close()
			top, e := physical.NewTopKSourceWithKeyInput(p, k, 17, []physical.SortKey{{Column: "key", NullsLast: true}}, nil)
			if e != nil {
				t.Fatal(e)
			}
			df, e := s.FromSource(top).Collect(context.Background())
			if e == nil {
				df.Close()
			}
			if !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("TopK budget identity", e)
			}
			s.Close()
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("failed TopK leaked")
			}
		})
	}
	p, k, closeSource := nestedTopKSources(t, false)
	defer closeSource()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	top, e := physical.NewTopKSourceWithKeyInput(p, k, 3, []physical.SortKey{{Column: "key"}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	df, e := s.FromSource(top).Collect(ctx)
	if e == nil {
		df.Close()
	}
	if !errors.Is(e, context.Canceled) {
		t.Fatal("TopK cancel identity", e)
	}
}

func TestNestedTopKBinaryOpaqueAndDecimalBoundary(t *testing.T) {
	base := residentNestedRecord(t, 0)
	defer base.Release()
	bin := array.NewBinaryBuilder(am.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer bin.Release()
	opaque := array.NewLargeStringBuilder(am.DefaultAllocator)
	defer opaque.Release()
	decType := &arrow.Decimal128Type{Precision: 9, Scale: 2}
	dec := array.NewDecimal128Builder(am.DefaultAllocator, decType)
	defer dec.Release()
	for r := 0; r < 65; r++ {
		bin.Append([]byte{byte(r), 0, 255})
		opaque.Append(fmt.Sprintf("opaque-%d-中", r))
		dec.Append(decimal128.FromI64(int64(r)))
	}
	ba, oa, da := bin.NewArray(), opaque.NewArray(), dec.NewArray()
	defer ba.Release()
	defer oa.Release()
	defer da.Release()
	fields := append([]arrow.Field(nil), base.Schema().Fields()...)
	fields = append(fields, arrow.Field{Name: "binary", Type: arrow.BinaryTypes.Binary}, arrow.Field{Name: "opaque", Type: arrow.BinaryTypes.LargeString})
	cols := append([]arrow.Array(nil), base.Columns()...)
	cols = append(cols, ba, oa)
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 65)
	defer rec.Release()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	s := NewSession(opts)
	defer s.Close()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	sorted := q.TopK(17, Desc("seq"))
	df, e := sorted.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	c, e := df.Column("ints")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	for i, v := range got {
		if !reflect.DeepEqual(v, nestedListBRaw(64-i, false)) {
			t.Fatal("opaque mixed native payload", i)
		}
	}
	checkOpaque := func() {
		oc, err := df.Column("opaque")
		if err != nil {
			t.Fatal(err)
		}
		oc.state.mu.RLock()
		defer oc.state.mu.RUnlock()
		spans, err := oc.lockedSpans()
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for _, span := range spans {
			v := span.batch.Opaque["opaque"]
			if v == nil {
				t.Fatal("missing retained opaque")
			}
			arr, ok := v.Value.(*array.LargeString)
			if !ok {
				t.Fatalf("opaque type %T", v.Value)
			}
			for row := span.start; row < span.end; row++ {
				if got := arr.Value(row); got != fmt.Sprintf("opaque-%d-中", 64-seen) {
					t.Fatal("opaque order", seen, got)
				}
				seen++
			}
		}
		if seen != 17 {
			t.Fatal("opaque row count", seen)
		}
	}
	checkOpaque()
	// Generic IPC export never admitted TypeArrow without a dedicated adapter.
	path := filepath.Join(t.TempDir(), "mixed.arrow")
	usedBeforeRejectedIPC := s.Engine().Memory().Used()
	if _, e = sorted.SinkIPC(context.Background(), path, IPCSinkOptions{}); e == nil || !strings.Contains(e.Error(), "explicit Arrow type adapter") {
		t.Fatal("opaque IPC boundary changed", e)
	}
	if s.Engine().Memory().Used() != usedBeforeRejectedIPC {
		t.Fatal("opaque IPC denial retained memory", usedBeforeRejectedIPC, s.Engine().Memory().Used())
	}
	if _, e = sorted.Select("binary", "seq").SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	file, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer file.Close()
	rd, e := ipc.NewFileReader(file)
	if e != nil {
		t.Fatal(e)
	}
	defer rd.Close()
	seen := 0
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		for j := 0; j < int(r.NumRows()); j++ {
			want := 64 - seen
			b := r.Column(0).(*array.Binary).Value(j)
			ordinal := r.Column(1).(*array.Int64).Value(j)
			if !reflect.DeepEqual(b, []byte{byte(want), 0, 255}) || ordinal != int64(want) {
				t.Fatal("binary IPC order", seen, b, ordinal)
			}
			seen++
		}
		r.Release()
	}
	if seen != 17 {
		t.Fatal("binary IPC rows", seen)
	}
	decimalRec := array.NewRecordBatch(arrow.NewSchema(append(fields, arrow.Field{Name: "decimal", Type: decType}), nil), append(cols, da), 65)
	defer decimalRec.Release()
	dq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, decimalRec)
	if e != nil {
		t.Fatal(e)
	}
	dd, e := dq.Select("decimal").Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	dc, e := dd.Column("decimal")
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := dc.Decimal128Chunks()
	if e != nil || len(chunks) == 0 {
		t.Fatal(e)
	}
	v, valid, e := chunks[0].Value(0)
	if e != nil || !valid || v != decimal128.FromI64(0) {
		t.Fatal("D1 changed", v, valid, e)
	}
	dd.Close()
	decimalTop, e := dq.TopK(17, Asc("seq")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	seqCol, e := decimalTop.Column("seq")
	if e != nil {
		t.Fatal(e)
	}
	seqValues, e := seqCol.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	expectedSeq := make([]any, 17)
	for i := range expectedSeq {
		expectedSeq[i] = int64(i)
	}
	if !reflect.DeepEqual(seqValues, expectedSeq) {
		t.Fatal("Decimal TopK stable ordinal", seqValues)
	}
	decimalCol, e := decimalTop.Column("decimal")
	if e != nil {
		t.Fatal(e)
	}
	decimalChunks, e := decimalCol.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	decimalRows := 0
	for _, ch := range decimalChunks {
		if ch.Precision() != decType.Precision || ch.Scale() != decType.Scale {
			t.Fatal("Decimal TopK metadata")
		}
		for i := 0; i < ch.Len(); i++ {
			value, valid, e := ch.Value(i)
			if e != nil || !valid || value != decimal128.FromI64(int64(decimalRows)) {
				t.Fatal("Decimal TopK coefficient", decimalRows, value, valid, e)
			}
			decimalRows++
		}
	}
	if decimalRows != 17 {
		t.Fatal("Decimal TopK rows", decimalRows)
	}
	decimalTop.Close()

	s.Close()
	checkOpaque()
	again, e := c.ToAnySlice()
	if e != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("mixed source-close ownership", e)
	}
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("mixed owner leak")
	}
}

// Diagnostic separation of TopK-owned candidates from downstream owned Collect.
// Same 195-row maximum-depth fixture and 64MiB limit as the failing full Collect.
type nestedTopKPeakManager struct {
	memory.Manager
	peak int64
}

func (m *nestedTopKPeakManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, e := m.Manager.Reserve(ctx, n)
	if u := m.Used(); u > m.peak {
		m.peak = u
	}
	return r, e
}
func TestNestedTopKCandidateAccounting195(t *testing.T) {
	p, k, closeSource := nestedTopKSources(t, false)
	defer closeSource()
	m := &nestedTopKPeakManager{Manager: memory.NewBudgetManager(64 << 20)}
	top, e := physical.NewTopKSourceWithKeyInput(p, k, 1000000, []physical.SortKey{{Column: "key", NullsLast: true}}, m)
	if e != nil {
		t.Fatal(e)
	}
	st, e := source.OpenBatchStream(context.Background(), top, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: m})
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	dst := memory.NewBatch()
	seen, pages := 0, 0
	order := residentNestedOrder(false)
	var first, previous int64
	for {
		ok, e := st.Next(context.Background(), dst)
		if e != nil {
			st.Close()
			t.Fatalf("candidate-only seen=%d used=%d peak=%d err=%v", seen, m.Used(), m.peak, e)
		}
		if !ok {
			break
		}
		if dst.Rows <= 0 || dst.Rows > 64 || seen+dst.Rows > len(order) {
			t.Fatal("snapshot output page rows", seen, dst.Rows)
		}
		for row := 0; row < dst.Rows; row++ {
			if got := dst.Int64s["seq"].Values[row]; got != int64(order[seen+row]) {
				t.Fatalf("page stable ordinal row=%d got=%d want=%d", seen+row, got, order[seen+row])
			}
		}
		u := m.Used()
		if seen == 0 {
			first = u
		} else if u >= previous {
			t.Fatalf("candidate charge did not fall after emission row=%d previous=%d used=%d", seen, previous, u)
		}
		previous = u
		seen += dst.Rows
		pages++
	}
	if e := st.Close(); e != nil {
		t.Fatal(e)
	}
	t.Logf("candidate-only rows=%d pages=%d first-retained=%d peak=%d final=%d", seen, pages, first, m.peak, m.Used())
	if seen != 195 || pages != 4 || m.Used() != 0 || p.opens != 1 || k.opens != 0 {
		t.Fatal("candidate accounting/routing", seen, m.Used(), p.opens, k.opens)
	}
}

// Higher-limit diagnosis only; the original 64MiB positive remains unchanged.
func TestNestedTopKCollectAccountingDiagnostic128(t *testing.T) {
	var peak195, peak256 int64
	for _, count := range []int{195, 256, 1000000} {
		p, k, closeSource := nestedTopKSources(t, false)
		opts := engine.DefaultOptions()
		opts.MemoryLimitBytes = 128 << 20
		opts.PreparedCacheBytes = 0
		s := NewSession(opts)
		top, e := physical.NewTopKSourceWithKeyInput(p, k, count, []physical.SortKey{{Column: "key", NullsLast: true}}, nil)
		if e != nil {
			t.Fatal(e)
		}
		budget, e := s.Engine().BeginQueryMemory()
		if e != nil {
			t.Fatal(e)
		}
		df, e := s.FromSource(top).Collect(memory.WithQueryBudget(context.Background(), budget))
		snap := budget.Snapshot()
		if e != nil {
			budget.Close()
			s.Close()
			closeSource()
			t.Fatalf("diagnostic k=%d snapshot=%+v error=%v", count, snap, e)
		}
		residentNestedCheck(t, df, residentNestedOrder(false))
		t.Logf("diagnostic128 k=%d query-snapshot=%+v engine-retained=%d", count, snap, s.Engine().Memory().Used())
		switch count {
		case 195:
			peak195 = snap.PeakBytes
		case 256:
			peak256 = snap.PeakBytes
			if peak195 > peak256 {
				t.Fatalf("smaller index cap increased peak: k195=%d k256=%d", peak195, peak256)
			}
			t.Logf("dynamic index-cap delta: k195=%d k256=%d delta=%d", peak195, peak256, peak256-peak195)
		case 1000000:
			if peak256 != snap.PeakBytes {
				t.Fatalf("same actual dynamic cap changed peak: k256=%d k1e6=%d", peak256, snap.PeakBytes)
			}
		}
		budget.Close()
		s.Close()
		closeSource()
		df.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("diagnostic final retained", s.Engine().Memory().Used())
		}
	}
}
