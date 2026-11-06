package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Capturing only query budgets (not input batches) lets the tests independently
// verify every finished execution lost ownership while the root keeps idle bytes.
type reuseTestSource struct {
	*generatedStreamSource
	mu      sync.Mutex
	queries []*memory.QueryBudget
}

func (s *reuseTestSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.mu.Lock()
	s.queries = append(s.queries, memory.QueryBudgetFromContext(ctx))
	s.mu.Unlock()
	return s.generatedStreamSource.NewBatchStream(ctx, o)
}
func (s *reuseTestSource) NewBatchStreamWithPartitions(ctx context.Context, o source.ReadOptions, _ []source.Partition) (source.BatchStream, error) {
	return s.NewBatchStream(ctx, o)
}
func reuseTestOptions() Options {
	o := streamTestOptions()
	o.PreparedCacheBytes = 4 << 20
	o.MemoryLimitBytes = 8 << 20
	return o
}
func prepareReuse(t *testing.T, e *Engine, s source.Source, parallel bool) *PreparedQuery {
	t.Helper()
	q := streamTestQuery()
	if parallel {
		q.Hints.ForceStrategy = "partitioned-hash"
		q.Hints.Cardinality = 1024
	}
	p, err := e.Prepare(context.Background(), s, q, PrepareOptions{MaxConcurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func runReuse(t *testing.T, p *PreparedQuery, ctx context.Context, materialize bool) Result {
	t.Helper()
	r, err := p.Execute(ctx, ExecuteOptions{Materialize: materialize})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestPreparedReuseContinuousOwnershipAndDetachedReferences(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "partitioned"}[parallel], func(t *testing.T) {
			o := reuseTestOptions()
			o.Workers = 4
			o.Pipeline.MaxInFlightBytes = 2 << 20
			e := New(o)
			defer e.Close()
			src := &reuseTestSource{generatedStreamSource: &generatedStreamSource{rows: 8192, groups: 127}}
			p := prepareReuse(t, e, src, parallel)
			defer p.Close()
			first := runReuse(t, p, context.Background(), true)
			saved, _ := json.Marshal(first.Rows)
			var retained int64
			for i := 0; i < 20; i++ {
				r := runReuse(t, p, context.Background(), i%2 == 0)
				if r.SelectedRows != 8192 || r.GroupCount != 127 || r.Checksum != 8192 {
					t.Fatalf("bad reset result: %+v", r)
				}
				stats := e.PreparedCacheStats()
				if stats.IdleEntries != 1 || stats.IdleBytes <= 0 || stats.IdleBytes != e.memory.Used() || stats.ReservedBytes != stats.IdleBytes || stats.ReservedBytes > stats.LimitBytes {
					t.Fatalf("cache=%+v root=%d", stats, e.memory.Used())
				}
				if retained != 0 && retained != stats.IdleBytes {
					t.Fatalf("retained capacity grew: %d -> %d", retained, stats.IdleBytes)
				}
				retained = stats.IdleBytes
				u := e.cache.head
				if u.state.ctx != nil || u.workspace.ctx != nil {
					t.Fatal("idle context retained")
				}
				if u.partitioned != nil && (u.partitioned.query != nil || u.partitioned.budget != nil) {
					t.Fatal("idle plan/query retained")
				}
				if _, err := u.state.account.Reserve(context.Background(), 1); !errors.Is(err, memory.ErrReusableAccountDetached) {
					t.Fatalf("idle owner still live: %v", err)
				}
			}
			for _, q := range src.queries {
				if q == nil || q.Used() != 0 {
					t.Fatalf("finished query retained: %v", q)
				}
			}
			current, _ := json.Marshal(first.Rows)
			if string(current) != string(saved) {
				t.Fatal("returned results changed after reuse")
			}
			if e.PreparedCacheStats().Hits != 20 {
				t.Fatalf("no actual warm reuse: %+v", e.PreparedCacheStats())
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if e.memory.Used() != 0 || e.PreparedCacheStats().ReservedBytes != 0 {
				t.Fatal("plan Close retained cache")
			}
		})
	}
}
func TestPreparedReuseExternalBudgetsBypass(t *testing.T) {
	e := New(reuseTestOptions())
	defer e.Close()
	src := &generatedStreamSource{rows: 1024, groups: 8}
	p := prepareReuse(t, e, src, false)
	defer p.Close()
	runReuse(t, p, context.Background(), false)
	before := e.PreparedCacheStats()
	for _, root := range []*memory.EngineBudget{e.memory, memory.NewEngineBudget(8 << 20)} {
		q, err := root.BeginQuery(memory.BudgetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		runReuse(t, p, memory.WithQueryBudget(context.Background(), q), true)
		if q.Used() != 0 {
			t.Fatalf("external budget retained %d", q.Used())
		}
		q.Close()
		if got := e.PreparedCacheStats(); got != before {
			t.Fatalf("external execution used cache: %+v -> %+v", before, got)
		}
	}
}
func TestPreparedReuseRootPressureAndAdmissionEviction(t *testing.T) {
	o := reuseTestOptions()
	e := New(o)
	defer e.Close()
	src := &generatedStreamSource{rows: 1024, groups: 8}
	p := prepareReuse(t, e, src, false)
	defer p.Close()
	runReuse(t, p, context.Background(), false)
	charge := e.PreparedCacheStats().IdleBytes
	e.cache.mu.Lock()
	e.cache.limit = charge
	e.cache.mu.Unlock()
	p2 := prepareReuse(t, e, src, false)
	defer p2.Close()
	runReuse(t, p2, context.Background(), false)
	if st := e.PreparedCacheStats(); st.IdleEntries != 1 || st.IdleBytes != charge || st.Evictions != 1 {
		t.Fatalf("admission cap: %+v", st)
	}
	// Another plan/root reservation can evict idle state even though the old
	// prepared plan stays open. No special cache-aware allocation API is needed.
	lease, err := e.Memory().Reserve(context.Background(), e.memory.Limit())
	if err != nil {
		t.Fatal(err)
	}
	if st := e.PreparedCacheStats(); st.IdleBytes != 0 || st.ReservedBytes != 0 || st.Evictions != 2 {
		t.Fatalf("pressure reclaim: %+v", st)
	}
	if e.memory.Used() != e.memory.Limit() {
		t.Fatal("root pressure accounting mismatch")
	}
	lease.Release()
	runReuse(t, p, context.Background(), false)
	if e.PreparedCacheStats().IdleBytes != charge {
		t.Fatal("evicted plan could not run cold")
	}
}
func TestPreparedReuseAttachFailureDiscardsBeforeColdFallback(t *testing.T) {
	o := reuseTestOptions()
	o.Workers = 4
	o.Pipeline.MaxInFlightBytes = 2 << 20
	e := New(o)
	defer e.Close()
	p := prepareReuse(t, e, &generatedStreamSource{rows: 8192, groups: 127}, true)
	defer p.Close()
	runReuse(t, p, context.Background(), false)
	u := e.cache.head
	stateBytes, workBytes := u.state.Used(), u.workspace.Used()
	if stateBytes <= 0 || workBytes <= 0 {
		t.Fatal("both accounts must own allocations")
	}
	q, err := e.memory.BeginQuery(memory.BudgetOptions{HardLimitBytes: stateBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	// State Attach succeeds, workspace Attach fails. Destruction must release
	// tokens from both destinations without a root accounting gap or old owner.
	if got := e.cache.take(memory.WithQueryBudget(context.Background(), q), p, q); got != nil {
		t.Fatal("oversize cache unexpectedly attached")
	}
	if q.Used() != 0 || e.memory.Used() != 0 || e.PreparedCacheStats().ReservedBytes != 0 {
		t.Fatalf("partial attach leaked: q=%d root=%d cache=%+v", q.Used(), e.memory.Used(), e.PreparedCacheStats())
	}
	runReuse(t, p, context.Background(), false)
}
func TestPreparedReuseCancellationDestroysActiveEntry(t *testing.T) {
	e := New(reuseTestOptions())
	defer e.Close()
	src := &generatedStreamSource{rows: 1024, groups: 8}
	p := prepareReuse(t, e, src, false)
	defer p.Close()
	runReuse(t, p, context.Background(), false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src.cancel = cancel
	src.cancelAt = 128
	_, err := p.Execute(ctx, ExecuteOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if e.memory.Used() != 0 || e.PreparedCacheStats().ReservedBytes != 0 {
		t.Fatal("cancel cached partial state")
	}
	src.cancel = nil
	runReuse(t, p, context.Background(), false)
}
func TestPreparedReuseConcurrentQueriesPressureAndClose(t *testing.T) {
	e := New(reuseTestOptions())
	p := prepareReuse(t, e, &generatedStreamSource{rows: 2048, groups: 127}, false)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				r, err := p.Execute(context.Background(), ExecuteOptions{})
				if err != nil {
					errs <- err
					return
				}
				if r.Checksum != 2048 || r.GroupCount != 127 {
					errs <- errors.New("concurrent result mismatch")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if st := e.PreparedCacheStats(); st.IdleEntries < 1 || st.IdleEntries > 4 || st.ReservedBytes > st.LimitBytes || st.IdleBytes != e.memory.Used() {
		t.Fatalf("concurrent cache: %+v root=%d", st, e.memory.Used())
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if e.memory.Used() != 0 || e.PreparedCacheStats().ReservedBytes != 0 {
		t.Fatal("engine Close leaked")
	}
	if _, err := p.Execute(context.Background(), ExecuteOptions{}); !errors.Is(err, ErrEngineClosed) {
		t.Fatalf("execute after engine Close: %v", err)
	}
	p.Close()
}
func TestEngineCloseWaitsForAcceptedQueuedExecution(t *testing.T) {
	e := New(reuseTestOptions())
	p := prepareReuse(t, e, &generatedStreamSource{rows: 1024, groups: 8}, false)
	p.executions = make(chan struct{}, 1)
	p.executions <- struct{}{}
	ctx := &queuedExecuteContext{Context: context.Background(), entered: make(chan struct{})}
	execution := make(chan error, 1)
	go func() { _, err := p.Execute(ctx, ExecuteOptions{}); execution <- err }()
	<-ctx.entered
	closed := make(chan error, 2)
	go func() { closed <- e.Close() }()
	deadline := time.After(time.Second)
	for {
		if errors.Is(e.checkOpen(), ErrEngineClosed) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Close not started")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	go func() { closed <- e.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close finished while accepted execution queued: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	<-p.executions
	if err := <-execution; err != nil {
		t.Fatalf("accepted query rejected by Close: %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close stuck")
		}
	}
	p.Close()
	if e.memory.Used() != 0 {
		t.Fatal("close retained idle cache")
	}
}

// This oracle counts backing capacities directly, without reservation counters,
// cache ledgers, Estimate* functions, or RetainedBytes implementations.
func reusePhysicalSlices(v reflect.Value) int64 {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	var bytes int64
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.Slice:
			bytes += int64(f.Cap()) * int64(f.Type().Elem().Size())
		case reflect.Struct:
			bytes += reusePhysicalSlices(f)
		}
	}
	return bytes
}
func reusePhysicalTable(t *aggregateStateTable) int64 {
	switch t.shape {
	case GroupShapeI32:
		return reusePhysicalSlices(reflect.ValueOf(t.i32))
	case GroupShapeI64:
		return reusePhysicalSlices(reflect.ValueOf(t.u64))
	case GroupShapeU32:
		return reusePhysicalSlices(reflect.ValueOf(t.u32))
	case GroupShapePairFixed:
		return reusePhysicalSlices(reflect.ValueOf(t.pair))
	}
	panic("ineligible table")
}
func TestPreparedReuseActualCapacitiesAndInputBound(t *testing.T) {
	var retained int64
	for _, rows := range []int{1024, 1 << 20} {
		o := reuseTestOptions()
		o.Workers = 4
		o.Pipeline.MaxInFlightBytes = 2 << 20
		e := New(o)
		p := prepareReuse(t, e, &generatedStreamSource{rows: rows, groups: 127}, true)
		runReuse(t, p, context.Background(), false)
		u := e.cache.head
		s := u.partitioned
		var actualState int64
		for _, owner := range s.owners {
			if owner != nil {
				actualState += reusePhysicalTable(owner.table)
			}
		}
		actualWorkspace := reusePhysicalSlices(reflect.ValueOf(s)) + reusePhysicalSlices(reflect.ValueOf(s.wave))
		if actualState != u.state.Used() || actualWorkspace != u.workspace.Used() || actualState+actualWorkspace != e.memory.Used() {
			t.Fatalf("actual state=%d workspace=%d, charged state=%d workspace=%d root=%d", actualState, actualWorkspace, u.state.Used(), u.workspace.Used(), e.memory.Used())
		}
		if retained != 0 && retained != u.bytes() {
			t.Fatalf("cache scratch grew with input: %d -> %d", retained, u.bytes())
		}
		retained = u.bytes()
		p.Close()
		e.Close()
		if e.memory.Used() != 0 {
			t.Fatal("Close retained actual backing")
		}
	}
}
func TestPreparedReuseConcurrentRootPressure(t *testing.T) {
	o := reuseTestOptions()
	o.MemoryLimitBytes = 1 << 20
	e := New(o)
	defer e.Close()
	plans := make([]*PreparedQuery, 4)
	for i := range plans {
		plans[i] = prepareReuse(t, e, &generatedStreamSource{rows: 4096, groups: 127}, false)
		defer plans[i].Close()
	}
	var wg sync.WaitGroup
	for _, p := range plans {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				r, err := p.Execute(context.Background(), ExecuteOptions{})
				if err != nil {
					t.Error(err)
					return
				}
				if r.Checksum != 4096 {
					t.Error("pressure altered result")
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 80; i++ {
			// Keep enough active-query headroom. Reclamation and admission can race,
			// but a root reservation may never claim more than the hard cap.
			lease, err := e.Memory().Reserve(context.Background(), (1<<20)-(64<<10))
			if err != nil {
				if !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Error(err)
				}
				continue
			}
			if e.memory.Used() > e.memory.Limit() {
				t.Error("root cap exceeded")
			}
			lease.Release()
		}
	}()
	wg.Wait()
	st := e.PreparedCacheStats()
	if st.ReservedBytes != st.IdleBytes || st.IdleBytes != e.memory.Used() || st.ReservedBytes > st.LimitBytes {
		t.Fatalf("pressure leak: %+v root=%d", st, e.memory.Used())
	}
}

func TestPreparedReuseNullableLayoutsAndGrowthAcrossOwners(t *testing.T) {
	for _, shape := range []string{"i32", "i64", "u32", "pair"} {
		t.Run(shape, func(t *testing.T) {
			const rows = 131
			b := memory.NewBatch()
			b.Reset(rows)
			key32, key64, keyU, second, vals := make([]int32, rows), make([]int64, rows), make([]uint32, rows), make([]int64, rows), make([]int64, rows)
			keysValid, secondValid, valueValid := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
			typ := memory.TypeInt32
			switch shape {
			case "i32":
				b.Int32s["key"] = &memory.Int32Vector{Values: key32, Validity: keysValid}
			case "i64", "pair":
				typ = memory.TypeInt64
				b.Int64s["key"] = &memory.Int64Vector{Values: key64, Validity: keysValid}
			case "u32":
				typ = memory.TypeUint32
				b.Uint32s["key"] = &memory.Uint32Vector{Values: keyU, Validity: keysValid}
			}
			b.Int64s["value"] = &memory.Int64Vector{Values: vals, Validity: valueValid}
			fields := []memory.Field{{Name: "key", Type: typ, Nullable: true}, {Name: "value", Type: memory.TypeInt64, Nullable: true}}
			q := partitionedQuery()
			q.Hints.Cardinality = 1
			if shape == "pair" {
				b.Int64s["second"] = &memory.Int64Vector{Values: second, Validity: secondValid}
				fields = append(fields, memory.Field{Name: "second", Type: memory.TypeInt64, Nullable: true})
				q.GroupBy = append(q.GroupBy, "second")
			}
			src, err := inmemory.New(memory.NewSchema(fields...), b)
			if err != nil {
				t.Fatal(err)
			}
			o := reuseTestOptions()
			o.Workers = 4
			o.Pipeline.MaxInFlightBytes = 2 << 20
			e := New(o)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			type expected struct {
				rows, values  int
				sum, min, max int64
			}
			for iteration := 0; iteration < 8; iteration++ {
				clear(keysValid)
				clear(secondValid)
				clear(valueValid)
				want := map[string]expected{}
				for row := 0; row < rows; row++ {
					key := int64(row%(2+iteration*19) + iteration*1000)
					value := int64(row%13 - 6)
					key32[row], key64[row], keyU[row], second[row], vals[row] = int32(key), key, uint32(key), int64(row%3), value
					kv, sv, vv := row%(iteration+2) != 0, row%3 != 0, row%4 != 0
					if kv {
						memory.Set(keysValid, row)
					}
					if sv {
						memory.Set(secondValid, row)
					}
					if vv {
						memory.Set(valueValid, row)
					}
					canonical := "NULL"
					if kv {
						canonical = fmt.Sprint(key)
					}
					if shape == "pair" {
						canonical += "/"
						if sv {
							canonical += fmt.Sprint(second[row])
						} else {
							canonical += "NULL"
						}
					}
					state := want[canonical]
					state.rows++
					if vv {
						if state.values == 0 {
							state.min, state.max = value, value
						}
						state.values++
						state.sum += value
						state.min = min(state.min, value)
						state.max = max(state.max, value)
					}
					want[canonical] = state
				}
				r := runReuse(t, p, context.Background(), true)
				if r.GroupCount != len(want) {
					t.Fatalf("iteration%d groups=%d want=%d", iteration, r.GroupCount, len(want))
				}
				for _, row := range r.Rows {
					canonical := "NULL"
					if row.Keys[0].Valid {
						if shape == "u32" {
							canonical = fmt.Sprint(row.Keys[0].Uint64)
						} else {
							canonical = fmt.Sprint(row.Keys[0].Int64)
						}
					}
					if shape == "pair" {
						canonical += "/"
						if row.Keys[1].Valid {
							canonical += fmt.Sprint(row.Keys[1].Int64)
						} else {
							canonical += "NULL"
						}
					}
					state, ok := want[canonical]
					if !ok {
						t.Fatalf("stale/duplicate key %s", canonical)
					}
					delete(want, canonical)
					a := row.Aggregates
					valid := state.values > 0
					if row.Sum != state.sum || row.Count != uint64(state.rows) || a[0].Valid != valid || a[2].Int64 != int64(state.values) || a[3].Valid != valid || a[4].Valid != valid || a[5].Valid != valid {
						t.Fatalf("key%s state%+v row%+v", canonical, state, row)
					}
					if valid && (a[3].Int64 != state.min || a[4].Int64 != state.max || a[5].Float64 != float64(state.sum)/float64(state.values)) {
						t.Fatalf("key%s aggregates%+v expected%+v", canonical, a, state)
					}
				}
				if len(want) != 0 {
					t.Fatal("missing groups")
				}
				u := e.cache.head
				var actual int64
				for _, owner := range u.partitioned.owners {
					if owner != nil {
						actual += reusePhysicalTable(owner.table)
					}
				}
				if actual != u.state.Used() {
					t.Fatalf("growth/reset capacity lost charge: actual%d charged%d", actual, u.state.Used())
				}
			}
			if e.PreparedCacheStats().Hits != 7 {
				t.Fatal("nullable test did not reuse")
			}
		})
	}
}

func TestPreparedReuseCloseWaitsForUnlinkedPressureVictim(t *testing.T) {
	for _, scope := range []string{"plan", "engine"} {
		t.Run(scope, func(t *testing.T) {
			e := New(reuseTestOptions())
			p := prepareReuse(t, e, &generatedStreamSource{rows: 1024, groups: 8}, false)
			runReuse(t, p, context.Background(), false)
			// Deterministic point immediately after a root reclaimer takes its exclusive
			// victim, but before it drops backing buffers. No active execution remains.
			e.cache.mu.Lock()
			victim := e.cache.tail
			e.cache.unlink(victim)
			e.cache.mu.Unlock()
			done := make(chan error, 1)
			go func() {
				if scope == "plan" {
					done <- p.Close()
				} else {
					done <- e.Close()
				}
			}()
			select {
			case err := <-done:
				t.Fatalf("Close raced in-flight eviction: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			if e.memory.Used() != victim.bytes() {
				t.Fatal("unlinked victim lost root charge")
			}
			e.cache.discard(victim)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close failed to join eviction")
			}
			p.Close()
			e.Close()
			if e.memory.Used() != 0 {
				t.Fatal("evicted bytes survived Close")
			}
		})
	}
}

func TestPreparedReuseExplicitDefaultsAndBypass(t *testing.T) {
	if DefaultOptions().PreparedCacheBytes != 64<<20 {
		t.Fatal("documented default changed")
	}
	for _, limit := range []int64{0, 1, 64 << 20} {
		o := reuseTestOptions()
		o.PreparedCacheBytes = limit
		e := New(o)
		p := prepareReuse(t, e, &generatedStreamSource{rows: 1024, groups: 8}, false)
		runReuse(t, p, context.Background(), false)
		st := e.PreparedCacheStats()
		if st.LimitBytes != min(limit, o.MemoryLimitBytes) {
			t.Fatalf("cache limit=%d", st.LimitBytes)
		}
		if limit < 2 && (e.memory.Used() != 0 || st.IdleEntries != 0) {
			t.Fatal("disabled/oversize entry retained")
		}
		p.Close()
		e.Close()
	}
	e := New(reuseTestOptions())
	defer e.Close()
	p := prepareReuse(t, e, &generatedStreamSource{rows: 1024, groups: 8}, false)
	defer p.Close()
	runReuse(t, p, context.Background(), false)
	before := e.PreparedCacheStats()
	q, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	stream, err := p.NewResidentAggregateStream(ctx, p.resultSchema(), 1)
	if err != nil {
		t.Fatal(err)
	}
	dst := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	stream.Close()
	if q.Used() != 0 || e.PreparedCacheStats() != before {
		t.Fatal("public stream touched cache or leaked query ownership")
	}
}
func TestPreparedReuseStringAndDictionaryExcluded(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		t.Run(fmt.Sprint(dictionary), func(t *testing.T) {
			b := memory.NewBatch()
			b.Reset(3)
			b.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
			key := memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingView}
			if dictionary {
				key.Encoding = memory.EncodingDictionary
				key.DictionaryDomain = "test-reuse"
				b.DictStrings["key"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Encoding: memory.EncodingDictionary, DictionaryDomain: key.DictionaryDomain}
			} else {
				b.Strings["key"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("aba"), Encoding: memory.EncodingView}
			}
			src, err := inmemory.New(memory.NewSchema(key, memory.Field{Name: "value", Type: memory.TypeInt64}), b)
			if err != nil {
				t.Fatal(err)
			}
			if dictionary {
				src.WithDictionary("key", source.Dictionary{Domain: key.DictionaryDomain, Offsets: []uint32{0, 1, 2}, Data: []byte("ab")})
			}
			e := New(reuseTestOptions())
			defer e.Close()
			p := prepareReuse(t, e, src, false)
			defer p.Close()
			first := runReuse(t, p, context.Background(), true)
			saved, _ := json.Marshal(first.Rows)
			for i := 0; i < 3; i++ {
				runReuse(t, p, context.Background(), true)
			}
			after, _ := json.Marshal(first.Rows)
			if string(saved) != string(after) {
				t.Fatal("string result changed")
			}
			if e.PreparedCacheStats().IdleEntries != 0 || e.memory.Used() != 0 {
				t.Fatal("string/dictionary state cached")
			}
		})
	}
}

func TestPreparedReuseMaterializedRowsSurviveChangedInput(t *testing.T) {
	b := memory.NewBatch()
	b.Reset(3)
	keys := []int32{1, 1, 2}
	vals := []int64{3, 5, 7}
	b.Int32s["key"] = &memory.Int32Vector{Values: keys}
	b.Int64s["value"] = &memory.Int64Vector{Values: vals}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	e := New(reuseTestOptions())
	defer e.Close()
	p := prepareReuse(t, e, src, false)
	defer p.Close()
	first := runReuse(t, p, context.Background(), true)
	saved, _ := json.Marshal(first.Rows)
	keys[0], keys[1], keys[2] = 8, 9, 10
	vals[0], vals[1], vals[2] = 100, 200, 300
	second := runReuse(t, p, context.Background(), true)
	if second.Checksum != 600 || second.GroupCount != 3 {
		t.Fatal("input update not observed")
	}
	after, _ := json.Marshal(first.Rows)
	if string(saved) != string(after) {
		t.Fatal("old result aliases reset state")
	}
	if e.PreparedCacheStats().Hits != 1 {
		t.Fatal("test did not reuse")
	}
}
