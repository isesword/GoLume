package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"gocolumn/planner"
	"gocolumn/radix"
	"gocolumn/source"
	"sync"
	"sync/atomic"
)

type workerWorkspace struct {
	reader            source.Reader
	batch             *memory.Batch
	selection         memory.Bitmap
	expr              expr.EvalWorkspace
	selectedKeys      []uint64
	selectedPairs     []hashagg.Key128
	selectedPairValid []uint8
	selectedVals      []int64
	selectedRows      int
	nullRows          int
	nullSum           int64
}

type queryWorkspace struct {
	owner        *PreparedQuery
	workers      []workerWorkspace
	reservations []memory.Reservation

	i32  *hashagg.I32Table
	u64  *hashagg.U64Table
	u32  *hashagg.U32Table
	pair *hashagg.PairTable
	str  *hashagg.StringTable

	selectedKeys      []uint64
	selectedPairs     []hashagg.Key128
	selectedPairValid []uint8
	selectedVals      []int64
	chunkLens         []int
	nullSums          []int64
	nullCounts        []int
	radix64           *radix.Workspace
	radixPair         *radix.PairWorkspace
	radixBounded      bool
	fallbackU64       *hashagg.U64Table
	fallbackPair      *hashagg.PairTable
}

func (w *queryWorkspace) reserve(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}
	r, err := w.owner.e.memory.Reserve(ctx, n)
	if err != nil {
		return err
	}
	w.reservations = append(w.reservations, r)
	return nil
}
func (w *queryWorkspace) close() error {
	for i := range w.workers {
		if w.workers[i].reader != nil {
			_ = w.workers[i].reader.Close()
		}
	}
	for _, r := range w.reservations {
		r.Release()
	}
	w.reservations = nil
	return nil
}

func (p *PreparedQuery) newWorkspace(ctx context.Context) (*queryWorkspace, error) {
	w := &queryWorkspace{owner: p}
	ok := false
	defer func() {
		if !ok {
			_ = w.close()
		}
	}()
	workers := 1
	if p.strategy.Kind == planner.StrategyRadix {
		workers = p.strategy.Workers
		if workers < 1 {
			workers = 1
		}
	}
	words := memory.WordsFor(p.maxPartRows)
	exprBytes := int64(workers * len(p.cq.Predicate.Instructions) * 2 * words * 8)
	selBytes := int64(workers * words * 8)
	if err := w.reserve(ctx, exprBytes+selBytes); err != nil {
		return nil, fmt.Errorf("predicate workspace: %w", err)
	}
	w.workers = make([]workerWorkspace, workers)
	for i := range w.workers {
		rd, err := p.src.NewReader(ctx, source.ReadOptions{Kernels: p.e.kernels})
		if err != nil {
			return nil, err
		}
		w.workers[i].reader = rd
		w.workers[i].batch = memory.NewBatch()
		w.workers[i].selection = make(memory.Bitmap, words)
		w.workers[i].expr.Ensure(p.maxPartRows, len(p.cq.Predicate.Instructions))
	}
	if p.strategy.Kind == planner.StrategyRadix {
		rows := p.maxPartRows
		w.radixBounded = p.executionPolicy == ExecutionBounded || p.e.opts.MemoryLimitBytes > 0
		waveRows := workers * rows
		if !w.radixBounded {
			waveRows = int(p.stats.Rows)
			w.chunkLens = make([]int, len(p.parts))
			w.nullSums = make([]int64, len(p.parts))
			w.nullCounts = make([]int, len(p.parts))
		}
		switch p.cq.GroupShape {
		case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
			if err := w.reserve(ctx, int64(waveRows)*(8+8)+radix.EstimateWorkspaceBytes(waveRows, p.cq.CardinalityHint, p.strategy.RadixBits, workers)); err != nil {
				return nil, fmt.Errorf("radix workspace: %w", err)
			}
			w.selectedKeys = make([]uint64, waveRows)
			w.selectedVals = make([]int64, waveRows)
			if w.radixBounded {
				for i := range w.workers {
					start := i * rows
					w.workers[i].selectedKeys = w.selectedKeys[start : start+rows]
					w.workers[i].selectedVals = w.selectedVals[start : start+rows]
				}
			}
			w.radix64 = radix.NewWorkspace(waveRows, p.cq.CardinalityHint, p.strategy.RadixBits, p.e.kernels)
		case GroupShapePairFixed:
			if err := w.reserve(ctx, int64(waveRows)*(16+1+8)+radix.EstimatePairWorkspaceBytes(waveRows, p.cq.CardinalityHint, p.strategy.RadixBits, workers)); err != nil {
				return nil, fmt.Errorf("pair radix workspace: %w", err)
			}
			w.selectedPairs = make([]hashagg.Key128, waveRows)
			w.selectedPairValid = make([]uint8, waveRows)
			w.selectedVals = make([]int64, waveRows)
			if w.radixBounded {
				for i := range w.workers {
					start := i * rows
					w.workers[i].selectedPairs = w.selectedPairs[start : start+rows]
					w.workers[i].selectedPairValid = w.selectedPairValid[start : start+rows]
					w.workers[i].selectedVals = w.selectedVals[start : start+rows]
				}
			}
			w.radixPair = radix.NewPairWorkspace(waveRows, p.cq.CardinalityHint, p.strategy.RadixBits, p.e.kernels)
		default:
			return nil, fmt.Errorf("radix workspace unsupported for group shape %d", p.cq.GroupShape)
		}
	} else if p.strategy.Kind != planner.StrategySorted {
		switch p.cq.GroupShape {
		case GroupShapeI32:
			tableOpts := hashagg.TableOptions{Aggregates: p.cq.AggregateLayout, Reserve: func(n int64) error { return w.reserve(context.Background(), n) }}
			if err := w.reserve(ctx, hashagg.EstimateI32TableBytesWithOptions(p.cq.CardinalityHint, tableOpts)); err != nil {
				return nil, err
			}
			w.i32 = hashagg.NewI32TableWithOptions(p.cq.CardinalityHint, tableOpts)
		case GroupShapeI64:
			tableOpts := hashagg.TableOptions{Aggregates: p.cq.AggregateLayout, Reserve: func(n int64) error { return w.reserve(context.Background(), n) }}
			if err := w.reserve(ctx, hashagg.EstimateU64TableBytesWithOptions(p.cq.CardinalityHint, tableOpts)); err != nil {
				return nil, err
			}
			w.u64 = hashagg.NewU64TableWithOptions(p.cq.CardinalityHint, tableOpts)
		case GroupShapeU32, GroupShapeDictionaryString:
			tableOpts := hashagg.TableOptions{Aggregates: p.cq.AggregateLayout, Reserve: func(n int64) error { return w.reserve(context.Background(), n) }}
			if err := w.reserve(ctx, hashagg.EstimateU32TableBytesWithOptions(p.cq.CardinalityHint, tableOpts)); err != nil {
				return nil, err
			}
			w.u32 = hashagg.NewU32TableWithOptions(p.cq.CardinalityHint, tableOpts)
		case GroupShapePairFixed:
			tableOpts := hashagg.TableOptions{Aggregates: p.cq.AggregateLayout, Reserve: func(n int64) error { return w.reserve(context.Background(), n) }}
			if err := w.reserve(ctx, hashagg.EstimatePairTableBytesWithOptions(p.cq.CardinalityHint, tableOpts)); err != nil {
				return nil, err
			}
			w.pair = hashagg.NewPairTableWithOptions(p.cq.CardinalityHint, tableOpts)
		case GroupShapeStringView:
			arenaHint := p.cq.CardinalityHint * p.cq.AverageKeyBytes
			if arenaHint > 1<<20 {
				arenaHint = 1 << 20
			}
			if arenaHint < 4096 {
				arenaHint = 4096
			}
			tableOpts := hashagg.TableOptions{Aggregates: p.cq.AggregateLayout, Reserve: func(n int64) error { return w.reserve(context.Background(), n) }}
			if err := w.reserve(ctx, hashagg.EstimateStringTableBytesWithOptions(p.cq.CardinalityHint, arenaHint, tableOpts)); err != nil {
				return nil, err
			}
			w.str = hashagg.NewStringTableWithOptions(p.cq.CardinalityHint, arenaHint, tableOpts)
		}
	}
	ok = true
	return w, nil
}

func (w *queryWorkspace) ensureFallbackU64(ctx context.Context) (*hashagg.U64Table, error) {
	if w.fallbackU64 != nil {
		return w.fallbackU64, nil
	}
	if err := w.reserve(ctx, hashagg.EstimateU64TableBytes(w.owner.cq.CardinalityHint)); err != nil {
		return nil, err
	}
	w.fallbackU64 = hashagg.NewU64TableWithOptions(w.owner.cq.CardinalityHint, hashagg.TableOptions{Reserve: func(n int64) error {
		return w.reserve(context.Background(), n)
	}})
	return w.fallbackU64, nil
}

func (w *queryWorkspace) ensureFallbackPair(ctx context.Context) (*hashagg.PairTable, error) {
	if w.fallbackPair != nil {
		return w.fallbackPair, nil
	}
	if err := w.reserve(ctx, hashagg.EstimatePairTableBytes(w.owner.cq.CardinalityHint)); err != nil {
		return nil, err
	}
	w.fallbackPair = hashagg.NewPairTableWithOptions(w.owner.cq.CardinalityHint, hashagg.TableOptions{Reserve: func(n int64) error {
		return w.reserve(context.Background(), n)
	}})
	return w.fallbackPair, nil
}

type workspacePool struct {
	max     int
	create  func(context.Context) (*queryWorkspace, error)
	idle    chan *queryWorkspace
	created atomic.Int32
	mu      sync.Mutex
	all     map[*queryWorkspace]struct{}
	closed  atomic.Bool
}

func newWorkspacePool(max int, create func(context.Context) (*queryWorkspace, error)) *workspacePool {
	if max < 1 {
		max = 1
	}
	return &workspacePool{max: max, create: create, idle: make(chan *queryWorkspace, max), all: make(map[*queryWorkspace]struct{})}
}
func (p *workspacePool) acquire(ctx context.Context) (*queryWorkspace, error) {
	if p.closed.Load() {
		return nil, ErrPreparedClosed
	}
	select {
	case w := <-p.idle:
		return w, nil
	default:
	}
	for {
		cur := p.created.Load()
		if int(cur) < p.max && p.created.CompareAndSwap(cur, cur+1) {
			w, err := p.create(ctx)
			if err != nil {
				p.created.Add(-1)
				return nil, err
			}
			p.mu.Lock()
			p.all[w] = struct{}{}
			p.mu.Unlock()
			return w, nil
		}
		select {
		case w := <-p.idle:
			return w, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
func (p *workspacePool) release(w *queryWorkspace) {
	if w == nil || p.closed.Load() {
		return
	}
	p.idle <- w
}
func (p *workspacePool) close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	p.mu.Lock()
	all := make([]*queryWorkspace, 0, len(p.all))
	for w := range p.all {
		all = append(all, w)
	}
	p.all = nil
	p.mu.Unlock()
	var first error
	for _, w := range all {
		if err := w.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
