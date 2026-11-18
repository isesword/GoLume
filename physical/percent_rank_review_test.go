package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

// This source is single-open and invalidates the immediately preceding borrowed
// vectors on Next and Close. Values may not be retained by raw slice aliases.
type percentRankReviewSource struct {
	source.Source
	opens, closes int
	afterNext     func()
}

func (s *percentRankReviewSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.opens != 1 {
		return nil, fmt.Errorf("source cannot replay")
	}
	ch, e := s.Source.NewBatchStream(ctx, o)
	if e != nil {
		return nil, e
	}
	return &percentRankReviewBorrow{child: ch, owner: s}, nil
}

type percentRankReviewBorrow struct {
	child  source.BatchStream
	owner  *percentRankReviewSource
	ints   [][]int64
	floats [][]float64
	closed bool
}

func (s *percentRankReviewBorrow) poison() {
	for _, v := range s.ints {
		for i := range v {
			v[i] = -999
		}
	}
	for _, v := range s.floats {
		for i := range v {
			v[i] = math.Inf(1)
		}
	}
	s.ints = nil
	s.floats = nil
}
func (s *percentRankReviewBorrow) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.poison()
	ok, e := s.child.Next(ctx, dst)
	if e != nil || !ok {
		return ok, e
	}
	for _, v := range dst.Int64s {
		s.ints = append(s.ints, v.Values)
	}
	for _, v := range dst.Float64s {
		s.floats = append(s.floats, v.Values)
	}
	if s.owner.afterNext != nil {
		s.owner.afterNext()
	}
	return true, nil
}
func (s *percentRankReviewBorrow) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.owner.closes++
	s.poison()
	return s.child.Close()
}

func percentRankReviewInput(t *testing.T, permutation []int) *percentRankReviewSource {
	t.Helper()
	keys := []float64{0, 0, -1, math.Copysign(0, -1), 0, 2, math.NaN(), math.NaN(), 0, 0, 0, 0}
	fields := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "__golume_percent_rank", Type: memory.TypeInt64})
	batches := []*memory.Batch{}
	for start := 0; start < len(permutation); start += 4 {
		end := start + 4
		if end > len(permutation) {
			end = len(permutation)
		}
		n := end - start
		b := memory.NewBatch()
		b.Reset(n)
		id, g, collision := make([]int64, n), make([]int64, n), make([]int64, n)
		key := make([]float64, n)
		valid := make(memory.Bitmap, memory.WordsFor(n))
		for j, index := range permutation[start:end] {
			id[j] = int64(index)
			collision[j] = 777 + int64(index)
			if index >= 9 {
				g[j] = 2
			} else if index == 8 {
				g[j] = 1
			}
			key[j] = keys[index]
			if index >= 2 && index <= 8 {
				memory.Set(valid, j)
			}
		}
		b.Int64s["id"] = &memory.Int64Vector{Values: id}
		b.Int64s["g"] = &memory.Int64Vector{Values: g}
		b.Int64s["__golume_percent_rank"] = &memory.Int64Vector{Values: collision}
		b.Float64s["key"] = &memory.Float64Vector{Values: key, Validity: valid}
		batches = append(batches, b)
	}
	src, e := inmemory.New(fields, batches...)
	if e != nil {
		t.Fatal(e)
	}
	return &percentRankReviewSource{Source: src}
}
func percentRankReviewQuery(t *testing.T, hard int64, disk bool) (context.Context, *memory.QueryBudget) {
	t.Helper()
	opts := memory.BudgetOptions{HardLimitBytes: hard}
	if disk {
		opts.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	}
	q, e := memory.NewEngineBudget(hard).BeginQuery(opts)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := q.Close(); e != nil {
			t.Error(e)
		}
	})
	return memory.WithQueryBudget(context.Background(), q), q
}
func percentRankReviewWindow(t *testing.T, src source.Source, q *memory.QueryBudget, mode WindowExecutionMode) source.Source {
	t.Helper()
	win, e := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "key"}}, []WindowExpression{
		{Function: WindowPercentRank, Alias: "key"}, {Function: WindowPercentRank, Alias: "p"}, {Function: WindowRank, Alias: "rank"},
	}, []memory.Field{{Name: "key", Type: memory.TypeFloat64}, {Name: "p", Type: memory.TypeFloat64}, {Name: "rank", Type: memory.TypeInt64}}, q, WindowSourceOptions{Mode: mode})
	if e != nil {
		t.Fatal(e)
	}
	return win
}
func TestPercentRankReviewPeersBorrowAliases(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			order := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
			if mode != WindowExecutionOrdered {
				order = []int{7, 9, 2, 4, 8, 0, 11, 5, 1, 6, 10, 3}
			}
			input := percentRankReviewInput(t, order)
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			win := percentRankReviewWindow(t, input, q, mode)
			plan := source.StreamPlanOf(win)
			wantClass := source.StreamSpillable
			if mode == WindowExecutionResident {
				wantClass = source.StreamBlocking
			}
			if plan.Class != wantClass {
				t.Fatal("partition-sized state misclassified", plan.Class, wantClass)
			}
			stream, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 4})
			if e != nil {
				t.Fatal(e)
			}
			defer stream.Close()
			out := memory.NewBatch()
			seen := 0
			wantRank := []int64{1, 1, 3, 4, 4, 6, 7, 7, 1, 1, 1, 1}
			for {
				ok, e := stream.Next(ctx, out)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				for row := 0; row < out.Rows; row++ {
					id := int(out.Int64s["id"].Values[row])
					if seen >= len(order) || id != order[seen] {
						t.Fatal("original order", seen, id)
					}
					seen++
					want := 0.0
					if id < 8 {
						want = float64(wantRank[id]-1) / 7
					}
					for _, name := range []string{"key", "p"} {
						v := out.Float64s[name]
						if v == nil || !v.IsValid(row) || v.Values[row] != want {
							t.Fatal("peer formula", id, name, want, v)
						}
					}
					if out.Int64s["rank"].Values[row] != wantRank[id] {
						t.Fatal("mixed rank", id)
					}
					if out.Int64s["__golume_percent_rank"].Values[row] != 777+int64(id) {
						t.Fatal("private column collision")
					}
				}
			}
			out.ClearColumns()
			if seen != len(order) {
				t.Fatal("rows", seen)
			}
			if e := stream.Close(); e != nil {
				t.Fatal(e)
			}
			if e := stream.Close(); e != nil {
				t.Fatal(e)
			}
			if input.opens != 1 || input.closes != 1 {
				t.Fatal("source replay/close", input.opens, input.closes)
			}
			if q.Used() != 0 || q.Snapshot().PeakBytes > q.Limit() {
				t.Fatal("memory after close", q.Snapshot())
			}
			store, e := q.SpillManager()
			if e != nil {
				t.Fatal(e)
			}
			if store.(*memory.QuerySpillStore).Used() != 0 {
				t.Fatal("disk after close")
			}
		})
	}
}
func TestPercentRankReviewHardBudgetNoDisk(t *testing.T) {
	input := percentRankReviewInput(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	ctx, q := percentRankReviewQuery(t, 1024, false)
	win := percentRankReviewWindow(t, input, q, WindowExecutionOrdered)
	stream, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 4})
	if e == nil {
		out := memory.NewBatch()
		for {
			ok, err := stream.Next(ctx, out)
			if err != nil {
				e = err
				break
			}
			if !ok {
				break
			}
		}
		out.ClearColumns()
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("1KiB must fail without unbudgeted fallback", e)
	}
	if q.Used() != 0 {
		t.Fatal("failed admission leak", q.Snapshot())
	}
}

func TestPercentRankReviewCancelAndEarlyClose(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, cancelDuringInput := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode%d/cancel%v", mode, cancelDuringInput), func(t *testing.T) {
				input := percentRankReviewInput(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
				base, q := percentRankReviewQuery(t, 8<<20, true)
				ctx, cancel := context.WithCancel(base)
				defer cancel()
				if cancelDuringInput {
					calls := 0
					input.afterNext = func() {
						calls++
						if calls == 2 {
							cancel()
						}
					}
				}
				win := percentRankReviewWindow(t, input, q, mode)
				stream, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 4})
				if e != nil {
					t.Fatal(e)
				}
				out := memory.NewBatch()
				ok, e := stream.Next(ctx, out)
				if cancelDuringInput {
					if !errors.Is(e, context.Canceled) {
						t.Fatal("midcapture cancellation", ok, e)
					}
				} else if e != nil || !ok {
					t.Fatal("first output", ok, e)
				}
				out.ClearColumns()
				if e := stream.Close(); e != nil {
					t.Fatal(e)
				}
				if e := stream.Close(); e != nil {
					t.Fatal(e)
				}
				if input.opens != 1 || input.closes != 1 {
					t.Fatal("source lifecycle", input.opens, input.closes)
				}
				if q.Used() != 0 {
					t.Fatal("cancel/earlyclose memory leak", q.Snapshot())
				}
				store, e := q.SpillManager()
				if e != nil {
					t.Fatal(e)
				}
				if store.(*memory.QuerySpillStore).Used() != 0 {
					t.Fatal("cancel/earlyclose disk leak")
				}
			})
		}
	}
}

func TestPercentRankReviewRejectUnlimitedOrdered(t *testing.T) {
	input := percentRankReviewInput(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	manager := memory.NewBudgetManager(0)
	win, e := NewWindowSourceWithOptions(input, []string{"g"}, []SortKey{{Column: "key"}}, []WindowExpression{{Function: WindowPercentRank, Alias: "p"}}, []memory.Field{{Name: "p", Type: memory.TypeFloat64}}, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if e == nil {
		var stream source.BatchStream
		stream, e = source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 4})
		if stream != nil {
			_ = stream.Close()
		}
	}
	if e == nil {
		t.Fatal("ordered whole-partition state accepted unlimited budget")
	}
	if manager.Used() != 0 {
		t.Fatal("unlimited rejection leak")
	}
}

func TestPercentRankReviewCorruptPageMetadataCleanup(t *testing.T) {
	input := percentRankReviewInput(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	ctx, q := percentRankReviewQuery(t, 8<<20, true)
	win := percentRankReviewWindow(t, input, q, WindowExecutionOrdered)
	_, wrapped, _, lease, e := preparePercentRankWindow(ctx, win.(*WindowSource), input)
	if e != nil {
		t.Fatal(e)
	}
	defer lease.Release()
	raw, e := source.OpenBatchStream(ctx, wrapped, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 4})
	if e != nil {
		t.Fatal(e)
	}
	spool := raw.(*percentRankPartitionStream)
	defer spool.Close()
	ok, e := spool.capturePartition(ctx)
	if e != nil || !ok || len(spool.pages.Values) < 2 {
		t.Fatal("capture", ok, e)
	}
	spool.pages.Values[0].rows++ // In-memory reference claims a different decoded shape.
	out := memory.NewBatch()
	ok, e = spool.Next(ctx, out)
	if ok || !errors.Is(e, memory.ErrSpillCorrupt) {
		t.Fatal("bad page metadata emitted", ok, e)
	}
	out.ClearColumns()
	if e := spool.Close(); e != nil {
		t.Fatal(e)
	}
	lease.Release()
	if input.opens != 1 || input.closes != 1 {
		t.Fatal("corrupt source lifecycle", input.opens, input.closes)
	}
	if q.Used() != 0 {
		t.Fatal("corrupt cleanup memory", q.Snapshot())
	}
	store, e := q.SpillManager()
	if e != nil {
		t.Fatal(e)
	}
	if store.(*memory.QuerySpillStore).Used() != 0 {
		t.Fatal("corrupt cleanup disk")
	}
}

func TestPercentRankReviewDescendingNullsLast(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			order := []int{6, 7, 5, 3, 4, 2, 0, 1, 8, 9, 10, 11}
			input := percentRankReviewInput(t, order)
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			win, e := NewWindowSourceWithOptions(input, []string{"g"}, []SortKey{{Column: "key", Descending: true, NullsLast: true}}, []WindowExpression{{Function: WindowPercentRank, Alias: "p"}}, []memory.Field{{Name: "p", Type: memory.TypeFloat64}}, q, WindowSourceOptions{Mode: mode})
			if e != nil {
				t.Fatal(e)
			}
			stream, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 4})
			if e != nil {
				t.Fatal(e)
			}
			defer stream.Close()
			out := memory.NewBatch()
			seen := 0
			rank := []int64{7, 7, 6, 4, 4, 3, 1, 1, 1, 1, 1, 1}
			for {
				ok, e := stream.Next(ctx, out)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				for row := 0; row < out.Rows; row++ {
					id := int(out.Int64s["id"].Values[row])
					if seen >= len(order) || id != order[seen] {
						t.Fatal("order", id, seen)
					}
					seen++
					want := 0.0
					if id < 8 {
						want = float64(rank[id]-1) / 7
					}
					if out.Float64s["p"].Values[row] != want {
						t.Fatal("descending/null placement", id, out.Float64s["p"].Values[row], want)
					}
				}
			}
			out.ClearColumns()
			if seen != len(order) {
				t.Fatal("missing rows")
			}
			if e := stream.Close(); e != nil {
				t.Fatal(e)
			}
			if q.Used() != 0 {
				t.Fatal("leak", q.Snapshot())
			}
		})
	}
}

// Mixed outputs must read the captured input, not an earlier alias replacement.
// The signed total crosses MaxInt64 before cancellation; float expectations are
// exact rational results, not an oracle accumulated in floating point.
func TestPercentRankReviewResidentMixedOutputs(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64},
		memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "u", Type: memory.TypeUint64, Nullable: true},
		memory.Field{Name: "f", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true})
	xs := []int64{math.MaxInt64, 1, -math.MaxInt64, 0, 5, 0}
	us := []uint64{2, 3, 4, 0, 7, 0}
	fs := []float64{1e16, 1, -1e16, 0, 6, 0}
	vs := []int64{10, 0, 30, 40, 0, 8}
	order := []int{4, 2, 0, 5, 3, 1}
	var batches []*memory.Batch
	for _, id := range order {
		b := memory.NewBatch()
		b.Reset(1)
		g := int64(0)
		if id >= 4 {
			g = 1
		}
		valid := memory.Bitmap{1}
		if id == 3 || id == 5 {
			valid = memory.Bitmap{0}
		}
		vv := memory.Bitmap{1}
		if id == 1 || id == 4 {
			vv = memory.Bitmap{0}
		}
		b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(id)}}
		b.Int64s["g"] = &memory.Int64Vector{Values: []int64{g}}
		b.Int64s["x"] = &memory.Int64Vector{Values: []int64{xs[id]}, Validity: valid}
		b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{us[id]}, Validity: valid}
		b.Float64s["f"] = &memory.Float64Vector{Values: []float64{fs[id]}, Validity: valid}
		b.Int64s["v"] = &memory.Int64Vector{Values: []int64{vs[id]}, Validity: vv}
		batches = append(batches, b)
	}
	raw, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	input := &percentRankReviewSource{Source: raw}
	whole := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowUnboundedFollowing}}
	previous := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowPreceding, Offset: 1}}
	current := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
	expressions := []WindowExpression{
		{Function: WindowPercentRank, Alias: "x"},
		{Function: WindowSum, Column: "x", Alias: "signed", Frame: whole},
		{Function: WindowSum, Column: "u", Alias: "unsigned", Frame: whole},
		{Function: WindowSum, Column: "f", Alias: "exact_sum", Frame: whole},
		{Function: WindowAvg, Column: "f", Alias: "exact_avg", Frame: whole},
		{Function: WindowShift, Column: "v", Alias: "shift", Frame: previous},
		{Function: WindowDiff, Column: "v", Alias: "diff", Frame: previous},
		{Function: WindowForwardFill, Column: "v", Alias: "forward", Frame: current},
		{Function: WindowBackwardFill, Column: "v", Alias: "backward", Frame: current},
		{Function: WindowMin, Column: "v", Alias: "minimum", Frame: whole},
		{Function: WindowMax, Column: "v", Alias: "maximum", Frame: whole},
		{Function: WindowCountRows, Alias: "rows", Frame: whole},
		{Function: WindowCountValues, Column: "v", Alias: "values", Frame: whole},
	}
	fields := make([]memory.Field, len(expressions))
	for i, e := range expressions {
		typ := memory.TypeInt64
		nullable := true
		switch e.Alias {
		case "x":
			typ = memory.TypeFloat64
			nullable = false
		case "unsigned":
			typ = memory.TypeUint64
		case "exact_sum", "exact_avg":
			typ = memory.TypeFloat64
		case "rows", "values":
			nullable = false
		}
		fields[i] = memory.Field{Name: e.Alias, Type: typ, Nullable: nullable}
	}
	ctx, q := percentRankReviewQuery(t, 8<<20, true)
	win, err := NewWindowSourceWithOptions(input, []string{"g"}, []SortKey{{Column: "id"}}, expressions, fields, q, WindowSourceOptions{Mode: WindowExecutionResident})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	wantInts := map[string][]int64{
		"signed": {1, 1, 1, 1, 5, 5}, "shift": {0, 10, 0, 30, 0, 0}, "diff": {0, 0, 0, 10, 0, 0},
		"forward": {10, 10, 30, 40, 0, 8}, "backward": {10, 30, 30, 40, 8, 8},
		"minimum": {10, 10, 10, 10, 8, 8}, "maximum": {40, 40, 40, 40, 8, 8}, "rows": {4, 4, 4, 4, 2, 2}, "values": {3, 3, 3, 3, 1, 1},
	}
	masks := map[string]uint64{"shift": 10, "diff": 8, "forward": 47}
	seen := 0
	var previousBatch *memory.Batch
	for {
		out := memory.NewBatch()
		ok, nextErr := stream.Next(ctx, out)
		if previousBatch != nil && (previousBatch.Rows != 0 || len(previousBatch.Int64s) != 0 || len(previousBatch.Float64s) != 0 || len(previousBatch.Uint64s) != 0) {
			t.Fatal("previousBatch borrowed destination retained vectors")
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if !ok {
			if out.Rows != 0 || len(out.Int64s) != 0 || len(out.Float64s) != 0 || len(out.Uint64s) != 0 {
				t.Fatal("EOF destination is not empty")
			}
			break
		}
		if out.Rows != 1 || seen+out.Rows > len(order) {
			t.Fatal("invalid page size", out.Rows, seen)
		}
		for row := 0; row < out.Rows; row++ {
			id := order[seen]
			seen++
			if out.Int64s["id"].Values[row] != int64(id) {
				t.Fatal("original order lost")
			}
			pr := []float64{0, 1.0 / 3, 2.0 / 3, 1, 0, 1}[id]
			if out.Float64s["x"].Values[row] != pr || !out.Float64s["x"].IsValid(row) {
				t.Fatal("percent rank", id)
			}
			for name, want := range wantInts {
				v := out.Int64s[name]
				mask, limited := masks[name]
				valid := !limited || mask&(1<<uint(id)) != 0
				if v.IsValid(row) != valid || (valid && v.Values[row] != want[id]) {
					t.Fatalf("%s id%d validity=%v value=%d wantvalid=%v want=%d", name, id, v.IsValid(row), v.Values[row], valid, want[id])
				}
			}
			uw := uint64(9)
			sw, aw := float64(1), 1.0/3
			if id >= 4 {
				uw = 7
				sw = 6
				aw = 6
			}
			if !out.Uint64s["unsigned"].IsValid(row) || out.Uint64s["unsigned"].Values[row] != uw {
				t.Fatal("unsigned", id)
			}
			for name, want := range map[string]float64{"exact_sum": sw, "exact_avg": aw} {
				v := out.Float64s[name]
				if !v.IsValid(row) || v.Values[row] != want {
					t.Fatalf("%s id%d got %.17g want %.17g", name, id, v.Values[row], want)
				}
			}
		}
		previousBatch = out
	}
	if seen != len(order) {
		t.Fatal("missing rows", seen)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	if input.opens != 1 || input.closes != 1 {
		t.Fatal("source lifecycle", input.opens, input.closes)
	}
	if q.Used() != 0 {
		t.Fatalf("leaked %+v", q.Snapshot())
	}
	store, err := q.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if store.(*memory.QuerySpillStore).Used() != 0 {
		t.Fatal("spill leak")
	}
}
