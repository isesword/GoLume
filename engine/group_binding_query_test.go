package engine

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// This file intentionally uses only public pre-binding APIs, so the same
// correctness checks and benchmark can be copied into the frozen baseline.
type groupBindingCase struct {
	keys, rows, batch         int
	family, pattern, strategy string
	countOnly                 bool
	mode                      kernel.Mode
}

func (c groupBindingCase) name() string {
	return fmt.Sprintf("%s/keys=%d/%s/%s/count=%t/rows=%d/batch=%d/mode=%s", c.family, c.keys, c.pattern, c.strategy, c.countOnly, c.rows, c.batch, c.mode)
}

// Comparable semantic cells form an independent oracle, not serialized engine
// keys. NaNs share a category, zeros compare numerically, and NULL has no payload.
type groupBindingCell struct {
	valid  bool
	kind   byte
	number float64
	text   string
}
type groupBindingKey [8]groupBindingCell
type groupBindingWant struct {
	count, values uint64
	signed        int64
	unsigned      uint64
	floating      float64
}
type groupBindingFixture struct {
	src      *inmemory.Source
	query    expr.Query
	want     map[groupBindingKey]groupBindingWant
	selected int
}

func groupBindingCellAt(c groupBindingCase, row, lane int) groupBindingCell {
	g := row % 34
	cell := groupBindingCell{valid: c.pattern == "dense" || (g/2+lane)%7 != 0}
	if !cell.valid {
		return groupBindingCell{}
	}
	if c.family == "numeric" || (c.family == "mixed" && lane%3 == 0) {
		cell.kind = 'n'
		switch g {
		case 0, 1:
			cell.number = 0
		case 2, 3:
			cell.kind = 'N'
		default:
			cell.number = float64(g/2) - 9
		}
	} else {
		cell.kind = 's'
		cell.text = fmt.Sprintf("key\x00%02d", g/2)
		if g/2 == 0 {
			cell.text = ""
		}
	}
	return cell
}
func makeGroupBindingFixture(tb testing.TB, c groupBindingCase) groupBindingFixture {
	tb.Helper()
	f := groupBindingFixture{want: make(map[groupBindingKey]groupBindingWant)}
	fields := []memory.Field{{Name: "v", Type: memory.TypeInt64, Nullable: true}, {Name: "u", Type: memory.TypeUint64, Nullable: true}, {Name: "x", Type: memory.TypeFloat64, Nullable: true}, {Name: "pred", Type: memory.TypeInt32}}
	for lane := 0; lane < c.keys; lane++ {
		name := fmt.Sprintf("k%d", lane)
		f.query.GroupBy = append(f.query.GroupBy, name)
		field := memory.Field{Name: name, Type: memory.TypeString, Nullable: c.pattern != "dense"}
		if c.family == "numeric" || (c.family == "mixed" && lane%3 == 0) {
			field.Type = memory.TypeFloat64
		} else if c.family == "dictionary" || (c.family == "mixed" && lane%3 == 2) {
			field.Encoding = memory.EncodingDictionary
		}
		fields = append(fields, field)
	}
	f.query.Aggregates = []expr.Aggregate{expr.Count("n"), expr.CountColumn("v", "valid_n")}
	if !c.countOnly {
		f.query.Aggregates = append(f.query.Aggregates, expr.Sum("v", "signed"), expr.Sum("u", "unsigned"), expr.Sum("x", "floating"))
	}
	if c.pattern == "selective" || c.pattern == "empty" {
		f.query.Filter = expr.GE(expr.Col("pred"), expr.I64(0))
	}
	if c.strategy != "spill" {
		f.query.Hints.ForceStrategy = c.strategy
	}
	f.query.Hints.Cardinality = 128
	order := make([]int, c.rows)
	keyAt := func(row int) groupBindingKey {
		var k groupBindingKey
		for lane := 0; lane < c.keys; lane++ {
			k[lane] = groupBindingCellAt(c, row, lane)
		}
		return k
	}
	for i := range order {
		order[i] = i
	}
	if c.strategy == "sorted" {
		sort.SliceStable(order, func(i, j int) bool {
			a, b := keyAt(order[i]), keyAt(order[j])
			for lane := 0; lane < c.keys; lane++ {
				x, y := a[lane], b[lane]
				if x == y {
					continue
				}
				if x.valid != y.valid {
					return !x.valid
				}
				if x.kind != y.kind {
					return x.kind < y.kind
				}
				if x.kind == 's' {
					return x.text < y.text
				}
				return x.number < y.number
			}
			return false
		})
	}
	var batches []*memory.Batch
	for start := 0; start < c.rows; start += c.batch {
		n := min(c.batch, c.rows-start)
		b := memory.NewBatch()
		b.Rows = n
		valid := make(memory.Bitmap, (n+63)/64)
		iv, uv, fv, pred := make([]int64, n), make([]uint64, n), make([]float64, n), make([]int32, n)
		for j := 0; j < n; j++ {
			row := order[start+j]
			iv[j] = (1 << 48) + int64(row%7)
			uv[j] = (1 << 49) + uint64(row%11)
			fv[j] = float64(row%13) / 2
			if row%11 != 0 {
				valid[j/64] |= uint64(1) << uint(j%64)
			}
			pred[j] = int32(row%5) - 4
			if c.pattern == "empty" {
				pred[j] = -1
			}
			if f.query.Filter != nil && pred[j] < 0 {
				continue
			}
			f.selected++
			k := keyAt(row)
			w := f.want[k]
			w.count++
			if row%11 != 0 {
				w.values++
				w.signed += iv[j]
				w.unsigned += uv[j]
				w.floating += fv[j]
			}
			f.want[k] = w
		}
		b.Int64s["v"] = &memory.Int64Vector{Values: iv, Validity: valid}
		b.Uint64s["u"] = &memory.Uint64Vector{Values: uv, Validity: valid}
		b.Float64s["x"] = &memory.Float64Vector{Values: fv, Validity: valid}
		b.Int32s["pred"] = &memory.Int32Vector{Values: pred}
		for lane := 0; lane < c.keys; lane++ {
			field := fields[4+lane]
			validity := make(memory.Bitmap, (n+63)/64)
			nums := make([]float64, n)
			offsets := []uint32{0}
			var data []byte
			indices := make([]uint32, n)
			// Alternate codebooks every batch, including a different unused entry.
			dictionary := []string{fmt.Sprintf("unused-%d", start)}
			codes := map[string]uint32{}
			for g := 0; g < 34; g++ {
				cell := groupBindingCellAt(groupBindingCase{family: "string", pattern: "dense"}, g, lane)
				if _, ok := codes[cell.text]; !ok {
					codes[cell.text] = uint32(len(dictionary))
					dictionary = append(dictionary, cell.text)
				}
			}
			if (start/c.batch)%2 != 0 {
				for i, j := 0, len(dictionary)-1; i < j; i, j = i+1, j-1 {
					dictionary[i], dictionary[j] = dictionary[j], dictionary[i]
				}
				for i, s := range dictionary {
					codes[s] = uint32(i)
				}
			}
			for j := 0; j < n; j++ {
				row := order[start+j]
				cell := groupBindingCellAt(c, row, lane)
				if cell.valid {
					validity[j/64] |= uint64(1) << uint(j%64)
				}
				nums[j] = cell.number
				if cell.kind == 'N' {
					nums[j] = math.Float64frombits(0x7ff8000000000001 + uint64(row%2))
				}
				if cell.valid && cell.kind == 'n' && cell.number == 0 && row%2 != 0 {
					nums[j] = math.Copysign(0, -1)
				}
				data = append(data, cell.text...)
				offsets = append(offsets, uint32(len(data)))
				indices[j] = codes[cell.text]
			}
			if c.pattern == "dense" {
				validity = nil
			}
			switch {
			case field.Type == memory.TypeFloat64:
				b.Float64s[field.Name] = &memory.Float64Vector{Values: nums, Validity: validity}
			case field.Encoding == memory.EncodingDictionary:
				data = nil
				offsets = []uint32{0}
				for _, s := range dictionary {
					data = append(data, s...)
					offsets = append(offsets, uint32(len(data)))
				}
				b.DictStrings[field.Name] = &memory.DictionaryStringVector{Indices: indices, Offsets: offsets, Data: data, Validity: validity}
			default:
				b.Strings[field.Name] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: validity}
			}
		}
		batches = append(batches, b)
	}
	var err error
	f.src, err = inmemory.New(memory.NewSchema(fields...), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	if c.strategy == "sorted" {
		var cols []source.OrderingColumn
		for _, s := range f.query.GroupBy {
			cols = append(cols, source.OrderingColumn{Name: s})
		}
		f.src.WithOrdering(cols...)
	}
	return f
}
func prepareGroupBinding(tb testing.TB, c groupBindingCase, f groupBindingFixture) (*Engine, *PreparedQuery) {
	tb.Helper()
	opts := DefaultOptions()
	opts.Workers = 1
	opts.Pipeline.Workers = 1
	opts.PreparedCacheBytes = 0
	opts.KernelMode = c.mode
	opts.MemoryLimitBytes = 64 << 20
	if c.strategy == "spill" {
		opts.ExecutionPolicy = ExecutionBounded
		opts.Spill = SpillOptions{Directory: tb.TempDir(), MaxBytes: 64 << 20}
	}
	e := New(opts)
	p, err := e.Prepare(context.Background(), f.src, f.query, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		e.Close()
		tb.Fatal(err)
	}
	return e, p
}
func checkGroupBindingResult(tb testing.TB, c groupBindingCase, f groupBindingFixture, r TypedResult) {
	tb.Helper()
	if len(r.Rows) != len(f.want) || r.Summary.SelectedRows != f.selected {
		tb.Fatalf("groups=%d want=%d selected=%d want=%d", len(r.Rows), len(f.want), r.Summary.SelectedRows, f.selected)
	}
	seen := make(map[groupBindingKey]bool)
	for _, row := range r.Rows {
		if len(row.Keys) != c.keys {
			tb.Fatalf("key count %d", len(row.Keys))
		}
		var k groupBindingKey
		for lane, key := range row.Keys {
			v := key.Value
			if !v.Valid {
				continue
			}
			cell := groupBindingCell{valid: true}
			if n, ok := v.AsFloat64(); ok {
				cell.kind = 'n'
				cell.number = n
				if math.IsNaN(n) {
					cell.kind = 'N'
					cell.number = 0
				}
			} else if s, ok := v.AsString(); ok {
				cell.kind = 's'
				cell.text = s
			} else {
				tb.Fatalf("unexpected key %+v", v)
			}
			k[lane] = cell
		}
		w, ok := f.want[k]
		if !ok || seen[k] {
			tb.Fatalf("unexpected/duplicate key %+v", k)
		}
		seen[k] = true
		if len(row.Aggregates) != len(f.query.Aggregates) {
			tb.Fatal("aggregate count")
		}
		for i, want := range []uint64{w.count, w.values} {
			got, ok := row.Aggregates[i].Value.AsUint64()
			if !ok || got != want {
				tb.Fatalf("count got=%d want=%d", got, want)
			}
		}
		if !c.countOnly {
			if w.values == 0 {
				for _, a := range row.Aggregates[2:] {
					if a.Value.Valid {
						tb.Fatal("all-null sum is valid")
					}
				}
				continue
			}
			si, ok := row.Aggregates[2].Value.AsInt64()
			if !ok || si != w.signed {
				tb.Fatalf("signed sum=%d want=%d", si, w.signed)
			}
			ui, ok := row.Aggregates[3].Value.AsUint64()
			if !ok || ui != w.unsigned {
				tb.Fatalf("unsigned sum=%d want=%d", ui, w.unsigned)
			}
			fl, ok := row.Aggregates[4].Value.AsFloat64()
			if !ok || fl != w.floating {
				tb.Fatalf("float sum=%v want=%v", fl, w.floating)
			}
		}
	}
}
func verifyGroupBinding(tb testing.TB, c groupBindingCase, f groupBindingFixture, e *Engine, p *PreparedQuery) int64 {
	tb.Helper()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		tb.Fatal(err)
	}
	defer budget.Close()
	r, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
	if err != nil {
		tb.Fatal(err)
	}
	checkGroupBindingResult(tb, c, f, r)
	snap := budget.Snapshot()
	if snap.UsedBytes != 0 || e.Memory().Used() != 0 {
		tb.Fatalf("retained query=%d engine=%d", snap.UsedBytes, e.Memory().Used())
	}
	return snap.PeakBytes
}
func TestGroupBindingQueries(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		for _, batch := range []int{63, 64, 65, 1021} {
			for _, countOnly := range []bool{false, true} {
				c := groupBindingCase{3, 2*1021 + 65, batch, "mixed", "selective", strategy, countOnly, kernel.ModeAuto}
				t.Run(c.name(), func(t *testing.T) {
					f := makeGroupBindingFixture(t, c)
					e, p := prepareGroupBinding(t, c, f)
					defer e.Close()
					defer p.Close()
					verifyGroupBinding(t, c, f, e, p)
					verifyGroupBinding(t, c, f, e, p)
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					if _, err := p.ExecuteTyped(ctx, ExecuteOptions{Materialize: true}); err == nil {
						t.Fatal("cancelled execution succeeded")
					}
					if e.Memory().Used() != 0 {
						t.Fatal("cancelled query retained memory")
					}
				})
			}
		}
	}
	for _, pattern := range []string{"dense", "nullable", "empty"} {
		c := groupBindingCase{8, 1021 + 65, 65, "mixed", pattern, "bitmap-direct", false, kernel.ModeScalar}
		t.Run(c.name(), func(t *testing.T) {
			f := makeGroupBindingFixture(t, c)
			e, p := prepareGroupBinding(t, c, f)
			defer e.Close()
			defer p.Close()
			verifyGroupBinding(t, c, f, e, p)
		})
	}
}

var groupBindingResultSink TypedResult

func BenchmarkGroupBindingQueries(b *testing.B) {
	cases := []groupBindingCase{
		{1, 32771, 1021, "numeric", "dense", "bitmap-direct", false, kernel.ModeAuto},
		{3, 32771, 1021, "mixed", "nullable", "bitmap-direct", false, kernel.ModeAuto},
		{8, 32771, 8192, "mixed", "selective", "bitmap-direct", false, kernel.ModeAuto},
		{1, 32771, 8192, "string", "nullable", "bitmap-direct", true, kernel.ModeAuto},
		{3, 262147, 1021, "dictionary", "selective", "bitmap-direct", false, kernel.ModeAuto},
		{8, 262147, 8192, "numeric", "dense", "partitioned-hash", false, kernel.ModeAuto},
	}
	for _, base := range cases {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
			c := base
			c.mode = mode
			b.Run(c.name(), func(b *testing.B) {
				f := makeGroupBindingFixture(b, c)
				e, p := prepareGroupBinding(b, c, f)
				defer e.Close()
				defer p.Close()
				peak := verifyGroupBinding(b, c, f, e, p)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
					if err != nil {
						b.Fatal(err)
					}
					groupBindingResultSink = r
				}
				b.StopTimer()
				b.ReportMetric(float64(peak), "peak-charged-B")
				if e.Memory().Used() != 0 {
					b.Fatal("benchmark retained charged memory")
				}
			})
		}
	}
}

// The stream overwrites the same vector objects and backing buffers at every
// Next. Bindings must be rebuilt even when the Batch and vectors retain their
// identities, and dictionary bytes must not escape into retained group state.
type groupBindingReuseSource struct {
	source.Source
	fail bool
}

func (s groupBindingReuseSource) Ordering() []source.OrderingColumn {
	return s.Source.(source.OrderingProvider).Ordering()
}
func (s groupBindingReuseSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	st, err := s.Source.NewBatchStream(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &groupBindingReuseStream{inner: st, raw: memory.NewBatch(), buf: memory.NewBatch(), fail: s.fail}, nil
}
func (s groupBindingReuseSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	st, err := s.Source.(source.PreparedBatchStreamProvider).NewBatchStreamWithPartitions(ctx, opts, parts)
	if err != nil {
		return nil, err
	}
	return &groupBindingReuseStream{inner: st, raw: memory.NewBatch(), buf: memory.NewBatch(), fail: s.fail}, nil
}

type groupBindingReuseStream struct {
	inner      source.BatchStream
	raw, buf   *memory.Batch
	fail, read bool
}

func (s *groupBindingReuseStream) Close() error { return s.inner.Close() }
func (s *groupBindingReuseStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.fail && s.read {
		return false, fmt.Errorf("group binding injected source failure")
	}
	ok, err := s.inner.Next(ctx, s.raw)
	if err != nil || !ok {
		return ok, err
	}
	s.read = true
	b := s.buf
	b.Rows = s.raw.Rows
	for name, v := range s.raw.Float64s {
		d := b.Float64s[name]
		if d == nil {
			d = &memory.Float64Vector{}
			b.Float64s[name] = d
		}
		d.Values = append(d.Values[:0], v.Values...)
		d.Validity = append(d.Validity[:0], v.Validity...)
	}
	for name, v := range s.raw.Int64s {
		d := b.Int64s[name]
		if d == nil {
			d = &memory.Int64Vector{}
			b.Int64s[name] = d
		}
		d.Values = append(d.Values[:0], v.Values...)
		d.Validity = append(d.Validity[:0], v.Validity...)
	}
	for name, v := range s.raw.Uint64s {
		d := b.Uint64s[name]
		if d == nil {
			d = &memory.Uint64Vector{}
			b.Uint64s[name] = d
		}
		d.Values = append(d.Values[:0], v.Values...)
		d.Validity = append(d.Validity[:0], v.Validity...)
	}
	for name, v := range s.raw.Int32s {
		d := b.Int32s[name]
		if d == nil {
			d = &memory.Int32Vector{}
			b.Int32s[name] = d
		}
		d.Values = append(d.Values[:0], v.Values...)
		d.Validity = append(d.Validity[:0], v.Validity...)
	}
	for name, v := range s.raw.Strings {
		d := b.Strings[name]
		if d == nil {
			d = &memory.StringViewVector{}
			b.Strings[name] = d
		}
		d.Offsets = append(d.Offsets[:0], v.Offsets...)
		d.Data = append(d.Data[:0], v.Data...)
		d.Validity = append(d.Validity[:0], v.Validity...)
	}
	for name, v := range s.raw.DictStrings {
		d := b.DictStrings[name]
		if d == nil {
			d = &memory.DictionaryStringVector{}
			b.DictStrings[name] = d
		}
		d.Indices = append(d.Indices[:0], v.Indices...)
		d.Offsets = append(d.Offsets[:0], v.Offsets...)
		d.Data = append(d.Data[:0], v.Data...)
		d.Validity = append(d.Validity[:0], v.Validity...)
	}
	*dst = *b
	return true, nil
}

func TestGroupBindingReuseAndClose(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fail=%t", strategy, fail), func(t *testing.T) {
				c := groupBindingCase{3, 4097, 65, "mixed", "nullable", strategy, false, kernel.ModeAuto}
				f := makeGroupBindingFixture(t, c)
				opts := DefaultOptions()
				opts.Workers = 1
				opts.Pipeline.Workers = 1
				opts.PreparedCacheBytes = 0
				opts.MemoryLimitBytes = 8 << 20
				if strategy == "spill" {
					opts.ExecutionPolicy = ExecutionBounded
					opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
				}
				e := New(opts)
				defer e.Close()
				p, err := e.Prepare(context.Background(), groupBindingReuseSource{f.src, fail}, f.query, PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				if !fail {
					verifyGroupBinding(t, c, f, e, p)
				}
				fields := append([]memory.Field(nil), f.src.Schema().Fields[4:]...)
				for i := range fields {
					fields[i].Encoding = memory.EncodingPlain
				}
				fields = append(fields, memory.Field{Name: "n", Type: memory.TypeUint64}, memory.Field{Name: "valid_n", Type: memory.TypeUint64}, memory.Field{Name: "signed", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "unsigned", Type: memory.TypeUint64, Nullable: true}, memory.Field{Name: "floating", Type: memory.TypeFloat64, Nullable: true})
				schema := memory.NewSchema(fields...)
				budget, err := e.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx := memory.WithQueryBudget(context.Background(), budget)
				var st source.BatchStream
				switch strategy {
				case "sorted":
					st, err = p.NewSortedAggregateStream(ctx, schema, 1)
				case "spill":
					st, err = p.NewSpillableAggregateStream(ctx, schema, 1)
				default:
					st, err = p.NewResidentAggregateStream(ctx, schema, 1)
				}
				if err != nil {
					t.Fatal(err)
				}
				if fail {
					for {
						ok, nextErr := st.Next(ctx, memory.NewBatch())
						if nextErr != nil {
							break
						}
						if !ok {
							t.Fatal("source failure was lost")
						}
					}
				} else {
					if ok, err := st.Next(ctx, memory.NewBatch()); err != nil || !ok {
						t.Fatalf("first batch: ok=%t err=%v", ok, err)
					}
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				if budget.Used() != 0 || e.Memory().Used() != 0 {
					t.Fatalf("close retained query=%d engine=%d", budget.Used(), e.Memory().Used())
				}
			})
		}
	}
}
