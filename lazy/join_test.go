package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"sort"
	"testing"
)

func TestLazyJoinFixedWidthSemantics(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 2, 3})
	right := makeI32Source(t, []int32{2, 2, 4})
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	for _, tc := range []struct {
		typ  JoinType
		want []string
	}{
		{InnerJoin, []string{"2/2", "2/2", "2/2", "2/2"}},
		{LeftJoin, []string{"1/NULL", "2/2", "2/2", "2/2", "2/2", "3/NULL"}},
		{RightJoin, []string{"2/2", "2/2", "2/2", "2/2", "NULL/4"}},
		{FullJoin, []string{"1/NULL", "2/2", "2/2", "2/2", "2/2", "3/NULL", "NULL/4"}},
		{SemiJoin, []string{"2", "2"}},
		{AntiJoin, []string{"1", "3"}},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			lf := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: tc.typ, LeftOn: []string{"key"}, RightOn: []string{"key"}})
			got, err := lf.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer got.Close()
			var rows []string
			for _, batch := range got.batches() {
				l := batch.Int32s["key"]
				r := batch.Int32s["key_right"]
				for row := 0; row < batch.Rows; row++ {
					lv := "NULL"
					if l.IsValid(row) {
						lv = fmt.Sprint(l.Values[row])
					}
					if tc.typ == SemiJoin || tc.typ == AntiJoin {
						if r != nil {
							t.Fatal("semi/anti join materialized right payload")
						}
						rows = append(rows, lv)
					} else {
						rv := "NULL"
						if r.IsValid(row) {
							rv = fmt.Sprint(r.Values[row])
						}
						rows = append(rows, lv+"/"+rv)
					}
				}
			}
			sort.Strings(rows)
			if got.Height() != int64(len(tc.want)) || !reflect.DeepEqual(rows, tc.want) {
				t.Fatalf("height=%d rows=%v want=%v", got.Height(), rows, tc.want)
			}
		})
	}
}

func TestLazyJoinCompositeNullNeverMatches(t *testing.T) {
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	leftBatch := memory.NewBatch()
	leftBatch.Rows = 3
	leftBatch.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 1, 2}, Validity: valid}
	leftBatch.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{9, 9, 8}}
	rightBatch := memory.NewBatch()
	rightBatch.Rows = 3
	rightBatch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 1, 2}}
	rightBatch.Uint32s["y"] = &memory.Uint32Vector{Values: []uint32{9, 7, 8}}
	ls, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeUint32}), leftBatch)
	rs, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "y", Type: memory.TypeUint32}), rightBatch)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	res, err := s.FromSource(ls).Join(s.FromSource(rs), JoinOptions{Type: InnerJoin, LeftOn: []string{"a", "b"}, RightOn: []string{"x", "y"}}).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if res.Height() != 2 {
		t.Fatalf("rows=%d want=2", res.Height())
	}
	if got := frameInt64Column(t, res, "a"); !equalI64(got, []int64{1, 2}) {
		t.Fatalf("matched keys=%v want=[1 2]", got)
	}
	for _, batch := range res.batches() {
		for row := 0; row < batch.Rows; row++ {
			if !batch.Int64s["a"].IsValid(row) {
				t.Fatal("NULL composite key matched")
			}
		}
	}
}

func makeI32Source(t *testing.T, keys []int32) *inmemory.Source {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = len(keys)
	b.Int32s["key"] = &memory.Int32Vector{Values: append([]int32(nil), keys...)}
	s, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}), b)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestLazyJoinDictionaryDomainsNormalizeOnce(t *testing.T) {
	leftDict := source.Dictionary{Domain: "left-domain", Offsets: []uint32{0, 1, 2}, Data: []byte("ab")}
	rightDict := source.Dictionary{Domain: "right-domain", Offsets: []uint32{0, 1, 2, 3}, Data: []byte("bac")}
	lb := memory.NewBatch()
	lb.Rows = 3
	lb.DictStrings["k"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0}, Offsets: leftDict.Offsets, Data: leftDict.Data, DictionaryDomain: leftDict.Domain, Encoding: memory.EncodingDictionary}
	rb := memory.NewBatch()
	rb.Rows = 3
	rb.DictStrings["k"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2}, Offsets: rightDict.Offsets, Data: rightDict.Data, DictionaryDomain: rightDict.Domain, Encoding: memory.EncodingDictionary}
	ls, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: leftDict.Domain}), lb)
	ls.WithDictionary("k", leftDict)
	rs, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: rightDict.Domain}), rb)
	rs.WithDictionary("k", rightDict)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	res, err := s.FromSource(ls).Join(s.FromSource(rs), JoinOptions{Type: InnerJoin, LeftOn: []string{"k"}, RightOn: []string{"k"}}).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if res.Height() != 3 {
		t.Fatalf("rows=%d want=3", res.Height())
	}
}

func TestPreparedJoinPipelineMemoryAccountingAndClose(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 3})
	right := makeI32Source(t, []int32{2, 3, 4})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	s := NewSession(opts)
	defer s.Close()
	p, err := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Close(); err != nil {
		t.Fatal(err)
	}
	if used := s.Engine().Memory().Used(); used <= 0 {
		t.Fatalf("join build was not memory-accounted: %d", used)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if used := s.Engine().Memory().Used(); used != 0 {
		t.Fatalf("memory after close=%d want=0", used)
	}
	if _, err := p.Execute(context.Background()); !errors.Is(err, ErrPreparedFrameClosed) {
		t.Fatalf("execute after close err=%v", err)
	}
}

func TestPreparedJoinPipelineBudgetFailureDoesNotPoisonRetry(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 3})
	right := makeI32Source(t, []int32{2, 3, 4})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	opts.ExecutionPolicy = engine.ExecutionMemoryOnly
	s := NewSession(opts)
	defer s.Close()
	p, err := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	manager := s.Engine().Memory()
	baseline := manager.Used()
	pressure, err := manager.Reserve(context.Background(), manager.Limit()-baseline)
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Execute(context.Background())
	pressure.Release()
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("err=%v want budget", err)
	}
	if used := manager.Used(); used != baseline {
		t.Fatalf("failed execution retained %d bytes, want prepared baseline %d", used, baseline)
	}
	result, err := p.Execute(context.Background())
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if result.Height() != 2 {
		t.Fatalf("retry rows=%d want=2", result.Height())
	}
	_ = result.Close()
}

func TestPreparedJoinPipelineCancellationDoesNotPoisonRetry(t *testing.T) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	left := makeI32Source(t, []int32{1, 2, 3})
	right := makeI32Source(t, []int32{2, 3, 4})
	p, err := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	baseline := s.Engine().Memory().Used()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution err=%v", err)
	}
	if used := s.Engine().Memory().Used(); used != baseline {
		t.Fatalf("canceled execution memory=%d want baseline=%d", used, baseline)
	}
	result, err := p.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 2 {
		t.Fatalf("retry rows=%d want=2", result.Height())
	}
}
