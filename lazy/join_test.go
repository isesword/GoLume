package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestLazyJoinFixedWidthSemantics(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 2, 3})
	right := makeI32Source(t, []int32{2, 2, 4})
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	for _, tc := range []struct {
		typ  JoinType
		want int
	}{{InnerJoin, 4}, {LeftJoin, 6}, {RightJoin, 5}, {FullJoin, 7}, {SemiJoin, 2}, {AntiJoin, 2}} {
		lf := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: tc.typ, LeftOn: []string{"key"}, RightOn: []string{"key"}})
		got, err := lf.CollectJoin(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", tc.typ, err)
		}
		if got.OutputRows != tc.want || len(got.Rows) != tc.want {
			t.Fatalf("%s rows=%d len=%d want=%d", tc.typ, got.OutputRows, len(got.Rows), tc.want)
		}
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
	res, err := s.FromSource(ls).Join(s.FromSource(rs), JoinOptions{Type: InnerJoin, LeftOn: []string{"a", "b"}, RightOn: []string{"x", "y"}}).CollectJoin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.OutputRows != 2 {
		t.Fatalf("rows=%d want=2", res.OutputRows)
	}
	for _, row := range res.Rows {
		if row.Left.Row == 1 {
			t.Fatal("NULL composite key matched")
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
	res, err := s.FromSource(ls).Join(s.FromSource(rs), JoinOptions{Type: InnerJoin, LeftOn: []string{"k"}, RightOn: []string{"k"}}).CollectJoin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.OutputRows != 3 {
		t.Fatalf("rows=%d want=3", res.OutputRows)
	}
}

func TestPreparedJoinMemoryAccountingAndClose(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 3})
	right := makeI32Source(t, []int32{2, 3, 4})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	s := NewSession(opts)
	defer s.Close()
	p, err := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).PrepareJoin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Execute(context.Background()); err != nil {
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
	if _, err := p.Execute(context.Background()); !errors.Is(err, ErrPreparedJoinClosed) {
		t.Fatalf("execute after close err=%v", err)
	}
}

func TestPreparedJoinBudgetFailureDoesNotPoisonRetry(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 3})
	right := makeI32Source(t, []int32{2, 3, 4})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 8
	s := NewSession(opts)
	defer s.Close()
	p, err := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).PrepareJoin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Execute(context.Background()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("err=%v want budget", err)
	}
	if used := s.Engine().Memory().Used(); used != 0 {
		t.Fatalf("failed build leaked %d bytes", used)
	}
}
