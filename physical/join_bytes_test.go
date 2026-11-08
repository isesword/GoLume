package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"strings"
	"testing"
)

type genericJoinRow struct {
	s, x string
	n    int64
	u    uint32
	null int
}

func genericJoinSource(tb testing.TB, rows []genericJoinRow, dict bool, chunk int) *inmemory.Source {
	tb.Helper()
	sf := memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}
	var offsets = []uint32{0}
	var data []byte
	codes := map[string]uint32{}
	for _, r := range rows {
		if _, ok := codes[r.s]; !ok {
			codes[r.s] = uint32(len(codes))
			data = append(data, r.s...)
			offsets = append(offsets, uint32(len(data)))
		}
	}
	if dict {
		sf.Encoding = memory.EncodingDictionary
		sf.DictionaryDomain = "test-domain"
	}
	schema := memory.NewSchema(sf, memory.Field{Name: "x", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}, memory.Field{Name: "n", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "u", Type: memory.TypeUint32, Nullable: true}, memory.Field{Name: "row", Type: memory.TypeInt64})
	var batches []*memory.Batch
	for start := 0; start < len(rows); start += chunk {
		end := minInt(start+chunk, len(rows))
		b := memory.NewBatch()
		b.Rows = end - start
		sv := &memory.StringViewVector{Offsets: []uint32{0}, Validity: memory.EnsureBitmap(nil, b.Rows), Encoding: memory.EncodingView}
		xv := &memory.StringViewVector{Offsets: []uint32{0}, Validity: memory.EnsureBitmap(nil, b.Rows), Encoding: memory.EncodingView}
		nv := &memory.Int64Vector{Validity: memory.EnsureBitmap(nil, b.Rows)}
		uv := &memory.Uint32Vector{Validity: memory.EnsureBitmap(nil, b.Rows)}
		dv := &memory.DictionaryStringVector{Offsets: offsets, Data: data, DictionaryDomain: "test-domain", Encoding: memory.EncodingDictionary, Validity: sv.Validity}
		for i, r := range rows[start:end] {
			sv.Data = append(sv.Data, r.s...)
			sv.Offsets = append(sv.Offsets, uint32(len(sv.Data)))
			xv.Data = append(xv.Data, r.x...)
			xv.Offsets = append(xv.Offsets, uint32(len(xv.Data)))
			nv.Values = append(nv.Values, r.n)
			uv.Values = append(uv.Values, r.u)
			dv.Indices = append(dv.Indices, codes[r.s])
			for j, v := range []memory.Bitmap{sv.Validity, xv.Validity, nv.Validity, uv.Validity} {
				if r.null != j+1 {
					memory.Set(v, i)
				}
			}
		}
		if dict {
			b.DictStrings["s"] = dv
		} else {
			b.Strings["s"] = sv
		}
		b.Strings["x"] = xv
		b.Int64s["n"] = nv
		b.Uint32s["u"] = uv
		b.Int64s["row"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
		for i := 0; i < b.Rows; i++ {
			b.Int64s["row"].Values[i] = int64(start + i)
		}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		tb.Fatal(err)
	}
	if dict {
		src.WithDictionary("s", source.Dictionary{Domain: "test-domain", Offsets: offsets, Data: data})
	}
	return src
}

func genericJoinExpected(left, right []genericJoinRow, keys int, typ join.Type) [][2]int64 {
	var pairs [][2]int64
	matched := make([]bool, len(right))
	match := func(l, r genericJoinRow) bool {
		if (l.null > 0 && l.null <= keys) || (r.null > 0 && r.null <= keys) {
			return false
		}
		return l.s == r.s && (keys < 2 || l.x == r.x) && (keys < 3 || l.n == r.n) && (keys < 4 || l.u == r.u)
	}
	for li, l := range left {
		found := false
		for ri := len(right) - 1; ri >= 0; ri-- {
			if !match(l, right[ri]) {
				continue
			}
			found = true
			matched[ri] = true
			if typ == join.Anti {
				continue
			}
			pairs = append(pairs, [2]int64{int64(li), int64(ri)})
			if typ == join.Semi {
				break
			}
		}
		if !found && (typ == join.Left || typ == join.Full || typ == join.Anti) {
			pairs = append(pairs, [2]int64{int64(li), -1})
		}
	}
	if typ == join.Right || typ == join.Full {
		for ri, ok := range matched {
			if !ok {
				pairs = append(pairs, [2]int64{-1, int64(ri)})
			}
		}
	}
	if typ == join.Semi || typ == join.Anti {
		for i := range pairs {
			pairs[i][1] = -1
		}
	}
	return pairs
}

func collectGenericJoin(t *testing.T, s *JoinSource) ([][2]int64, []*memory.Batch) {
	t.Helper()
	ctx := context.Background()
	stream, err := source.OpenBatchStream(ctx, s, source.ReadOptions{Kernels: s.kernels})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var pairs [][2]int64
	var batches []*memory.Batch
	for {
		b := memory.NewBatch()
		ok, err := stream.Next(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		snapshot, err := cloneBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, snapshot)
		for i := 0; i < b.Rows; i++ {
			l, r := int64(-1), int64(-1)
			if v := b.Int64s["row"]; v.IsValid(i) {
				l = v.Values[i]
			}
			if v := b.Int64s["row_right"]; v != nil && v.IsValid(i) {
				r = v.Values[i]
			}
			pairs = append(pairs, [2]int64{l, r})
		}
	}
	return pairs, batches
}

func TestGenericJoinStringsCompositeAndAllModes(t *testing.T) {
	left := []genericJoinRow{{"a", "bc", 1, 7, 0}, {"ab", "c", 1, 7, 0}, {"", "", 0, 0, 0}, {"汉🙂\x00", "x", 2, 9, 0}, {"a", "bc", 1, 7, 3}, {"missing", "", 0, 0, 0}, {"a", "bc", 1, 7, 1}}
	right := []genericJoinRow{{"a", "bc", 1, 7, 0}, {"a", "bc", 1, 7, 0}, {"ab", "c", 1, 7, 0}, {"", "", 0, 0, 0}, {"汉🙂\x00", "x", 2, 9, 0}, {"汉🙂\x00", "x", 2, 8, 0}, {"a", "bc", 9, 7, 0}, {"a", "bc", 1, 7, 1}, {"right only", "", 0, 0, 0}}
	names := []string{"s", "x", "n", "u"}
	for keys := 1; keys <= 4; keys++ {
		for _, leftDict := range []bool{false, true} {
			for _, rightDict := range []bool{false, true} {
				if keys == 1 && leftDict && rightDict {
					continue
				} // Existing dictionary fast path has independent tests.
				for typ := join.Inner; typ <= join.Full; typ++ {
					t.Run(fmt.Sprintf("k%d/ld%t/rd%t/%s", keys, leftDict, rightDict, typ), func(t *testing.T) {
						ls, rs := genericJoinSource(t, left, leftDict, 2), genericJoinSource(t, right, rightDict, 3)
						ks := make([]JoinKey, keys)
						for i := range ks {
							ks[i] = JoinKey{names[i], names[i]}
						}
						manager := memory.NewBudgetManager(32 << 20)
						js, err := NewJoinSource(context.Background(), ls, rs, typ, ks, "", manager, kernel.New(kernel.ModeScalar))
						if err != nil {
							t.Fatal(err)
						}
						if js.Strategy() != "canonical-bytes-hash" {
							t.Fatal(js.Strategy())
						}
						got, _ := collectGenericJoin(t, js)
						want := genericJoinExpected(left, right, keys, typ)
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("got=%v want=%v", got, want)
						}
						if err := js.Close(); err != nil {
							t.Fatal(err)
						}
						if manager.Used() != 0 {
							t.Fatalf("leaked %d", manager.Used())
						}
					})
				}
			}
		}
	}
}

func TestGenericJoinRejectsUnsafeStrategiesBeforeReading(t *testing.T) {
	base := genericJoinSource(t, []genericJoinRow{{s: "x"}}, false, 1)
	for _, opts := range []JoinSourceOptions{{Spillable: true, SpillConfigured: true}, {RequireBounded: true}, {RadixBits: 1}} {
		opts.Kernels = kernel.New(kernel.ModeScalar)
		left, right := &countingSource{Source: base}, &countingSource{Source: base}
		_, err := NewJoinSourceWithOptions(context.Background(), left, right, join.Inner, []JoinKey{{"s", "s"}}, "", nil, opts)
		if err == nil || !strings.Contains(err.Error(), "MemoryOnly") {
			t.Fatalf("err=%v", err)
		}
		if left.reads != 0 || right.reads != 0 {
			t.Fatal("unsupported strategy read input")
		}
	}
}

func TestGenericJoinBudgetRetryAndCancellation(t *testing.T) {
	rows := []genericJoinRow{{s: strings.Repeat("a", 4096)}, {s: "b"}}
	src := genericJoinSource(t, rows, false, 1)
	ctx := context.Background()
	kernels := kernel.New(kernel.ModeScalar)
	for _, limit := range []int64{1, 5000, 9500} {
		manager := memory.NewBudgetManager(limit)
		js, err := NewJoinSource(ctx, src, src, join.Inner, []JoinKey{{"s", "s"}}, "", manager, kernels)
		if js != nil {
			js.Close()
			t.Fatalf("limit %d unexpectedly succeeded", limit)
		}
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("limit %d: %v", limit, err)
		}
		if manager.Used() != 0 {
			t.Fatalf("limit %d leaked %d", limit, manager.Used())
		}
	}
	manager := memory.NewBudgetManager(1 << 20)
	js, err := NewJoinSource(ctx, src, src, join.Inner, []JoinKey{{"s", "s"}}, "", manager, kernels)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := collectGenericJoin(t, js)
	if len(got) != 2 {
		t.Fatal(got)
	}
	js.Close()
	if manager.Used() != 0 {
		t.Fatal(manager.Used())
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = NewJoinSource(ctx, src, src, join.Inner, []JoinKey{{"s", "s"}}, "", manager, kernels)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatal(manager.Used())
	}
}

func TestGenericJoinCanonicalLengthAndTypes(t *testing.T) {
	rows := []genericJoinRow{{"a", "bc", 1, 1, 0}, {"ab", "c", 1, 1, 0}, {"a\x00", "bc", 1, 1, 0}}
	src := genericJoinSource(t, rows, false, 3)
	js, err := NewJoinSource(context.Background(), src, src, join.Inner, []JoinKey{{"s", "s"}, {"x", "x"}, {"n", "n"}}, "", nil, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	got, _ := collectGenericJoin(t, js)
	if len(got) != 3 {
		t.Fatal(got)
	}
	for _, fields := range [][2]memory.Field{
		{{Type: memory.TypeInt64}, {Type: memory.TypeUint64}},
		{{Type: memory.TypeTimestamp, ArrowType: "timestamp[ms]"}, {Type: memory.TypeTimestamp, ArrowType: "timestamp[us]"}},
		{{Type: memory.TypeFloat64}, {Type: memory.TypeFloat64}},
	} {
		if err := validateJoinKeyFields(fields[0], fields[1]); err == nil {
			t.Fatal("invalid fields accepted", fields)
		}
	}
}

func TestGenericJoinOutputOwnsPlainBytes(t *testing.T) {
	makeSource := func(value string) (*inmemory.Source, *memory.Batch) {
		b := memory.NewBatch()
		b.Rows = 1
		b.Strings["key"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(value))}, Data: []byte(value), Encoding: memory.EncodingView}
		src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingView}), b)
		if err != nil {
			t.Fatal(err)
		}
		return src, b
	}
	left, lb := makeSource("key")
	right, rb := makeSource("key")
	kernels := kernel.New(kernel.ModeScalar)
	js, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{"key", "key"}}, "", nil, kernels)
	if err != nil {
		t.Fatal(err)
	}
	// Retained build payload and its hash keys cannot alias reader-owned bytes.
	copy(rb.Strings["key"].Data, "bad")
	stream, err := source.OpenBatchStream(context.Background(), js, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	output := memory.NewBatch()
	ok, err := stream.Next(context.Background(), output)
	if err != nil || !ok || output.Rows != 1 {
		t.Fatalf("ok=%t rows=%d err=%v", ok, output.Rows, err)
	}
	copy(lb.Strings["key"].Data, "bad")
	output, err = cloneBatch(output)
	if err != nil {
		t.Fatal(err)
	}
	stream.Close()
	js.Close()
	for _, name := range []string{"key", "key_right"} {
		if got := string(output.Strings[name].ValueBytes(0)); got != "key" {
			t.Fatalf("%s aliases input: %q", name, got)
		}
	}
}

func TestGenericJoinMalformedStringKeyIsNotEmpty(t *testing.T) {
	for _, b := range []*memory.Batch{
		{Strings: map[string]*memory.StringViewVector{"s": {Offsets: []uint32{2, 1}, Data: []byte("x")}}},
		{Strings: map[string]*memory.StringViewVector{"s": {Offsets: []uint32{0, 2}, Data: []byte("x")}}},
		{DictStrings: map[string]*memory.DictionaryStringVector{"s": {Indices: []uint32{2}, Offsets: []uint32{0, 1}, Data: []byte("x")}}},
	} {
		if _, _, err := joinKeyString(b, "s", 0); err == nil {
			t.Fatal("malformed string silently encoded as empty")
		}
	}
}

func TestGenericJoinThreeIntegerKeys(t *testing.T) {
	rows := []genericJoinRow{{n: -1, u: 1}, {n: 0, u: 0}, {n: 1, u: 2}}
	src := genericJoinSource(t, rows, false, 2)
	js, err := NewJoinSource(context.Background(), src, src, join.Inner, []JoinKey{{"n", "n"}, {"u", "u"}, {"row", "row"}}, "", nil, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	got, _ := collectGenericJoin(t, js)
	if want := [][2]int64{{0, 0}, {1, 1}, {2, 2}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}

func TestGenericJoinProbeBudgetFailureAndRetry(t *testing.T) {
	rows := []genericJoinRow{{s: strings.Repeat("z", 1024)}}
	src := genericJoinSource(t, rows, false, 1)
	manager := memory.NewBudgetManager(1 << 20)
	ctx := context.Background()
	kernels := kernel.New(kernel.ModeScalar)
	js, err := NewJoinSource(ctx, src, src, join.Inner, []JoinKey{{"s", "s"}}, "", manager, kernels)
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	base := manager.Used()
	hold, err := manager.Reserve(ctx, manager.Limit()-base-3000)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(ctx, js, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Next(ctx, memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("expected output reservation failure: %v", err)
	}
	stream.Close()
	hold.Release()
	if manager.Used() != base {
		t.Fatalf("failed probe leaked %d", manager.Used()-base)
	}
	got, _ := collectGenericJoin(t, js)
	if len(got) != 1 {
		t.Fatal(got)
	}
	if manager.Used() != base {
		t.Fatalf("retry leaked %d", manager.Used()-base)
	}
}

func TestGenericJoinDictionaryOutputOwnsBytes(t *testing.T) {
	left := genericJoinSource(t, []genericJoinRow{{s: "key"}}, false, 1)
	right := genericJoinSource(t, []genericJoinRow{{s: "key"}}, true, 1)
	manager := memory.NewBudgetManager(1 << 20)
	js, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{"s", "s"}}, "", manager, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	dictionary, err := right.Dictionary(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	copy(dictionary.Data, "bad")
	got, batches := collectGenericJoin(t, js)
	if len(got) != 1 {
		t.Fatal(got)
	}
	js.Close()
	if manager.Used() != 0 {
		t.Fatal(manager.Used())
	}
	if value := string(batches[0].DictStrings["s_right"].DictionaryValue(0)); value != "key" {
		t.Fatalf("output dictionary borrowed source bytes: %q", value)
	}
}
