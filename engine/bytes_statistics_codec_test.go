package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func bytesCodecFixture(t *testing.T) ([]memory.DataType, []aggregate.StatisticsLayout, *aggregate.StatisticsBundle) {
	t.Helper()
	ts := []memory.DataType{memory.TypeString, memory.TypeUint64, memory.TypeBinary, memory.TypeFloat64}
	ls := []aggregate.StatisticsLayout{{First: true, Last: true}, {First: true}, {Last: true}, {Moments: true}}
	mgr := memory.NewBudgetManager(1 << 20)
	b, err := aggregate.NewStatisticsBundle(ts, ls, func(n int64) (memory.Reservation, error) { return mgr.Reserve(context.Background(), n) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b.Close()
		if mgr.Used() != 0 {
			t.Fatal("bundle leaked")
		}
	})
	if err = b.EnsureGroups(3); err != nil {
		t.Fatal(err)
	}
	for g := 0; g < 3; g++ {
		for i := 0; i < 3; i++ {
			for lane, typ := range ts {
				ordinal := aggregate.SourceOrdinal{Partition: uint64(g), Row: uint64(i)}
				if byteStatisticsType(typ) {
					value := []byte{0, 255, byte(g), byte(i)}
					if g == 1 {
						value = nil
					}
					err = b.AddBytes(g, lane, value, g != 2, ordinal)
				} else {
					err = b.AddBits(g, lane, uint64(i+1), g != 2, ordinal)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return ts, ls, b
}
func TestBytesStatisticsCodecRoundTrip(t *testing.T) {
	ts, ls, b := bytesCodecFixture(t)
	nt, nl, nb := numericCodecFixture(t)
	for _, mixed := range []bool{false, true} {
		for _, shape := range []GroupShape{GroupShapeI32, GroupShapePairFixed, GroupShapeStringView, GroupShapeGeneric} {
			nts, nls := nt, nl
			if !mixed {
				nts = nil
				nls = nil
			}
			mgr := memory.NewBudgetManager(1 << 20)
			visit := func(fn func(aggregateRecord) error) error {
				for g := 0; g < 3; g++ {
					if err := fn(aggregateRecord{a: uint64(g), b: 9, validMask: 1, stringKey: []byte("key"), groupID: g, statisticsBundle: b, numericBundle: nb}); err != nil {
						return err
					}
				}
				return nil
			}
			run, err := encodeBytesStatisticsAggregateRun(context.Background(), shape, nts, nls, ts, ls, visit, nil, mgr)
			if err != nil {
				t.Fatal(err)
			}
			if mgr.Used() != int64(len(run.payload)) {
				t.Fatal("not exactly reserved")
			}
			r, err := newBytesStatisticsAggregateRunReader(run.payload, shape, nts, nls, ts, ls)
			if err != nil {
				t.Fatal(err)
			}
			var first aggregateRecord
			g := 0
			if err = r.visit(func(rec aggregateRecord) error {
				if rec.bytesStatisticsRaw == nil || rec.statisticsBundle != nil {
					t.Fatal("not borrowed")
				}
				if g == 0 {
					first = rec
				}
				for lane, typ := range ts {
					if byteStatisticsType(typ) {
						if !reflect.DeepEqual(bytesAggregateRecordState(rec, lane, ts, ls), b.BytesResult(g, lane)) { // nil and empty slices are both valid zero-length wire views.
							got, want := bytesAggregateRecordState(rec, lane, ts, ls), b.BytesResult(g, lane)
							got.First = nil
							got.Last = nil
							want.First = nil
							want.Last = nil
							if !reflect.DeepEqual(got, want) {
								t.Fatalf("byte state %v %v", got, want)
							}
							if string(bytesAggregateRecordState(rec, lane, ts, ls).First) != string(b.BytesResult(g, lane).First) || string(bytesAggregateRecordState(rec, lane, ts, ls).Last) != string(b.BytesResult(g, lane).Last) {
								t.Fatal("payload mismatch")
							}
						}
					} else if statisticsAggregateRecordState(rec, lane, typ, ls[lane]) != b.Result(g, lane) {
						t.Fatal("native mismatch")
					}
				}
				if mixed {
					for lane, typ := range nt {
						assertNumericCodecState(t, numericAggregateRecordState(rec, lane, typ, nl[lane]), nb.Result(g, lane))
					}
				}
				g++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if g != 3 || string(bytesAggregateRecordState(first, 0, ts, ls).First) != string(b.BytesResult(0, 0).First) {
				t.Fatal("borrowed lifetime changed on next")
			}
			// Re-encoding borrowed frames exercises native scans past variable-width lanes.
			visitRaw := func(fn func(aggregateRecord) error) error {
				fresh, e := newBytesStatisticsAggregateRunReader(run.payload, shape, nts, nls, ts, ls)
				if e != nil {
					return e
				}
				return fresh.visit(fn)
			}
			copyRun, e := encodeBytesStatisticsAggregateRun(context.Background(), shape, nts, nls, ts, ls, visitRaw, nil, mgr)
			if e != nil {
				t.Fatal(e)
			}
			if string(copyRun.payload) != string(run.payload) {
				t.Fatal("raw reencode changed bytes")
			}
			copyRun.close()
			for cut := 0; cut < len(run.payload); cut++ {
				reader, e := newBytesStatisticsAggregateRunReader(run.payload[:cut], shape, nts, nls, ts, ls)
				if e == nil {
					e = reader.visit(func(aggregateRecord) error { return nil })
				}
				if !errors.Is(e, memory.ErrSpillCorrupt) {
					t.Fatalf("prefix %d: %v", cut, e)
				}
			}
			run.close()
			if mgr.Used() != 0 {
				t.Fatal("run leaked")
			}
		}
	}
}
func TestBytesStatisticsCodecCorruptionAndResources(t *testing.T) {
	ts, ls, b := bytesCodecFixture(t)
	mgr := memory.NewBudgetManager(1 << 20)
	visit := func(fn func(aggregateRecord) error) error {
		return fn(aggregateRecord{validMask: 1, statisticsBundle: b})
	}
	run, err := encodeBytesStatisticsAggregateRun(context.Background(), GroupShapeI32, nil, nil, ts, ls, visit, nil, mgr)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	base := bytesStatisticsAggregateRunHeaderLen + 2*len(ts) + 9
	mutations := []func([]byte){
		func(d []byte) { d[8] = 4 }, func(d []byte) { d[11] = 1 }, func(d []byte) { d[24] = byte(memory.TypeBinary) },
		func(d []byte) { d[base] = 2 }, func(d []byte) { d[base+1] = 2 }, func(d []byte) { d[base+2] = 2 },
		func(d []byte) { binary.LittleEndian.PutUint64(d[base+3:], 1) }, // first ordinal after last
		func(d []byte) { binary.LittleEndian.PutUint32(d[base+35:], ^uint32(0)) },
		func(d []byte) { binary.LittleEndian.PutUint32(d[base+39:], ^uint32(0)) },
		func(d []byte) { d[base+1] = 0 }, // NULL with payload
		func(d []byte) { d[base] = 0 },   // unseen has nonzero endpoint state
	}
	for i, mutate := range mutations {
		d := append([]byte(nil), run.payload...)
		mutate(d)
		r, e := newBytesStatisticsAggregateRunReader(d, GroupShapeI32, nil, nil, ts, ls)
		if e == nil {
			e = r.visit(func(aggregateRecord) error { return nil })
		}
		if !errors.Is(e, memory.ErrSpillCorrupt) {
			t.Fatalf("mutation %d: %v", i, e)
		}
	}
	d := append(append([]byte(nil), run.payload...), 0)
	r, e := newBytesStatisticsAggregateRunReader(d, GroupShapeI32, nil, nil, ts, ls)
	if e == nil {
		e = r.visit(func(aggregateRecord) error { return nil })
	}
	if !errors.Is(e, memory.ErrSpillCorrupt) {
		t.Fatal("trailing accepted", e)
	}
	tiny := memory.NewBudgetManager(1)
	if _, e := encodeBytesStatisticsAggregateRun(context.Background(), GroupShapeI32, nil, nil, ts, ls, visit, nil, tiny); e == nil || tiny.Used() != 0 {
		t.Fatal("quota/leak", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := encodeBytesStatisticsAggregateRun(ctx, GroupShapeI32, nil, nil, ts, ls, visit, nil, mgr); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	ctx, cancel = context.WithCancel(context.Background())
	calls := 0
	before := mgr.Used()
	cancelling := func(fn func(aggregateRecord) error) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return visit(fn)
	}
	if _, e := encodeBytesStatisticsAggregateRun(ctx, GroupShapeI32, nil, nil, ts, ls, cancelling, nil, mgr); !errors.Is(e, context.Canceled) || mgr.Used() != before {
		t.Fatal("cancel leaked", e)
	}
	if _, e := newStatisticsAggregateRunReader(run.payload, GroupShapeI32, nil, nil, ts, ls); !errors.Is(e, memory.ErrSpillCorrupt) {
		t.Fatal("V4 accepted variable descriptors", e)
	}
}

func TestBytesStatisticsReaderNoRecordAllocations(t *testing.T) {
	ts, ls, b := bytesCodecFixture(t)
	mgr := memory.NewBudgetManager(1 << 20)
	visit := func(fn func(aggregateRecord) error) error {
		return fn(aggregateRecord{validMask: 1, statisticsBundle: b})
	}
	run, err := encodeBytesStatisticsAggregateRun(context.Background(), GroupShapeI32, nil, nil, ts, ls, visit, nil, mgr)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	reader, err := newBytesStatisticsAggregateRunReader(run.payload, GroupShapeI32, nil, nil, ts, ls)
	if err != nil {
		t.Fatal(err)
	}
	initial := *reader
	if n := testing.AllocsPerRun(100, func() {
		*reader = initial
		record, ok, e := reader.next()
		if e != nil || !ok {
			panic("read failed")
		}
		for i, typ := range ts {
			if byteStatisticsType(typ) {
				_ = bytesAggregateRecordState(record, i, ts, ls)
			} else {
				_ = statisticsAggregateRecordState(record, i, typ, ls[i])
			}
		}
	}); n != 0 {
		t.Fatalf("record allocated %g times", n)
	}
}
