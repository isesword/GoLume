package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math"
	"testing"
)

func statisticsCodecFixture(t *testing.T) ([]memory.DataType, []aggregate.StatisticsLayout, *aggregate.StatisticsBundle) {
	t.Helper()
	ts := []memory.DataType{memory.TypeUint64, memory.TypeFloat64, memory.TypeBool, memory.TypeInt64}
	ls := []aggregate.StatisticsLayout{{Moments: true, First: true, Last: true}, {Moments: true, First: true, Last: true}, {First: true}, {Moments: true}}
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
			bits := []uint64{math.MaxUint64 - uint64(i), 0x7ff8000000000073, 1, math.MaxInt64 - uint64(i)}
			if g == 1 {
				bits[1] = 0x8000000000000000
			}
			for j, v := range bits {
				if err = b.AddBits(g, j, v, g != 2, aggregate.SourceOrdinal{Partition: uint64(g), Row: uint64(i)}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return ts, ls, b
}
func TestStatisticsAggregateCodecRoundTrip(t *testing.T) {
	ts, ls, b := statisticsCodecFixture(t)
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
					if err := fn(aggregateRecord{a: uint64(g), b: 9, validMask: 1, stringKey: []byte("canonical-key"), groupID: g, statisticsBundle: b, numericBundle: nb}); err != nil {
						return err
					}
				}
				return nil
			}
			run, err := encodeStatisticsAggregateRun(context.Background(), shape, nts, nls, ts, ls, visit, nil, mgr)
			if err != nil {
				t.Fatal(err)
			}
			if mgr.Used() != int64(len(run.payload)) {
				t.Fatal("payload not exactly reserved")
			}
			r, err := newStatisticsAggregateRunReader(run.payload, shape, nts, nls, ts, ls)
			if err != nil {
				t.Fatal(err)
			}
			g := 0
			if err = r.visit(func(rec aggregateRecord) error {
				if len(rec.statisticsRaw) != len(ts)*statisticsAggregateStateBytes || rec.statisticsBundle != nil {
					t.Fatal("not borrowed")
				}
				for lane, typ := range ts {
					got := statisticsAggregateRecordState(rec, lane, typ, ls[lane])
					want := b.Result(g, lane)
					if got != want {
						t.Fatalf("state mismatch %+v %+v", got, want)
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
			if g != 3 {
				t.Fatal(g)
			}
			// Every strict prefix is corrupt, including partial final raw state.
			for cut := 0; cut < len(run.payload); cut++ {
				rr, e := newStatisticsAggregateRunReader(run.payload[:cut], shape, nts, nls, ts, ls)
				if e == nil {
					e = rr.visit(func(aggregateRecord) error { return nil })
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
func TestStatisticsAggregateCodecCorruptAndResources(t *testing.T) {
	ts, ls, b := statisticsCodecFixture(t)
	mgr := memory.NewBudgetManager(1 << 20)
	visit := func(fn func(aggregateRecord) error) error {
		return fn(aggregateRecord{validMask: 1, statisticsBundle: b})
	}
	run, err := encodeStatisticsAggregateRun(context.Background(), GroupShapeI32, nil, nil, ts, ls, visit, nil, mgr)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	offset := statisticsAggregateRunHeaderLen + 2*len(ts) + 9
	mutations := []func([]byte){
		func(p []byte) { p[0] ^= 1 }, func(p []byte) { p[8] = 3 }, func(p []byte) { p[11] = 1 }, func(p []byte) { p[12] = 1 }, func(p []byte) { p[16] = 0 },
		func(p []byte) { binary.LittleEndian.PutUint32(p[20:24], math.MaxUint32) },
		func(p []byte) { p[24] = byte(memory.TypeFloat32) }, func(p []byte) { p[25] |= 128 },
		func(p []byte) { p[offset+40] = 4 }, func(p []byte) { p[offset+41] = 2 }, func(p []byte) { p[offset+90] = 2 },
		func(p []byte) { p[offset+91] = 2 },
		func(p []byte) { p[offset+41] = 0 },
		func(p []byte) { binary.LittleEndian.PutUint64(p[offset+42:], math.MaxUint64) },
		func(p []byte) { binary.LittleEndian.PutUint64(p[offset+50:], 3) },
		func(p []byte) { binary.LittleEndian.PutUint64(p[offset+66:], 0) },
		func(p []byte) { p[offset+90] = 0 },
		// Bool lane rejects noncanonical boolean bits and undeclared moments.
		func(p []byte) { p[offset+2*statisticsAggregateStateBytes+74] = 2 },
		func(p []byte) { p[offset+2*statisticsAggregateStateBytes] = 1 },
		// Moments-only lane rejects hidden endpoint bytes.
		func(p []byte) { p[offset+3*statisticsAggregateStateBytes+41] = 1 },

		func(p []byte) { binary.LittleEndian.PutUint64(p[offset+24:], math.Float64bits(-1)) },
		func(p []byte) { binary.LittleEndian.PutUint64(p[offset:], 0) },
	}
	for i, mutate := range mutations {
		p := append([]byte(nil), run.payload...)
		mutate(p)
		r, e := newStatisticsAggregateRunReader(p, GroupShapeI32, nil, nil, ts, ls)
		if e == nil {
			e = r.visit(func(aggregateRecord) error { return nil })
		}
		if !errors.Is(e, memory.ErrSpillCorrupt) {
			t.Fatalf("mutation %d: %v", i, e)
		}
	}
	p := append(append([]byte(nil), run.payload...), 0)
	r, e := newStatisticsAggregateRunReader(p, GroupShapeI32, nil, nil, ts, ls)
	if e == nil {
		e = r.visit(func(aggregateRecord) error { return nil })
	}
	if !errors.Is(e, memory.ErrSpillCorrupt) {
		t.Fatal("trailing bytes accepted")
	}
	tiny := memory.NewBudgetManager(1)
	if _, err = encodeStatisticsAggregateRun(context.Background(), GroupShapeI32, nil, nil, ts, ls, visit, nil, tiny); err == nil || tiny.Used() != 0 {
		t.Fatal("failed reservation leaked/accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = encodeStatisticsAggregateRun(ctx, GroupShapeI32, nil, nil, ts, ls, visit, nil, mgr); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	used := mgr.Used()
	calls := 0
	changing := func(fn func(aggregateRecord) error) error {
		calls++
		if calls == 2 {
			return nil
		}
		return visit(fn)
	}
	if _, err = encodeStatisticsAggregateRun(context.Background(), GroupShapeI32, nil, nil, ts, ls, changing, nil, mgr); err == nil || mgr.Used() != used {
		t.Fatal("changed visitor accepted/leaked")
	}
}
func FuzzStatisticsAggregateCodec(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("GLAGG004"))
	seed := make([]byte, statisticsAggregateRunHeaderLen+2+9+statisticsAggregateStateBytes)
	copy(seed, statisticsAggregateRunMagic[:])
	binary.LittleEndian.PutUint16(seed[8:], statisticsAggregateRunVersion)
	seed[10] = byte(GroupShapeI32)
	binary.LittleEndian.PutUint32(seed[16:], 1)
	binary.LittleEndian.PutUint32(seed[20:], 1)
	seed[24] = byte(memory.TypeInt64)
	seed[25] = 7
	seed[26] = 1
	f.Add(seed)
	f.Fuzz(func(t *testing.T, p []byte) {
		r, err := newStatisticsAggregateRunReader(p, GroupShapeI32, nil, nil, []memory.DataType{memory.TypeInt64}, []aggregate.StatisticsLayout{{Moments: true, First: true, Last: true}})
		if err == nil {
			err = r.visit(func(aggregateRecord) error { return nil })
		}
		if err != nil && !errors.Is(err, memory.ErrSpillCorrupt) {
			t.Fatal(err)
		}
	})
}
