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

func numericCodecFixture(t *testing.T) ([]memory.DataType, []aggregate.Layout, *aggregate.NumericBundle) {
	t.Helper()
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeString, memory.TypeInt64}
	full := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	layouts := []aggregate.Layout{full, full, full, full, full, {CountRows: true, CountValues: true}, full}
	manager := memory.NewBudgetManager(1 << 20)
	bundle, err := aggregate.NewNumericBundle(types, layouts, func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bundle.Close()
		if manager.Used() != 0 {
			t.Fatalf("fixture retained %d bytes", manager.Used())
		}
	})
	if err := bundle.EnsureGroups(5); err != nil {
		t.Fatal(err)
	}
	for group := 0; group < 5; group++ {
		values := []uint64{uint64(0xfd), uint64(0xfffffffe), (uint64(1) << 63) + 13, uint64(math.Float32bits(-1.25)), math.Float64bits(1.25), 0, math.MaxInt64}
		switch group {
		case 2:
			values[3], values[4] = 0x7fc00073, 0x7ff8000000000073
		case 3:
			values[3], values[4] = uint64(math.Float32bits(float32(math.Inf(1)))), math.Float64bits(math.Inf(-1))
		case 4:
			values[3], values[4] = 0x80000000, 0x8000000000000000
		}
		for lane, bits := range values {
			bundle.AddBits(group, lane, bits, group != 1)
		}
	}
	// Force the legacy Int64 wrapping-mean behavior independently of other lanes.
	bundle.AddBits(0, 6, 1, true)
	return types, layouts, bundle
}

func assertNumericCodecState(t *testing.T, got, want aggregate.NumericResult) {
	t.Helper()
	gotAvg, wantAvg := math.Float64bits(got.AvgSum), math.Float64bits(want.AvgSum)
	got.AvgSum, want.AvgSum = 0, 0
	if got != want || gotAvg != wantAvg {
		t.Fatalf("state=%+v avgBits=%x want=%+v avgBits=%x", got, gotAvg, want, wantAvg)
	}
}

func TestNumericAggregateCodecMixedRoundTrip(t *testing.T) {
	types, layouts, bundle := numericCodecFixture(t)
	manager := memory.NewBudgetManager(1 << 20)
	visit := func(fn func(aggregateRecord) error) error {
		for group := 0; group < 5; group++ {
			if err := fn(aggregateRecord{a: uint64(group), validMask: 1, numericBundle: bundle, groupID: group}); err != nil {
				return err
			}
		}
		return nil
	}
	run, err := encodeNumericAggregateRun(context.Background(), GroupShapeI32, types, layouts, visit, nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	if manager.Used() != int64(len(run.payload)) {
		t.Fatalf("reservation=%d payload=%d", manager.Used(), len(run.payload))
	}
	reader, err := newNumericAggregateRunReader(run.payload, GroupShapeI32, types, layouts)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	err = reader.visit(func(record aggregateRecord) error {
		group := int(record.a)
		if record.numericBundle != nil || len(record.numericRaw) != len(types)*numericAggregateStateBytes {
			t.Fatal("record did not borrow raw numeric state")
		}
		for lane, typ := range types {
			assertNumericCodecState(t, numericAggregateRecordState(record, lane, typ, layouts[lane]), bundle.Result(group, lane))
		}
		seen++
		return nil
	})
	if err != nil || seen != 5 {
		t.Fatalf("read %d: %v", seen, err)
	}
	// Uint64 sum/min/max are raw integer bits, never rounded through float64.
	r, err := newNumericAggregateRunReader(run.payload, GroupShapeI32, types, layouts)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := r.next()
	if err != nil {
		t.Fatal(err)
	}
	if got := numericAggregateRecordState(record, 2, types[2], layouts[2]); got.SumBits != (uint64(1)<<63)+13 || got.MinBits != got.SumBits {
		t.Fatalf("Uint64 precision lost: %+v", got)
	}
	legacy := numericAggregateRecordState(record, 6, types[6], layouts[6])
	if legacy.SumBits != uint64(1)<<63 || legacy.CountValues != 2 || legacy.AvgSum != 0 {
		t.Fatalf("legacy raw average=%+v", legacy)
	}
	copyRun, err := encodeNumericAggregateRun(context.Background(), GroupShapeI32, types, layouts, multiCodecVisit(record), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	copyRun.close()
	run.close()
	if manager.Used() != 0 {
		t.Fatalf("leaked %d bytes", manager.Used())
	}
}

func TestNumericAggregateCodecAllKeyShapes(t *testing.T) {
	types, layouts, bundle := numericCodecFixture(t)
	tests := []struct {
		shape  GroupShape
		record aggregateRecord
	}{
		{GroupShapeI32, aggregateRecord{a: 7, validMask: 1}}, {GroupShapeI64, aggregateRecord{}},
		{GroupShapeU32, aggregateRecord{a: 8, validMask: 1}}, {GroupShapeDictionaryString, aggregateRecord{a: 1, validMask: 1}},
		{GroupShapePairFixed, aggregateRecord{a: 9, b: 3, validMask: 3}}, {GroupShapeStringView, aggregateRecord{stringKey: []byte("中🙂"), validMask: 1}},
		{GroupShapeGlobal, aggregateRecord{}},
	}
	for _, tc := range tests {
		tc.record.numericBundle = bundle
		manager := memory.NewBudgetManager(1 << 20)
		run, err := encodeNumericAggregateRun(context.Background(), tc.shape, types, layouts, multiCodecVisit(tc.record), nil, manager)
		if err != nil {
			t.Fatal(err)
		}
		r, err := newNumericAggregateRunReader(run.payload, tc.shape, types, layouts)
		if err != nil {
			t.Fatal(err)
		}
		record, ok, err := r.next()
		if err != nil || !ok || record.a != tc.record.a || record.b != tc.record.b || record.validMask != tc.record.validMask || string(record.stringKey) != string(tc.record.stringKey) {
			t.Fatalf("shape%d record=%+v err=%v", tc.shape, record, err)
		}
		if _, ok, err = r.next(); err != nil || ok {
			t.Fatalf("shape%d EOF=%v %v", tc.shape, ok, err)
		}
		run.close()
		if manager.Used() != 0 {
			t.Fatal("leaked payload")
		}
	}
}

func TestNumericAggregateCodecPreservesIEEEPayloads(t *testing.T) {
	types := []memory.DataType{memory.TypeFloat32, memory.TypeFloat64}
	layout := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	layouts := []aggregate.Layout{layout, layout}
	states := []aggregate.NumericResult{
		{Type: types[0], Layout: layout, SumBits: 0x7fc00073, MinBits: 0x80000000, MaxBits: 0x7f800000, SumValid: true, MinValid: true, MaxValid: true, CountRows: 2, CountValues: 2, AvgSum: math.Float64frombits(0x7ff8000000000073)},
		{Type: types[1], Layout: layout, SumBits: 0xfff0000000000000, MinBits: 0x8000000000000000, MaxBits: 0, SumValid: true, MinValid: true, MaxValid: true, CountRows: 3, CountValues: 2, AvgSum: math.Copysign(0, -1)},
	}
	raw := make([]byte, 2*numericAggregateStateBytes)
	writer := aggregateRunWriter{buf: raw}
	for _, state := range states {
		encodeNumericAggregateState(&writer, state)
	}
	if writer.err != nil {
		t.Fatal(writer.err)
	}
	manager := memory.NewBudgetManager(1 << 20)
	run, err := encodeNumericAggregateRun(context.Background(), GroupShapeGlobal, types, layouts, multiCodecVisit(aggregateRecord{numericRaw: raw}), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	reader, err := newNumericAggregateRunReader(run.payload, GroupShapeGlobal, types, layouts)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := reader.next()
	if err != nil {
		t.Fatal(err)
	}
	for lane, want := range states {
		assertNumericCodecState(t, numericAggregateRecordState(record, lane, types[lane], layout), want)
	}
}

func TestNumericAggregateCodecRejectsCorruption(t *testing.T) {
	types, layouts, bundle := numericCodecFixture(t)
	manager := memory.NewBudgetManager(1 << 20)
	run, err := encodeNumericAggregateRun(context.Background(), GroupShapeI32, types, layouts, multiCodecVisit(aggregateRecord{a: 1, validMask: 1, numericBundle: bundle}), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	check := func(payload []byte) {
		t.Helper()
		r, err := newNumericAggregateRunReader(payload, GroupShapeI32, types, layouts)
		if err == nil {
			err = r.visit(func(aggregateRecord) error { return nil })
		}
		if !errors.Is(err, memory.ErrSpillCorrupt) {
			t.Fatalf("accepted corrupt payload: %v", err)
		}
	}
	for length := 0; length < len(run.payload); length++ {
		check(run.payload[:length])
	}
	offsets := []int{0, 8, 10, 11, 12, 20, 21}
	keyStart := numericAggregateRunHeaderLen + 2*len(types)
	offsets = append(offsets, keyStart)
	for lane := range types {
		for _, flag := range []int{8, 33, 42} {
			offsets = append(offsets, keyStart+9+lane*numericAggregateStateBytes+flag)
		}
	}
	for _, offset := range offsets {
		payload := append([]byte(nil), run.payload...)
		payload[offset] = 255
		check(payload)
	}
	count := append([]byte(nil), run.payload...)
	binary.LittleEndian.PutUint32(count[16:20], math.MaxUint32)
	check(count)
	check(append(append([]byte(nil), run.payload...), 0))
	if _, err := newNumericAggregateRunReader(run.payload, GroupShapeI32, types[:1], layouts[:1]); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("lane mismatch: %v", err)
	}
	wrongTypes := append([]memory.DataType(nil), types...)
	wrongTypes[0] = memory.TypeInt16
	if _, err := newNumericAggregateRunReader(run.payload, GroupShapeI32, wrongTypes, layouts); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("type mismatch: %v", err)
	}
	wrongLayouts := append([]aggregate.Layout(nil), layouts...)
	wrongLayouts[0] = aggregate.Layout{CountRows: true}
	if _, err := newNumericAggregateRunReader(run.payload, GroupShapeI32, types, wrongLayouts); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("layout mismatch: %v", err)
	}
}

func TestNumericAggregateCodecFailuresAndLegacy(t *testing.T) {
	types, layouts, bundle := numericCodecFixture(t)
	for _, mode := range []string{"reserve", "second-pass", "changed-count", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			limit := int64(1 << 20)
			if mode == "reserve" {
				limit = 1
			}
			manager := memory.NewBudgetManager(limit)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			passes := 0
			visit := func(fn func(aggregateRecord) error) error {
				passes++
				if passes == 2 {
					switch mode {
					case "second-pass":
						return errors.New("visitor failed")
					case "changed-count":
						return nil
					case "cancel":
						cancel()
					}
				}
				return fn(aggregateRecord{a: 1, validMask: 1, numericBundle: bundle})
			}
			run, err := encodeNumericAggregateRun(ctx, GroupShapeI32, types, layouts, visit, nil, manager)
			if run != nil || err == nil || manager.Used() != 0 {
				t.Fatalf("run=%v err=%v used=%d", run, err, manager.Used())
			}
			if mode != "reserve" && passes != 2 {
				t.Fatalf("second pass not reached: %d", passes)
			}
		})
	}
	manager := memory.NewBudgetManager(1 << 20)
	layout := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	old := aggregate.Int64Result{Sum: math.MinInt64, SumValid: true, CountRows: 5, CountValues: 3, Min: -9, MinValid: true, Max: 7, MaxValid: true}
	run, err := encodeNumericAggregateRun(context.Background(), GroupShapeI32, []memory.DataType{memory.TypeInt64}, []aggregate.Layout{layout}, multiCodecVisit(aggregateRecord{a: 1, validMask: 1, state: old}), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	reader, err := newNumericAggregateRunReader(run.payload, GroupShapeI32, []memory.DataType{memory.TypeInt64}, []aggregate.Layout{layout})
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := reader.next()
	if err != nil {
		t.Fatal(err)
	}
	want := aggregate.NumericResult{Type: memory.TypeInt64, Layout: layout, SumBits: uint64(old.Sum), SumValid: old.SumValid, CountRows: old.CountRows, CountValues: old.CountValues, MinBits: uint64(old.Min), MinValid: old.MinValid, MaxBits: uint64(old.Max), MaxValid: old.MaxValid}
	assertNumericCodecState(t, numericAggregateRecordState(record, 0, memory.TypeInt64, layout), want)
	before := manager.Used()
	for _, typ := range []memory.DataType{memory.TypeInvalid, memory.TypeUint64, memory.TypeString} {
		bad, err := encodeNumericAggregateRun(context.Background(), GroupShapeGlobal, []memory.DataType{typ}, []aggregate.Layout{layout}, multiCodecVisit(aggregateRecord{state: old}), nil, manager)
		if err == nil || bad != nil || manager.Used() != before {
			t.Fatalf("legacy reinterpretation accepted type=%v err=%v", typ, err)
		}
	}
	wrong := append([]memory.DataType(nil), types...)
	wrong[0] = memory.TypeInt16
	if _, err := encodeNumericAggregateRun(context.Background(), GroupShapeGlobal, wrong, layouts, multiCodecVisit(aggregateRecord{numericBundle: bundle}), nil, manager); err == nil {
		t.Fatal("bundle descriptor mismatch accepted")
	}
}

func TestNumericAggregateCodecReaderNoPerRecordAllocation(t *testing.T) {
	types, layouts, bundle := numericCodecFixture(t)
	manager := memory.NewBudgetManager(1 << 20)
	run, err := encodeNumericAggregateRun(context.Background(), GroupShapeI32, types, layouts, multiCodecVisit(aggregateRecord{a: 1, validMask: 1, numericBundle: bundle}), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	reader, err := newNumericAggregateRunReader(run.payload, GroupShapeI32, types, layouts)
	if err != nil {
		t.Fatal(err)
	}
	start := reader.pos
	n := testing.AllocsPerRun(100, func() {
		reader.pos = start
		reader.groups = 1
		record, ok, err := reader.next()
		if err != nil || !ok || numericAggregateRecordState(record, 2, types[2], layouts[2]).SumBits != (uint64(1)<<63)+13 {
			panic("bad record")
		}
	})
	if n != 0 {
		t.Fatalf("allocated %v objects per record", n)
	}
}

func TestNumericAggregateCodecStringLengthsAndFiltering(t *testing.T) {
	types, layouts, bundle := numericCodecFixture(t)
	manager := memory.NewBudgetManager(1 << 20)
	record := aggregateRecord{validMask: 1, stringKey: []byte("abc"), numericBundle: bundle}
	run, err := encodeNumericAggregateRun(context.Background(), GroupShapeStringView, types, layouts, multiCodecVisit(record, record), func(i int, _ aggregateRecord) bool { return i == 1 }, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	if run.groups != 1 {
		t.Fatalf("groups=%d", run.groups)
	}
	payload := append([]byte(nil), run.payload...)
	keyStart := numericAggregateRunHeaderLen + 2*len(types)
	binary.LittleEndian.PutUint32(payload[keyStart+1:keyStart+5], math.MaxUint32)
	reader, err := newNumericAggregateRunReader(payload, GroupShapeStringView, types, layouts)
	if err == nil {
		err = reader.visit(func(aggregateRecord) error { return nil })
	}
	if !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("string length=%v", err)
	}
	emptyPayload := append([]byte(nil), run.payload[:keyStart]...)
	binary.LittleEndian.PutUint32(emptyPayload[16:20], 0)
	reader, err = newNumericAggregateRunReader(emptyPayload, GroupShapeStringView, types, layouts)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reader.next(); err != nil || ok {
		t.Fatalf("empty next=%v %v", ok, err)
	}
	if _, err := newNumericAggregateRunReader(append(emptyPayload, 0), GroupShapeStringView, types, layouts); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("empty trailing=%v", err)
	}
	before := manager.Used()
	empty, err := encodeNumericAggregateRun(context.Background(), GroupShapeStringView, types, layouts, multiCodecVisit(record), func(int, aggregateRecord) bool { return false }, manager)
	if err != nil || empty.groups != 0 || len(empty.payload) != 0 || manager.Used() != before {
		t.Fatalf("filtered empty=%+v err=%v", empty, err)
	}
	empty.close()
	for _, raw := range [][]byte{make([]byte, 1), make([]byte, len(types)*numericAggregateStateBytes)} {
		if len(raw) > 8 {
			raw[8] = 2
		}
		bad, err := encodeNumericAggregateRun(context.Background(), GroupShapeGlobal, types, layouts, multiCodecVisit(aggregateRecord{numericRaw: raw}), nil, manager)
		if err == nil || bad != nil || manager.Used() != before {
			t.Fatal("invalid raw state accepted or leaked")
		}
	}
}
