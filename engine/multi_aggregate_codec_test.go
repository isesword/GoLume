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

func multiCodecLayouts() []aggregate.Layout {
	return []aggregate.Layout{{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}, {Avg: true, Min: true}}
}

func multiCodecStates() []aggregate.Int64Result {
	return []aggregate.Int64Result{{Sum: 12, SumValid: true, CountRows: 4, CountValues: 3, Min: -2, MinValid: true, Max: 9, MaxValid: true}, {Sum: -9, SumValid: true, CountValues: 2, Min: -7, MinValid: true}}
}

func multiCodecVisit(records ...aggregateRecord) aggregateRecordVisitor {
	return func(fn func(aggregateRecord) error) error {
		for _, r := range records {
			if err := fn(r); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestMultiAggregateCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		shape  GroupShape
		record aggregateRecord
	}{
		{"i32", GroupShapeI32, aggregateRecord{a: 7, validMask: 1}},
		{"i64-null", GroupShapeI64, aggregateRecord{}},
		{"u32", GroupShapeU32, aggregateRecord{a: 9, validMask: 1}},
		{"dictionary", GroupShapeDictionaryString, aggregateRecord{a: 3, validMask: 1}},
		{"pair", GroupShapePairFixed, aggregateRecord{a: 1, b: 2, validMask: 2}},
		{"string", GroupShapeStringView, aggregateRecord{stringKey: []byte("中🙂"), validMask: 1}},
		{"global", GroupShapeGlobal, aggregateRecord{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layouts, states := multiCodecLayouts(), multiCodecStates()
			tt.record.states = states
			manager := memory.NewBudgetManager(1 << 20)
			encoded, err := encodeMultiAggregateRun(context.Background(), tt.shape, layouts, multiCodecVisit(tt.record), nil, manager)
			if err != nil {
				t.Fatal(err)
			}
			defer encoded.close()
			if manager.Used() != int64(len(encoded.payload)) {
				t.Fatalf("used=%d payload=%d", manager.Used(), len(encoded.payload))
			}
			reader, err := newMultiAggregateRunReader(encoded.payload, tt.shape, layouts)
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := reader.next()
			if err != nil || !ok {
				t.Fatalf("next=%v %v", ok, err)
			}
			if got.a != tt.record.a || got.b != tt.record.b || got.validMask != tt.record.validMask || string(got.stringKey) != string(tt.record.stringKey) {
				t.Fatalf("key=%+v want=%+v", got, tt.record)
			}
			if got.states != nil || len(got.rawStates) != len(states)*aggregateStateBytes {
				t.Fatal("reader did not borrow raw state lanes")
			}
			for lane, want := range states {
				state := aggregateRecordState(got, lane)
				state.Avg, state.AvgValid = 0, false
				if state != want {
					t.Fatalf("lane%d=%+v want=%+v", lane, state, want)
				}
			}
			if _, ok, err := reader.next(); err != nil || ok {
				t.Fatalf("end=%v %v", ok, err)
			}
			// Re-encoding borrowed decoded records needs no temporary lane arrays.
			copyRun, err := encodeMultiAggregateRun(context.Background(), tt.shape, layouts, multiCodecVisit(got), nil, manager)
			if err != nil {
				t.Fatal(err)
			}
			if string(copyRun.payload) != string(encoded.payload) {
				t.Fatal("raw roundtrip changed payload")
			}
			copyRun.close()
			encoded.close()
			if manager.Used() != 0 {
				t.Fatalf("leaked %d bytes", manager.Used())
			}
		})
	}
}

func TestMultiAggregateCodecRawAverageMerge(t *testing.T) {
	layouts := []aggregate.Layout{{Avg: true}, {Avg: true}}
	manager := memory.NewBudgetManager(1 << 20)
	bundle, err := aggregate.NewInt64Bundle(layouts, func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) })
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	if err := bundle.EnsureGroups(1); err != nil {
		t.Fatal(err)
	}
	bundle.AddValue(0, 0, 2, true)
	bundle.AddValue(0, 0, 6, true)
	bundle.AddValue(0, 1, -10, true)
	record := aggregateRecord{a: 1, validMask: 1, bundle: bundle, groupID: 0, states: []aggregate.Int64Result{{Sum: 999}}}
	encoded, err := encodeMultiAggregateRun(context.Background(), GroupShapeI32, layouts, multiCodecVisit(record), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.close()
	reader, err := newMultiAggregateRunReader(encoded.payload, GroupShapeI32, layouts)
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok, err := reader.next()
	if err != nil || !ok {
		t.Fatalf("next=%v %v", ok, err)
	}
	first := aggregateRecordState(decoded, 0)
	if first.Sum != 8 || first.CountValues != 2 || first.Avg != 4 || !first.AvgValid {
		t.Fatalf("raw average=%+v", first)
	}
	merged := aggregate.NewInt64Vector(1, layouts[0])
	merged.InitValue(0, 10, true)
	merged.MergeResult(0, first)
	if got := merged.Result(0); got.Sum != 18 || got.CountValues != 3 || got.Avg != 6 {
		t.Fatalf("weighted merge=%+v", got)
	}
	if second := aggregateRecordState(decoded, 1); second.Sum != -10 || second.CountValues != 1 {
		t.Fatalf("lane1=%+v", second)
	}
	// The codec preserves the legacy wrapping sum, not a finalized average.
	wrap := aggregateRecord{a: 1, validMask: 1, states: []aggregate.Int64Result{{Sum: math.MinInt64, SumValid: true, CountValues: 2}, {}}}
	run, err := encodeMultiAggregateRun(context.Background(), GroupShapeI32, layouts, multiCodecVisit(wrap), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	r, err := newMultiAggregateRunReader(run.payload, GroupShapeI32, layouts)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := r.next()
	if err != nil || aggregateRecordState(got, 0).Sum != math.MinInt64 {
		t.Fatalf("wrapped state=%+v err=%v", got, err)
	}
}

func TestMultiAggregateCodecRejectsCorruption(t *testing.T) {
	layouts := multiCodecLayouts()
	manager := memory.NewBudgetManager(1 << 20)
	encoded, err := encodeMultiAggregateRun(context.Background(), GroupShapeI32, layouts, multiCodecVisit(aggregateRecord{a: 1, validMask: 1, states: multiCodecStates()}), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.close()
	check := func(t *testing.T, payload []byte) {
		t.Helper()
		reader, err := newMultiAggregateRunReader(payload, GroupShapeI32, layouts)
		if err == nil {
			err = reader.visit(func(aggregateRecord) error { return nil })
		}
		if !errors.Is(err, memory.ErrSpillCorrupt) {
			t.Fatalf("expected corrupt, got %v", err)
		}
	}
	for length := 0; length < len(encoded.payload); length++ {
		check(t, encoded.payload[:length])
	}
	cases := []struct {
		name   string
		offset int
		value  byte
	}{{"magic", 0, 0}, {"version", 8, 1}, {"shape", 10, 255}, {"reserved", 11, 1}, {"lanes", 12, 3}, {"layout0", 20, 0}, {"layout1", 21, 255}, {"keymask", 22, 2}}
	for lane := 0; lane < len(layouts); lane++ {
		for _, flag := range []int{8, 33, 42} {
			cases = append(cases, struct {
				name   string
				offset int
				value  byte
			}{name: "state-flag", offset: 22 + 9 + lane*aggregateStateBytes + flag, value: 2})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := append([]byte(nil), encoded.payload...)
			payload[tc.offset] = tc.value
			check(t, payload)
		})
	}
	check(t, append(append([]byte(nil), encoded.payload...), 0))
	badCount := append([]byte(nil), encoded.payload...)
	binary.LittleEndian.PutUint32(badCount[16:20], math.MaxUint32)
	check(t, badCount)
	if _, err := newMultiAggregateRunReader(encoded.payload, GroupShapeI32, layouts[:1]); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("expected lane mismatch: %v", err)
	}
	if _, err := newMultiAggregateRunReader(encoded.payload, GroupShapeI32, []aggregate.Layout{{}, {Sum: true}}); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("expected empty layout rejection: %v", err)
	}
}

func TestMultiAggregateCodecFailureReleasesBudget(t *testing.T) {
	layouts := multiCodecLayouts()
	record := aggregateRecord{a: 1, validMask: 1, states: multiCodecStates()}
	for _, mode := range []string{"reserve", "second-pass", "changed-count", "changed-lanes", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			record := record
			limit := int64(1 << 20)
			if mode == "reserve" {
				limit = 1
			}
			manager := memory.NewBudgetManager(limit)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			passes := 0
			visitor := func(fn func(aggregateRecord) error) error {
				passes++
				if passes == 2 {
					switch mode {
					case "second-pass":
						return errors.New("visitor failed")
					case "changed-count":
						return nil
					case "changed-lanes":
						record.states = record.states[:1]
					case "cancel":
						cancel()
					}
				}
				return fn(record)
			}
			run, err := encodeMultiAggregateRun(ctx, GroupShapeI32, layouts, visitor, nil, manager)
			if err == nil || run != nil {
				t.Fatalf("failure mode %s returned run=%v err=%v", mode, run, err)
			}
			if manager.Used() != 0 {
				t.Fatalf("leaked %d bytes", manager.Used())
			}
			if mode != "reserve" && passes != 2 {
				t.Fatalf("failure did not reach reserved second pass: %d", passes)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel error=%v", err)
			}
		})
	}
}

func TestMultiAggregateCodecFilteringAndRecordValidation(t *testing.T) {
	layouts := multiCodecLayouts()
	manager := memory.NewBudgetManager(1 << 20)
	records := []aggregateRecord{{a: 1, validMask: 1, states: multiCodecStates()}, {a: 2, validMask: 1, states: multiCodecStates()}}
	run, err := encodeMultiAggregateRun(context.Background(), GroupShapeI32, layouts, multiCodecVisit(records...), func(i int, _ aggregateRecord) bool { return i == 1 }, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	if run.groups != 1 {
		t.Fatalf("groups=%d", run.groups)
	}
	r, err := newMultiAggregateRunReader(run.payload, GroupShapeI32, layouts)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := r.next()
	if err != nil || got.a != 2 {
		t.Fatalf("filtered record=%+v err=%v", got, err)
	}
	empty, err := encodeMultiAggregateRun(context.Background(), GroupShapeI32, layouts, multiCodecVisit(records...), func(int, aggregateRecord) bool { return false }, manager)
	if err != nil || empty.groups != 0 || len(empty.payload) != 0 {
		t.Fatalf("empty=%+v err=%v", empty, err)
	}
	empty.close()
	for _, bad := range []aggregateRecord{{state: aggregate.Int64Result{}}, {states: multiCodecStates()[:1]}, {rawStates: make([]byte, 3)}, {validMask: 3, states: multiCodecStates()}} {
		before := manager.Used()
		if run, err := encodeMultiAggregateRun(context.Background(), GroupShapeI32, layouts, multiCodecVisit(bad), nil, manager); err == nil || run != nil {
			t.Fatalf("bad record accepted: %+v", bad)
		}
		if manager.Used() != before {
			t.Fatal("bad record leaked budget")
		}
	}
	if _, err := encodeMultiAggregateRun(nil, GroupShapeI32, layouts, multiCodecVisit(records...), nil, manager); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := encodeMultiAggregateRun(context.Background(), GroupShapeInvalid, layouts, multiCodecVisit(records...), nil, manager); err == nil {
		t.Fatal("invalid shape accepted")
	}
}

func TestMultiAggregateCodecReaderDoesNotAllocatePerRecord(t *testing.T) {
	layouts := multiCodecLayouts()
	manager := memory.NewBudgetManager(1 << 20)
	run, err := encodeMultiAggregateRun(context.Background(), GroupShapeI32, layouts, multiCodecVisit(aggregateRecord{a: 1, validMask: 1, states: multiCodecStates()}), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	r, err := newMultiAggregateRunReader(run.payload, GroupShapeI32, layouts)
	if err != nil {
		t.Fatal(err)
	}
	start := r.aggregateRunReader.pos
	n := testing.AllocsPerRun(100, func() {
		r.aggregateRunReader.pos = start
		r.aggregateRunReader.groups = 1
		record, ok, err := r.next()
		if err != nil || !ok || aggregateRecordState(record, 1).Sum != -9 {
			panic("invalid record")
		}
	})
	if n != 0 {
		t.Fatalf("reader allocated %v objects per record", n)
	}
}

func TestMultiAggregateCodecStringLengthAndEmptyRun(t *testing.T) {
	layouts := multiCodecLayouts()
	manager := memory.NewBudgetManager(1 << 20)
	run, err := encodeMultiAggregateRun(context.Background(), GroupShapeStringView, layouts, multiCodecVisit(aggregateRecord{validMask: 1, stringKey: []byte("abc"), states: multiCodecStates()}), nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	payload := append([]byte(nil), run.payload...)
	binary.LittleEndian.PutUint32(payload[23:27], math.MaxUint32)
	reader, err := newMultiAggregateRunReader(payload, GroupShapeStringView, layouts)
	if err == nil {
		err = reader.visit(func(aggregateRecord) error { return nil })
	}
	if !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("string length accepted: %v", err)
	}
	empty := append([]byte(nil), run.payload[:multiAggregateRunHeaderLen+len(layouts)]...)
	binary.LittleEndian.PutUint32(empty[16:20], 0)
	reader, err = newMultiAggregateRunReader(empty, GroupShapeStringView, layouts)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reader.next(); err != nil || ok {
		t.Fatalf("empty next=%v %v", ok, err)
	}
	if _, err := newMultiAggregateRunReader(append(empty, 0), GroupShapeStringView, layouts); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("empty trailing=%v", err)
	}
}
