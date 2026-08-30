package engine

import (
	"context"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"testing"
)

func TestAggregateRunCodecRoundTripAllKeyShapes(t *testing.T) {
	layout := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	state := aggregate.Int64Result{Sum: 12, SumValid: true, CountRows: 4, CountValues: 3, Min: -2, MinValid: true, Max: 9, MaxValid: true}
	tests := []struct {
		name   string
		shape  GroupShape
		record aggregateRecord
	}{
		{name: "i32", shape: GroupShapeI32, record: aggregateRecord{a: 7, validMask: 1, state: state}},
		{name: "i64-null", shape: GroupShapeI64, record: aggregateRecord{state: state}},
		{name: "u32", shape: GroupShapeU32, record: aggregateRecord{a: 9, validMask: 1, state: state}},
		{name: "dictionary", shape: GroupShapeDictionaryString, record: aggregateRecord{a: 3, validMask: 1, state: state}},
		{name: "pair", shape: GroupShapePairFixed, record: aggregateRecord{a: 1, b: 2, validMask: 2, state: state}},
		{name: "string", shape: GroupShapeStringView, record: aggregateRecord{stringKey: []byte("中🙂"), validMask: 1, state: state}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			encoded, err := encodeAggregateRun(context.Background(), tt.shape, layout, func(fn func(aggregateRecord) error) error {
				return fn(tt.record)
			}, nil, manager)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := newAggregateRunReader(encoded.payload, tt.shape, layout)
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := reader.next()
			if err != nil || !ok {
				t.Fatalf("next ok=%v err=%v", ok, err)
			}
			if got.a != tt.record.a || got.b != tt.record.b || got.validMask != tt.record.validMask || string(got.stringKey) != string(tt.record.stringKey) || got.state.Sum != state.Sum || got.state.CountRows != state.CountRows || got.state.CountValues != state.CountValues || got.state.Min != state.Min || got.state.Max != state.Max {
				t.Fatalf("record=%+v want=%+v", got, tt.record)
			}
			if _, ok, err := reader.next(); err != nil || ok {
				t.Fatalf("end ok=%v err=%v", ok, err)
			}
			encoded.close()
			if manager.Used() != 0 {
				t.Fatalf("encode reservation leaked %d", manager.Used())
			}
		})
	}
}

func TestAggregateRunCodecRejectsCorruption(t *testing.T) {
	layout := aggregate.DefaultLayout()
	manager := memory.NewBudgetManager(1 << 20)
	encoded, err := encodeAggregateRun(context.Background(), GroupShapeI32, layout, func(fn func(aggregateRecord) error) error {
		return fn(aggregateRecord{a: 1, validMask: 1, state: aggregate.Int64Result{Sum: 1, SumValid: true, CountRows: 1}})
	}, nil, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.close()
	cases := map[string][]byte{
		"truncated": append([]byte(nil), encoded.payload[:len(encoded.payload)-1]...),
		"version":   append([]byte(nil), encoded.payload...),
		"validity":  append([]byte(nil), encoded.payload...),
		"trailing":  append(append([]byte(nil), encoded.payload...), 0),
	}
	cases["version"][8]++
	// Header(16) + key validity(1) + key(8) + sum(8) = sum validity.
	cases["validity"][33] = 2
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			reader, err := newAggregateRunReader(payload, GroupShapeI32, layout)
			if err == nil {
				err = reader.visit(func(aggregateRecord) error { return nil })
			}
			if !errors.Is(err, memory.ErrSpillCorrupt) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
