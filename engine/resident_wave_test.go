package engine

import (
	"context"
	"errors"
	"gocolumn/memory"
	"testing"
)

func TestResidentWaveOwnershipAndInputIndependentPeak(t *testing.T) {
	var peak int64
	for _, rows := range []int{1024, 1 << 20} {
		opts := streamTestOptions()
		opts.Workers = 4
		opts.Pipeline.MaxInFlightBytes = 64 << 10
		eng := New(opts)
		src := &generatedStreamSource{rows: rows, groups: 17}
		prepared, err := eng.Prepare(context.Background(), src, partitionedQuery(), PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		budget, err := eng.BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		ctx := memory.WithQueryBudget(context.Background(), budget)
		stream, err := prepared.NewResidentAggregateStream(ctx, prepared.resultSchema(), 1)
		if err != nil {
			t.Fatal(err)
		}
		state := stream.(*residentAggregateStream).partitioned
		wave := state.wave
		if wave == nil || wave.capacity <= 64 || wave.capacity > 4*64 {
			t.Fatalf("wave=%+v", wave)
		}
		actual := int64(cap(wave.i32))*4 + int64(cap(wave.values))*8 + int64(cap(wave.keyValidity)+cap(wave.valueValidity)+cap(wave.selection))*8
		if wave.lease.Bytes() != actual {
			t.Fatalf("wave charged=%d actual=%d", wave.lease.Bytes(), actual)
		}
		batch := memory.NewBatch()
		if ok, err := stream.Next(ctx, batch); err != nil || !ok {
			t.Fatalf("next=%v err=%v", ok, err)
		}
		if state.wave.rows != 0 || stream.(*residentAggregateStream).checksum != int64(rows) {
			t.Fatalf("pending=%d sum=%d", state.wave.rows, stream.(*residentAggregateStream).checksum)
		}
		// Wave plus scatter never consumes the whole configured in-flight allowance.
		if wave.lease.Bytes()+state.scratchLease.Bytes() > opts.Pipeline.MaxInFlightBytes/4 {
			t.Fatal("operator wave exceeds its in-flight share")
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		snap := budget.Snapshot()
		if snap.UsedBytes != 0 || src.opened.Load() != src.closed.Load() {
			t.Fatalf("budget=%+v opened=%d closed=%d", snap, src.opened.Load(), src.closed.Load())
		}
		if peak != 0 && peak != snap.PeakBytes {
			t.Fatalf("peak depends on input rows: %d -> %d", peak, snap.PeakBytes)
		}
		peak = snap.PeakBytes
		budget.Close()
		prepared.Close()
		eng.Close()
	}
}

func TestResidentWaveCancellationReleasesPendingRows(t *testing.T) {
	opts := streamTestOptions()
	opts.Workers = 4
	opts.Pipeline.MaxInFlightBytes = 64 << 10
	eng := New(opts)
	defer eng.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &generatedStreamSource{rows: 1024, groups: 17, cancel: cancel, cancelAt: 128}
	_, err := eng.Execute(ctx, src, partitionedQuery(), ExecuteOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if eng.Memory().Used() != 0 || src.closed.Load() != 1 {
		t.Fatalf("used=%d closed=%d", eng.Memory().Used(), src.closed.Load())
	}
}
