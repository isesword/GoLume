package source

import (
	"context"
	"gocolumn/memory"
	"testing"
)

const phase30ProtocolBatches = 1024

type phase30FillProtocol interface {
	Next(context.Context, *memory.Batch) (bool, error)
}

type phase30FillPrototype struct {
	remaining int
	vector    memory.Int64Vector
}

func (s *phase30FillPrototype) Next(_ context.Context, dst *memory.Batch) (bool, error) {
	if s.remaining == 0 {
		return false, nil
	}
	s.remaining--
	dst.Int64s["x"] = &s.vector
	dst.Reset(len(s.vector.Values))
	return true, nil
}

type phase30Lease interface {
	Batch() *memory.Batch
	Release()
}

type phase30LeaseProtocol interface {
	Next(context.Context) (phase30Lease, bool, error)
}

type phase30ReusableLease struct {
	batch    *memory.Batch
	released bool
}

func (l *phase30ReusableLease) Batch() *memory.Batch { return l.batch }
func (l *phase30ReusableLease) Release()             { l.released = true }

type phase30LeasePrototype struct {
	remaining int
	vector    memory.Int64Vector
	lease     phase30ReusableLease
}

func (s *phase30LeasePrototype) Next(context.Context) (phase30Lease, bool, error) {
	if s.remaining == 0 {
		return nil, false, nil
	}
	s.remaining--
	s.lease.released = false
	s.lease.batch.Int64s["x"] = &s.vector
	s.lease.batch.Reset(len(s.vector.Values))
	return &s.lease, true, nil
}

func BenchmarkPhase30StreamProtocol(b *testing.B) {
	ctx := context.Background()
	values := make([]int64, 4096)
	b.Run("destination-fill", func(b *testing.B) {
		b.ReportAllocs()
		dst := memory.NewBatch()
		var stream phase30FillProtocol = &phase30FillPrototype{vector: memory.Int64Vector{Values: values}}
		for i := 0; i < b.N; i++ {
			impl := stream.(*phase30FillPrototype)
			impl.remaining = phase30ProtocolBatches
			for {
				ok, err := stream.Next(ctx, dst)
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					break
				}
			}
		}
	})
	b.Run("reusable-lease", func(b *testing.B) {
		b.ReportAllocs()
		impl := &phase30LeasePrototype{vector: memory.Int64Vector{Values: values}, lease: phase30ReusableLease{batch: memory.NewBatch()}}
		var stream phase30LeaseProtocol = impl
		for i := 0; i < b.N; i++ {
			impl.remaining = phase30ProtocolBatches
			for {
				lease, ok, err := stream.Next(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					break
				}
				_ = lease.Batch().Rows
				lease.Release()
			}
		}
	})
}
