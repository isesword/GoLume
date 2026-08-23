package memory

import (
	"context"
	"errors"
)

// ErrSpillUnsupported reports that no external spill implementation is configured.
var ErrSpillUnsupported = errors.New("spill is not configured")

// SpillHandle identifies a persisted spill object and its accounted size.
type SpillHandle interface {
	ID() string
	Bytes() int64
}

// SpillPartition is the minimal partition payload accepted by SpillManager.
// The representation is intentionally narrow until the external-memory execution format is finalized.
type SpillPartition struct {
	Partition int
	Keys      []uint64
	Values    []int64
}

// SpillManager persists and restores partitions when operators exceed in-memory budgets.
type SpillManager interface {
	Spill(ctx context.Context, p SpillPartition) (SpillHandle, error)
	Restore(ctx context.Context, h SpillHandle) (SpillPartition, error)
	Delete(ctx context.Context, h SpillHandle) error
}

// NoSpill is a SpillManager implementation that rejects every spill operation.
type NoSpill struct{}

// Spill returns ErrSpillUnsupported.
func (NoSpill) Spill(context.Context, SpillPartition) (SpillHandle, error) {
	return nil, ErrSpillUnsupported
}

// Restore returns ErrSpillUnsupported.
func (NoSpill) Restore(context.Context, SpillHandle) (SpillPartition, error) {
	return SpillPartition{}, ErrSpillUnsupported
}

// Delete returns ErrSpillUnsupported.
func (NoSpill) Delete(context.Context, SpillHandle) error { return ErrSpillUnsupported }
