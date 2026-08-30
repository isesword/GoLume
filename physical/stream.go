package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
)

func openUnaryInputStream(ctx context.Context, input source.Source, opts source.ReadOptions, parts []source.Partition, prepared bool) (source.BatchStream, error) {
	if prepared {
		return source.OpenBatchStreamWithPartitions(ctx, input, opts, parts)
	}
	return source.OpenBatchStream(ctx, input, opts)
}

// unaryStreamState centralizes the terminal error and idempotent child close
// rules shared by batch-local physical operators. Operator-owned workspaces are
// still closed by the concrete stream before calling fail or finish.
type unaryStreamState struct {
	child       source.BatchStream
	closeOwn    func() error
	ownedClosed bool
	exhausted   bool
	closed      bool
	terminalErr error
	closeErr    error
}

func (s *unaryStreamState) before(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return true, errors.New("batch stream destination is nil")
	}
	if ctx == nil {
		dst.ClearColumns()
		return true, errors.New("batch stream context is nil")
	}
	if s.closed {
		dst.ClearColumns()
		return true, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		dst.ClearColumns()
		return true, s.terminalErr
	}
	if s.exhausted {
		dst.ClearColumns()
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		dst.ClearColumns()
		return true, s.fail(err)
	}
	return false, nil
}

func (s *unaryStreamState) fail(err error) error {
	if s.terminalErr != nil {
		return s.terminalErr
	}
	s.closeErr = s.closeResources()
	s.terminalErr = errors.Join(err, s.closeErr)
	return s.terminalErr
}

func (s *unaryStreamState) finish() error {
	if s.exhausted {
		return s.terminalErr
	}
	s.exhausted = true
	s.closeErr = s.closeResources()
	if s.closeErr != nil {
		s.terminalErr = s.closeErr
	}
	return s.terminalErr
}

func (s *unaryStreamState) closeResources() error {
	if s.ownedClosed && s.child == nil {
		return s.closeErr
	}
	var ownErr error
	if !s.ownedClosed {
		s.ownedClosed = true
		if s.closeOwn != nil {
			ownErr = s.closeOwn()
		}
	}
	var childErr error
	if s.child != nil {
		childErr = s.child.Close()
		s.child = nil
	}
	s.closeErr = errors.Join(ownErr, childErr, s.closeErr)
	return s.closeErr
}

// Close releases operator-owned state before the child stream and is idempotent.
func (s *unaryStreamState) Close() error {
	if s == nil || s.closed {
		return s.closeErr
	}
	s.closed = true
	return s.closeResources()
}
