package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
)

// testReader is deliberately test-only: production physical operators expose
// BatchStream exclusively. It lets legacy assertions verify one pull without
// reinstating a Reader implementation in the physical package.
type testReader struct{ stream source.BatchStream }

func openTestReader(ctx context.Context, src source.Source, opts source.ReadOptions) (source.Reader, error) {
	stream, err := source.OpenBatchStream(ctx, src, opts)
	if err != nil {
		return nil, err
	}
	return &testReader{stream: stream}, nil
}

func (r *testReader) ReadPartition(ctx context.Context, _ source.Partition, dst *memory.Batch) error {
	ok, err := r.stream.Next(ctx, dst)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("test reader reached end of stream")
	}
	return nil
}
func (r *testReader) Close() error { return r.stream.Close() }
