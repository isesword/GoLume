package parquet_test

import (
	"context"
	"gocolumn/engine"
	"gocolumn/lazy"
	"path/filepath"
	"testing"
	"time"
)

func TestLazyParquetWindowTimestampOrdering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "window-events.parquet")
	stamp := time.Date(2024, time.February, 29, 23, 58, 59, 0, time.UTC)
	writeLazyTypedFixture(t, path, stamp)
	session := lazy.NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := frame.WithWindows(
		lazy.NewWindow(nil, lazy.OrderBy(lazy.Asc("event_time"))),
		lazy.RowNumber("rn"),
		lazy.RollingCountRows(lazy.RowsBetween(lazy.UnboundedPreceding, lazy.CurrentRow), "seen"),
	).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	rnSeries, err := result.Column("rn")
	if err != nil {
		t.Fatal(err)
	}
	seenSeries, err := result.Column("seen")
	if err != nil {
		t.Fatal(err)
	}
	rn, _, err := rnSeries.CopyInt64()
	if err != nil {
		t.Fatal(err)
	}
	seen, _, err := seenSeries.CopyInt64()
	if err != nil {
		t.Fatal(err)
	}
	if got := rn; len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("row number=%v", got)
	}
	if got := seen; len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("rolling count=%v", got)
	}
}
