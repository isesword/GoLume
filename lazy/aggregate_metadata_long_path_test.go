package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type aggregateMetadataResultKey struct {
	value int32
	valid bool
}

type aggregateMetadataResultRow struct {
	sum, min, max int64
	rows, values  uint64
	avg           float64
	valid         [4]bool
}

func readAggregateMetadataResult(batch *memory.Batch, row int) (aggregateMetadataResultKey, aggregateMetadataResultRow) {
	key := aggregateMetadataResultKey{valid: batch.Int32s["key"].IsValid(row)}
	if key.valid {
		key.value = batch.Int32s["key"].Values[row]
	}
	return key, aggregateMetadataResultRow{
		sum: batch.Int64s["sum"].Values[row], min: batch.Int64s["min"].Values[row], max: batch.Int64s["max"].Values[row],
		rows: batch.Uint64s["rows"].Values[row], values: batch.Uint64s["values"].Values[row], avg: batch.Float64s["avg"].Values[row],
		valid: [4]bool{batch.Int64s["sum"].IsValid(row), batch.Int64s["min"].IsValid(row), batch.Int64s["max"].IsValid(row), batch.Float64s["avg"].IsValid(row)},
	}
}

func aggregateMetadataLongDirectory(t *testing.T, extraBytes int) string {
	t.Helper()
	dir := t.TempDir()
	for extraBytes > 0 {
		component := min(extraBytes, 60)
		dir = filepath.Join(dir, strings.Repeat("p", component))
		extraBytes -= component
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func aggregateMetadataFileMinimum(t *testing.T, dir string) (files int, minimum int64) {
	t.Helper()
	if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".gls") {
			return fmt.Errorf("unexpected aggregate spill file %s", path)
		}
		files++
		// Independent lower bound: full directory bytes and the fixed store
		// handle/write allowance, plus the private handle. Codec bytes, maps
		// and operator reference arrays require additional admission.
		minimum += 512 + 2*int64(len(filepath.Dir(path))) + 256
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files, minimum
}

func TestBoundedAggregateLongSpillDirectory(t *testing.T) {
	for _, tc := range []struct {
		name       string
		extraBytes int
		batches    int
	}{
		{name: "short", batches: 8},
		{name: "long", extraBytes: 180, batches: 8},
		{name: "long_repeated_input", extraBytes: 768, batches: 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := spillAggregateInput(t, tc.batches, 512, 1024)
			query := func(session *Session) LazyFrame {
				return session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("rows"), CountColumn("value", "values"), Min("value", "min"), Max("value", "max"), Avg("value", "avg"))
			}
			resident := NewSession(engine.DefaultOptions())
			defer resident.Close()
			wantFrame, err := query(resident).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer wantFrame.Close()
			want := make(map[aggregateMetadataResultKey]aggregateMetadataResultRow)
			for _, batch := range wantFrame.batches() {
				for row := 0; row < batch.Rows; row++ {
					key, result := readAggregateMetadataResult(batch, row)
					if _, duplicate := want[key]; duplicate {
						t.Fatal("resident duplicate group", key)
					}
					want[key] = result
				}
			}

			dir := aggregateMetadataLongDirectory(t, tc.extraBytes)
			opts := engine.DefaultOptions()
			opts.MemoryLimitBytes = 256 << 10
			opts.QueryMemorySoftLimitBytes = 96 << 10
			opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 32 << 20}
			opts.ExecutionPolicy = engine.ExecutionBounded
			bounded := NewSession(opts)
			defer bounded.Close()
			prepared, err := query(bounded).Prepare(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			budget, err := bounded.Engine().BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			reader, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			out := memory.NewBatch()
			seen := make(map[aggregateMetadataResultKey]bool)
			liveFiles, liveMetadata, batches := false, false, 0
			for {
				ok, err := reader.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				batches++
				snapshot := budget.Snapshot()
				files, minimum := aggregateMetadataFileMinimum(t, dir)
				liveFiles = liveFiles || files > 0
				used := snapshot.Operators["HashAggregateMetadata"]
				liveMetadata = liveMetadata || used > 0
				if used < minimum || snapshot.PeakBytes > opts.MemoryLimitBytes {
					t.Fatalf("directory bytes=%d files=%d metadata=%d file-only minimum=%d snapshot=%+v", len(dir), files, used, minimum, snapshot)
				}
				if snapshot.Operators["HashAggregateOutput"] == 0 {
					t.Fatal("borrowed output lost its reservation before consumption")
				}
				for row := 0; row < out.Rows; row++ {
					key, got := readAggregateMetadataResult(out, row)
					expected, exists := want[key]
					if !exists || seen[key] || got != expected {
						t.Fatalf("key=%+v got=%+v want=%+v exists=%v duplicate=%v", key, got, expected, exists, seen[key])
					}
					seen[key] = true
				}
			}
			snapshot := budget.Snapshot()
			if !liveFiles || !liveMetadata || snapshot.SpillRequests == 0 || snapshot.SpilledBytes == 0 || batches < 2 || len(seen) != len(want) {
				t.Fatalf("fixture did not drain real spills: files=%v metadata=%v batches=%d groups=%d/%d snapshot=%+v", liveFiles, liveMetadata, batches, len(seen), len(want), snapshot)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if budget.Used() != 0 || out.Rows != 0 {
				t.Fatalf("EOF/Close must release before QueryBudget.Close: rows=%d snapshot=%+v", out.Rows, budget.Snapshot())
			}
			if files, _ := aggregateMetadataFileMinimum(t, dir); files != 0 {
				t.Fatalf("EOF retained %d files", files)
			}
			if err := budget.Close(); err != nil {
				t.Fatal(err)
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
				t.Fatalf("query directory not removed: entries=%v err=%v", entries, err)
			}
			t.Logf("directory bytes=%d rows=%d groups=%d peak=%d hard=%d", len(dir), tc.batches*512, len(seen), snapshot.PeakBytes, snapshot.HardLimitBytes)
		})
	}
}
