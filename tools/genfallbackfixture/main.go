// Command genfallbackfixture writes a synthetic functional-test substitute,
// not a reproduction of the repository's original benchmark dataset.
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	parquetsource "gocolumn/source/parquet"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

const (
	fixtureRows  = 1048576
	selectedRows = 472475
	groupCount   = 219196
	checksum     = int64(236716301)
	rowGroupRows = 65521 // Deliberately exercise partial batches and a final tail.
)

type aggregate struct {
	sum   int64
	count uint64
}

// fixtureRow uses an odd affine permutation over a power-of-two row count.
// Repeated keys and selected/rejected rows therefore span row groups.
func fixtureRow(row int) (pred int64, key int32, value int64) {
	i := (row*65537 + 12345) & (fixtureRows - 1)
	if i < selectedRows {
		value = int64(i%1001 + 1)
		if i == 0 {
			value += 7823
		}
		return 32768 + int64(i%1000), int32(i % groupCount), value
	}
	return 32767 - int64(i%1000), int32(groupCount + i%257), -int64(i%1000 + 1)
}

func writeFixture(path string) (map[int32]aggregate, error) {
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("pred", parquet.Repetitions.Required, -1),
		schema.NewInt32Node("key", parquet.Repetitions.Required, -1),
		schema.NewInt64Node("value", parquet.Repetitions.Required, -1),
	}, -1)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy), parquet.WithDictionaryDefault(false))
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	defer w.Close()
	oracle := make(map[int32]aggregate, groupCount)
	for start := 0; start < fixtureRows; start += rowGroupRows {
		n := min(rowGroupRows, fixtureRows-start)
		pred, keys, values := make([]int64, n), make([]int32, n), make([]int64, n)
		for row := 0; row < n; row++ {
			pred[row], keys[row], values[row] = fixtureRow(start + row)
			if pred[row] > 32767 {
				a := oracle[keys[row]]
				a.sum += values[row]
				a.count++
				oracle[keys[row]] = a
			}
		}
		rg, err := w.AppendRowGroupChecked()
		if err != nil {
			return nil, err
		}
		for _, vector := range []any{pred, keys, values} {
			column, err := rg.NextColumn()
			if err != nil {
				return nil, err
			}
			switch v := vector.(type) {
			case []int64:
				_, err = column.(*file.Int64ColumnChunkWriter).WriteBatch(v, nil, nil)
			case []int32:
				_, err = column.(*file.Int32ColumnChunkWriter).WriteBatch(v, nil, nil)
			}
			if err != nil {
				return nil, err
			}
			if err := column.Close(); err != nil {
				return nil, err
			}
		}
		if err := rg.Close(); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return oracle, nil
}

func verifyFixture(path string, oracle map[int32]aggregate) error {
	src, err := parquetsource.OpenFile(path)
	if err != nil {
		return err
	}
	stats, err := src.Stats(context.Background())
	if err != nil {
		return err
	}
	if stats.Rows != fixtureRows || stats.Partitions != (fixtureRows+rowGroupRows-1)/rowGroupRows {
		return fmt.Errorf("Parquet metadata differs: rows=%d row_groups=%d", stats.Rows, stats.Partitions)
	}
	for name, typ := range map[string]memory.DataType{"pred": memory.TypeInt64, "key": memory.TypeInt32, "value": memory.TypeInt64} {
		field, ok := src.Schema().Field(name)
		if !ok || field.Type != typ || field.Nullable {
			return fmt.Errorf("unexpected schema for %s", name)
		}
	}
	for _, strategy := range []string{"direct-fused", "bitmap-direct", "radix"} {
		opts := engine.DefaultOptions()
		opts.Workers = 4
		eng := engine.New(opts)
		q := expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(32767)), GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")}, Hints: expr.Hints{Selectivity: .451, Cardinality: 1 << 18, ForceStrategy: strategy, ForceWorkerCount: 4}}
		result, err := eng.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
		eng.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", strategy, err)
		}
		if result.GroupCount != groupCount || result.SelectedRows != selectedRows || result.Checksum != checksum || len(result.Rows) != len(oracle) {
			return fmt.Errorf("%s: summary mismatch groups=%d selected=%d sum=%d", strategy, result.GroupCount, result.SelectedRows, result.Checksum)
		}
		seen := make(map[int32]bool, len(oracle))
		for _, row := range result.Rows {
			if len(row.Keys) != 1 || !row.Keys[0].Valid {
				return fmt.Errorf("%s: invalid key", strategy)
			}
			if row.Keys[0].Type != memory.TypeInt32 || row.Keys[0].Int64 < 0 || row.Keys[0].Int64 >= groupCount {
				return fmt.Errorf("%s: out-of-range key %d", strategy, row.Keys[0].Int64)
			}
			key := int32(row.Keys[0].Int64)
			want, ok := oracle[key]
			if !ok || seen[key] || row.Sum != want.sum || row.Count != want.count {
				return fmt.Errorf("%s: group %d differs from row-loop oracle", strategy, key)
			}
			seen[key] = true
		}
		fmt.Printf("%s: verified all %d groups against independent row-loop oracle\n", strategy, len(oracle))
	}
	return nil
}

func main() {
	path := flag.String("out", "testdata/bench-1m.synthetic.parquet", "new synthetic output file; refuses to overwrite")
	flag.Parse()
	oracle, err := writeFixture(*path)
	if err == nil {
		err = verifyFixture(*path, oracle)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	f, err := os.Open(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("synthetic-only path=%s rows=%d row_groups=%d bytes=%d sha256=%x\n", *path, fixtureRows, (fixtureRows+rowGroupRows-1)/rowGroupRows, n, h.Sum(nil))
}
