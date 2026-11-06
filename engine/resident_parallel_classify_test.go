package engine

import (
	"context"
	"gocolumn/memory"
	"testing"
	"unsafe"
)

func TestParallelClassifyNullableOwnershipAndAccounting(t *testing.T) {
	const rows = 8193
	for _, workers := range []int{2, 3, 7} {
		opts := streamTestOptions()
		opts.Workers = workers
		opts.MemoryLimitBytes = 8 << 20
		eng := New(opts)
		query := partitionedQuery()
		query.Hints.Cardinality = 17
		prepared, err := eng.Prepare(context.Background(), &generatedStreamSource{rows: rows, groups: 17}, query, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		budget, err := eng.BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		ctx := memory.WithQueryBudget(context.Background(), budget)
		state, err := newResidentPartitionedState(ctx, prepared, budget)
		if err != nil {
			t.Fatal(err)
		}
		keys := make([]int32, rows)
		values := make([]int64, rows)
		keyValid := memory.EnsureBitmap(nil, rows)
		valueValid := memory.EnsureBitmap(nil, rows)
		selected := memory.EnsureBitmap(nil, rows)
		wantRows := 0
		wantSum := int64(0)
		expected := map[int32]partitionOracleState{}
		for row := 0; row < rows; row++ {
			keys[row] = int32(row%17 - 8)
			values[row] = int64(row%13 - 6)
			kv, vv := row%7 != 0, row%5 != 0
			if kv {
				memory.Set(keyValid, row)
			}
			if vv {
				memory.Set(valueValid, row)
			}
			if row%3 == 0 {
				continue
			}
			memory.Set(selected, row)
			wantRows++
			key := keys[row]
			if !kv {
				key = 999
				keys[row] = int32(row * 100)
			}
			current := expected[key]
			current.rows++
			if vv {
				current.sum += values[row]
				current.values++
				wantSum += values[row]
			}
			expected[key] = current
		}
		n, sum, err := state.aggregateCurrentBatch(ctx, aggregateBatchBinding{first: fixedColumn{typ: memory.TypeInt32, i32: keys, validity: keyValid}, values: &memory.Int64Vector{Values: values, Validity: valueValid}}, selected, rows)
		if err != nil || n != wantRows || sum != wantSum {
			t.Fatalf("workers=%d n=%d sum=%d err=%v", workers, n, sum, err)
		}
		if !state.parallelClassified {
			t.Fatal("test did not exercise parallel classification")
		}
		bytes := int64(cap(state.workerCounts)+cap(state.workerPositions))*int64(unsafe.Sizeof(int(0))) + int64(cap(state.scatterKeyFlags)+cap(state.scatterValueFlags))
		if state.parallelLease.Bytes() != bytes {
			t.Fatalf("parallel charged=%d actual=%d", state.parallelLease.Bytes(), bytes)
		}
		if err := state.visit(func(record aggregateRecord) error {
			key := int32(record.a)
			if record.validMask == 0 {
				key = 999
			}
			want, ok := expected[key]
			if !ok || record.state.Sum != want.sum || record.state.CountRows != want.rows || record.state.CountValues != want.values {
				t.Errorf("key=%d record=%+v expected=%+v", key, record.state, want)
			}
			delete(expected, key)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(expected) != 0 {
			t.Fatal("missing groups")
		}
		state.close()
		if budget.Used() != 0 {
			t.Fatalf("closed used=%d", budget.Used())
		}
		budget.Close()
		prepared.Close()
		eng.Close()
	}
}
