package lazy

import (
	"bytes"
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type aggregateMetadataMatrixKey struct {
	text  string
	a     int64
	b     uint32
	c     int32
	valid uint8
}

type aggregateMetadataMatrixFixture struct {
	src   *inmemory.Source
	keys  []string
	want  map[aggregateMetadataMatrixKey]aggregateMetadataResultRow
	dict  source.Dictionary
	codes map[string]uint32
}

func aggregateMetadataMatrixKeyForID(kind string, id int) aggregateMetadataMatrixKey {
	if kind == "string" || kind == "dictionary" {
		key := aggregateMetadataMatrixKey{}
		if id != 0 {
			key.valid = 1
			if id != 1 {
				key.text = fmt.Sprintf("中🙂%04d", id)
			}
		}
		return key
	}
	key := aggregateMetadataMatrixKey{}
	if id > 1 {
		key.a, key.valid = 1<<40+int64(id), key.valid|1
	}
	if id != 0 && id != 2 {
		key.b, key.valid = uint32(id*13%89), key.valid|2
	}
	if kind == "generic" && id != 0 {
		key.c, key.valid = -int32(id), key.valid|4
	}
	return key
}

func newAggregateMetadataMatrixFixture(t *testing.T, kind string) aggregateMetadataMatrixFixture {
	t.Helper()
	const batches, batchRows, groups = 32, 512, 1024
	fixture := aggregateMetadataMatrixFixture{want: make(map[aggregateMetadataMatrixKey]aggregateMetadataResultRow)}
	var fields []memory.Field
	switch kind {
	case "string", "dictionary":
		fixture.keys = []string{"key"}
		field := memory.Field{Name: "key", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}
		if kind == "dictionary" {
			field.Encoding, field.DictionaryDomain = memory.EncodingDictionary, "aggregate-metadata-key-matrix"
			fixture.dict = source.Dictionary{Domain: field.DictionaryDomain, Offsets: make([]uint32, groups+1)}
			fixture.codes = make(map[string]uint32, groups)
			// Deliberately permute domain-local codes rather than making the
			// logical group number its code. Code zero is used only by NULL.
			values := make([]string, groups)
			for id := 1; id < groups; id++ {
				key := aggregateMetadataMatrixKeyForID(kind, id)
				code := uint32(id * 37 % groups)
				values[code], fixture.codes[key.text] = key.text, code
			}
			for code, value := range values {
				fixture.dict.Data = append(fixture.dict.Data, value...)
				fixture.dict.Offsets[code+1] = uint32(len(fixture.dict.Data))
			}
		}
		fields = []memory.Field{field}
	case "pair", "generic":
		fixture.keys = []string{"a", "b"}
		fields = []memory.Field{
			{Name: "a", Type: memory.TypeInt64, Nullable: true},
			{Name: "b", Type: memory.TypeUint32, Nullable: true},
		}
		if kind == "generic" {
			fixture.keys = append(fixture.keys, "c")
			fields = append(fields, memory.Field{Name: "c", Type: memory.TypeInt32, Nullable: true})
		}
	default:
		t.Fatalf("unknown matrix key kind %q", kind)
	}
	fields = append(fields, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true})
	input := make([]*memory.Batch, 0, batches)
	for batchIndex := 0; batchIndex < batches; batchIndex++ {
		batch := memory.NewBatch()
		values := &memory.Int64Vector{Values: make([]int64, batchRows), Validity: memory.EnsureBitmap(nil, batchRows)}
		batch.Int64s["value"] = values
		switch kind {
		case "string":
			batch.Strings["key"] = &memory.StringViewVector{Offsets: make([]uint32, batchRows+1), Validity: memory.EnsureBitmap(nil, batchRows), Encoding: memory.EncodingView}
		case "dictionary":
			batch.DictStrings["key"] = &memory.DictionaryStringVector{Indices: make([]uint32, batchRows), Validity: memory.EnsureBitmap(nil, batchRows), Offsets: fixture.dict.Offsets, Data: fixture.dict.Data, DictionaryDomain: fixture.dict.Domain, Encoding: memory.EncodingDictionary}
		default:
			batch.Int64s["a"] = &memory.Int64Vector{Values: make([]int64, batchRows), Validity: memory.EnsureBitmap(nil, batchRows)}
			batch.Uint32s["b"] = &memory.Uint32Vector{Values: make([]uint32, batchRows), Validity: memory.EnsureBitmap(nil, batchRows)}
			if kind == "generic" {
				batch.Int32s["c"] = &memory.Int32Vector{Values: make([]int32, batchRows), Validity: memory.EnsureBitmap(nil, batchRows)}
			}
		}
		for row := 0; row < batchRows; row++ {
			ordinal := batchIndex*batchRows + row
			id := ordinal % groups
			key := aggregateMetadataMatrixKeyForID(kind, id)
			value := int64(id%17-8) + int64((ordinal/groups)%5-2)
			values.Values[row] = value
			// Group 7 has only NULL values; all other groups include valid
			// values, with changing sums/minima/maxima across input rounds.
			validValue := id != 7 && ordinal%11 != 0
			if validValue {
				memory.Set(values.Validity, row)
			}
			switch kind {
			case "string":
				vector := batch.Strings["key"]
				vector.Data = append(vector.Data, key.text...)
				vector.Offsets[row+1] = uint32(len(vector.Data))
				if key.valid != 0 {
					memory.Set(vector.Validity, row)
				}
			case "dictionary":
				vector := batch.DictStrings["key"]
				if key.valid != 0 {
					vector.Indices[row] = fixture.codes[key.text]
					memory.Set(vector.Validity, row)
				}
			default:
				batch.Int64s["a"].Values[row], batch.Uint32s["b"].Values[row] = key.a, key.b
				if key.valid&1 != 0 {
					memory.Set(batch.Int64s["a"].Validity, row)
				}
				if key.valid&2 != 0 {
					memory.Set(batch.Uint32s["b"].Validity, row)
				}
				if kind == "generic" {
					batch.Int32s["c"].Values[row] = key.c
					if key.valid&4 != 0 {
						memory.Set(batch.Int32s["c"].Validity, row)
					}
				}
			}
			result := fixture.want[key]
			result.rows++
			if validValue {
				if result.values == 0 || value < result.min {
					result.min = value
				}
				if result.values == 0 || value > result.max {
					result.max = value
				}
				result.sum += value
				result.values++
				result.valid = [4]bool{true, true, true, true}
				result.avg = float64(result.sum) / float64(result.values)
			}
			fixture.want[key] = result
		}
		batch.Reset(batchRows)
		input = append(input, batch)
	}
	var err error
	fixture.src, err = inmemory.New(memory.NewSchema(fields...), input...)
	if err != nil {
		t.Fatal(err)
	}
	if kind == "dictionary" {
		fixture.src.WithDictionary("key", fixture.dict)
	}
	if len(fixture.want) != groups {
		t.Fatalf("fixture key collision: groups=%d want=%d", len(fixture.want), groups)
	}
	return fixture
}

func (f aggregateMetadataMatrixFixture) readKey(t *testing.T, kind string, batch *memory.Batch, row int) aggregateMetadataMatrixKey {
	t.Helper()
	key := aggregateMetadataMatrixKey{}
	switch kind {
	case "string":
		vector := batch.Strings["key"]
		if vector == nil {
			t.Fatal("missing string-view key")
		}
		if vector.IsValid(row) {
			key.text, key.valid = string(vector.ValueBytes(row)), 1
		}
	case "dictionary":
		vector := batch.DictStrings["key"]
		if vector == nil || vector.DictionaryDomain != f.dict.Domain || vector.Encoding != memory.EncodingDictionary {
			t.Fatal("dictionary key lost encoding or domain")
		}
		if !bytes.Equal(vector.Data, f.dict.Data) || len(vector.Offsets) != len(f.dict.Offsets) {
			t.Fatal("dictionary key lost its stable codebook")
		}
		for i, offset := range vector.Offsets {
			if offset != f.dict.Offsets[i] {
				t.Fatal("dictionary offsets changed")
			}
		}
		if vector.IsValid(row) {
			code := vector.Indices[row]
			key.text, key.valid = string(vector.DictionaryValue(code)), 1
			if expected, exists := f.codes[key.text]; !exists || code != expected {
				t.Fatalf("dictionary value %q code=%d want=%d exists=%v", key.text, code, expected, exists)
			}
		}
	default:
		if batch.Int64s["a"] == nil || batch.Uint32s["b"] == nil {
			t.Fatal("missing fixed-width key components")
		}
		if batch.Int64s["a"].IsValid(row) {
			key.a, key.valid = batch.Int64s["a"].Values[row], key.valid|1
		}
		if batch.Uint32s["b"].IsValid(row) {
			key.b, key.valid = batch.Uint32s["b"].Values[row], key.valid|2
		}
		if kind == "generic" {
			if batch.Int32s["c"] == nil {
				t.Fatal("missing third generic key component")
			}
			if batch.Int32s["c"].IsValid(row) {
				key.c, key.valid = batch.Int32s["c"].Values[row], key.valid|4
			}
		}
	}
	return key
}

func readAggregateMetadataMatrixResult(t *testing.T, batch *memory.Batch, row int) aggregateMetadataResultRow {
	t.Helper()
	result := aggregateMetadataResultRow{
		rows: batch.Uint64s["rows"].Values[row], values: batch.Uint64s["values"].Values[row],
		valid: [4]bool{batch.Int64s["sum"].IsValid(row), batch.Int64s["min"].IsValid(row), batch.Int64s["max"].IsValid(row), batch.Float64s["avg"].IsValid(row)},
	}
	if !batch.Uint64s["rows"].IsValid(row) || !batch.Uint64s["values"].IsValid(row) {
		t.Fatal("COUNT returned NULL")
	}
	if result.valid[0] {
		result.sum = batch.Int64s["sum"].Values[row]
	}
	if result.valid[1] {
		result.min = batch.Int64s["min"].Values[row]
	}
	if result.valid[2] {
		result.max = batch.Int64s["max"].Values[row]
	}
	if result.valid[3] {
		result.avg = batch.Float64s["avg"].Values[row]
	}
	return result
}

// The wrapper deliberately exposes no prepared-partition or pushdown method:
// its scan stays serial, and the probe cannot be bypassed by a projected clone.
// Disappearance of a previously live run while another input batch is being
// read proves input compaction, rather than deletion during the final merge.
type aggregateMetadataMatrixSource struct {
	source.Source
	dir             string
	previous        map[string]bool
	compactionSteps int
}

func (s *aggregateMetadataMatrixSource) Replayability() source.Replayability {
	return source.ReplayStable
}

func (s *aggregateMetadataMatrixSource) Dictionary(ctx context.Context, name string) (source.Dictionary, error) {
	return s.Source.(source.DictionaryProvider).Dictionary(ctx, name)
}

func (s *aggregateMetadataMatrixSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	stream, err := s.Source.NewBatchStream(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &aggregateMetadataMatrixInput{BatchStream: stream, owner: s}, nil
}

type aggregateMetadataMatrixInput struct {
	source.BatchStream
	owner *aggregateMetadataMatrixSource
}

func (s *aggregateMetadataMatrixInput) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	ok, err := s.BatchStream.Next(ctx, dst)
	if err != nil || !ok {
		return ok, err
	}
	live := make(map[string]bool)
	if err := filepath.WalkDir(s.owner.dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".gls") {
			live[path] = true
		}
		return nil
	}); err != nil {
		return false, err
	}
	for path := range s.owner.previous {
		if !live[path] {
			s.owner.compactionSteps++
			break
		}
	}
	s.owner.previous = live
	return true, nil
}

func TestBoundedAggregateMetadataKeyMatrix(t *testing.T) {
	for _, kind := range []string{"string", "dictionary", "pair", "generic"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newAggregateMetadataMatrixFixture(t, kind)
			query := func(session *Session, input source.Source) LazyFrame {
				return session.FromSource(input).GroupBy(fixture.keys...).Agg(
					Sum("value", "sum"), Count("rows"), CountColumn("value", "values"),
					Min("value", "min"), Max("value", "max"), Avg("value", "avg"),
				)
			}
			resident := NewSession(engine.DefaultOptions())
			defer resident.Close()
			wantFrame, err := query(resident, fixture.src).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer wantFrame.Close()
			want := make(map[aggregateMetadataMatrixKey]aggregateMetadataResultRow)
			for _, batch := range wantFrame.batches() {
				for row := 0; row < batch.Rows; row++ {
					key := fixture.readKey(t, kind, batch, row)
					result := readAggregateMetadataMatrixResult(t, batch, row)
					expected, exists := fixture.want[key]
					_, duplicate := want[key]
					if !exists || duplicate || result != expected {
						t.Fatalf("resident key=%+v got=%+v want=%+v exists=%v duplicate=%v", key, result, expected, exists, duplicate)
					}
					want[key] = result
				}
			}
			if len(want) != len(fixture.want) {
				t.Fatalf("resident groups=%d want=%d", len(want), len(fixture.want))
			}

			dir := aggregateMetadataLongDirectory(t, 180)
			input := &aggregateMetadataMatrixSource{Source: fixture.src, dir: dir}
			opts := engine.DefaultOptions()
			opts.Workers, opts.Pipeline.Workers = 1, 1
			opts.MemoryLimitBytes = 256 << 10
			opts.QueryMemorySoftLimitBytes = 96 << 10
			opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 32 << 20}
			opts.ExecutionPolicy = engine.ExecutionBounded
			bounded := NewSession(opts)
			defer bounded.Close()
			prepared, err := query(bounded, input).Prepare(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			aggregateSource, ok := prepared.src.(*aggregateResultSource)
			if !ok || !aggregateSource.spillable {
				t.Fatalf("expected spillable aggregate source, got %T", prepared.src)
			}
			plan := aggregateSource.query.Plan()
			if plan.GroupColumns != len(fixture.keys) {
				t.Fatalf("prepared key columns=%d want=%d", plan.GroupColumns, len(fixture.keys))
			}
			// Three native components cannot use any specialized key shape.
			// Canonical framing must be reflected in the actual prepared plan.
			if kind == "generic" && (plan.GroupColumns != 3 || plan.KeyWidthBytes <= 8+4+4) {
				t.Fatalf("fixture did not select canonical generic keys: %+v", plan)
			}
			if kind == "dictionary" && plan.KeyWidthBytes != 4 {
				t.Fatalf("fixture did not retain specialized dictionary codes: %+v", plan)
			}
			budget, err := bounded.Engine().BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			diagnostics, ok := stream.(interface {
				Stats() engine.AggregateStreamStats
			})
			if !ok {
				t.Fatalf("aggregate stream %T has no spill diagnostics", stream)
			}
			out := memory.NewBatch()
			seen := make(map[aggregateMetadataMatrixKey]bool)
			for {
				ok, err := stream.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if snapshot := budget.Snapshot(); snapshot.PeakBytes > opts.MemoryLimitBytes || snapshot.Operators["HashAggregateOutput"] == 0 {
					t.Fatalf("invalid live output accounting: %+v", snapshot)
				}
				for row := 0; row < out.Rows; row++ {
					key := fixture.readKey(t, kind, out, row)
					result := readAggregateMetadataMatrixResult(t, out, row)
					expected, exists := want[key]
					if !exists || seen[key] || result != expected {
						t.Fatalf("bounded key=%+v got=%+v want=%+v exists=%v duplicate=%v", key, result, expected, exists, seen[key])
					}
					seen[key] = true
				}
			}
			stats, snapshot := diagnostics.Stats(), budget.Snapshot()
			if len(seen) != len(want) || stats.Groups != len(want) || stats.SelectedRows != 32*512 || stats.SpillRuns < 2 || stats.SpillBytes == 0 || snapshot.SpillRequests < 2 || input.compactionSteps < 2 {
				t.Fatalf("missing real repeated compaction: groups=%d/%d input_compactions=%d stats=%+v budget=%+v", len(seen), len(want), input.compactionSteps, stats, snapshot)
			}
			if budget.Used() != 0 || out.Rows != 0 || snapshot.PeakBytes <= 0 || snapshot.PeakBytes > opts.MemoryLimitBytes {
				t.Fatalf("EOF did not release before Close: rows=%d budget=%+v", out.Rows, snapshot)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if budget.Used() != 0 || out.Rows != 0 {
				t.Fatalf("Close did not release before QueryBudget.Close: rows=%d budget=%+v", out.Rows, budget.Snapshot())
			}
			if files, _ := aggregateMetadataFileMinimum(t, dir); files != 0 {
				t.Fatalf("EOF/Close retained %d spill files", files)
			}
			if err := budget.Close(); err != nil {
				t.Fatal(err)
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
				t.Fatalf("query spill directories remain: entries=%v err=%v", entries, err)
			}
			t.Logf("key=%s groups=%d input_compactions=%d spill_runs=%d peak=%d hard=%d", kind, len(seen), input.compactionSteps, stats.SpillRuns, snapshot.PeakBytes, opts.MemoryLimitBytes)
		})
	}
}
