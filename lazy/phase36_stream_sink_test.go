package lazy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestPhase36CollectBatchesBorrowsOneGenerationAndReleasesBudget(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	first := memory.NewBatch()
	first.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2}}
	first.Reset(2)
	second := memory.NewBatch()
	second.Int64s["x"] = &memory.Int64Vector{Values: []int64{3, 4, 5}}
	second.Reset(3)
	src, err := inmemory.New(schema, first, second)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	session := NewSession(opts)
	defer session.Close()

	reader, err := session.FromSource(src).CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := reader.Schema(); len(got.Fields) != 1 || got.Fields[0].Name != "x" {
		t.Fatalf("reader schema=%+v", got)
	}
	if !reader.Next(context.Background()) {
		t.Fatalf("first Next: %v", reader.Err())
	}
	firstView := reader.Batch()
	firstSeries, err := firstView.Column("x")
	if err != nil {
		t.Fatal(err)
	}
	firstChunks, err := firstSeries.Int64Chunks()
	if err != nil || len(firstChunks) != 1 {
		t.Fatalf("first chunks=%d err=%v", len(firstChunks), err)
	}
	if value, valid, err := firstChunks[0].Value(1); err != nil || !valid || value != 2 {
		t.Fatalf("first value=(%d,%v,%v)", value, valid, err)
	}

	if !reader.Next(context.Background()) {
		t.Fatalf("second Next: %v", reader.Err())
	}
	if firstView.Valid() {
		t.Fatal("previous BatchView remained valid after Next")
	}
	if _, err := firstView.Column("x"); !errors.Is(err, ErrBatchViewExpired) {
		t.Fatalf("expired view error=%v", err)
	}
	if _, _, err := firstChunks[0].Value(0); !errors.Is(err, ErrDataFrameClosed) {
		t.Fatalf("expired chunk error=%v", err)
	}
	secondView := reader.Batch()
	if secondView.Len() != 3 {
		t.Fatalf("second batch rows=%d", secondView.Len())
	}
	if reader.Next(context.Background()) {
		t.Fatal("unexpected third batch")
	}
	if err := errors.Join(reader.Err(), reader.Close(), reader.Close()); err != nil {
		t.Fatal(err)
	}
	if secondView.Valid() {
		t.Fatal("last BatchView remained valid after exhaustion")
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("batch reader budget leak=%d", used)
	}
}

func TestPhase36PreparedBatchesAreReusableAndEarlyCloseIsBounded(t *testing.T) {
	schema, batches := phase36NestedBatches(t)
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).Select("id", "name").Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	for execution := 0; execution < 2; execution++ {
		reader, err := prepared.ExecuteBatches(context.Background(), BatchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reader.Next(context.Background()) {
			t.Fatalf("execution %d: %v", execution, reader.Err())
		}
		if reader.Batch().Len() == 0 {
			t.Fatalf("execution %d returned empty first batch", execution)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		if used := session.Engine().Memory().Used(); used != 0 {
			t.Fatalf("execution %d early-close leak=%d", execution, used)
		}
	}
}

func TestPhase36SinkBatchesUsesPerBatchBackpressure(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	batch.Reset(3)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()

	var entered atomic.Int32
	release := make(chan struct{})
	counts := [2]atomic.Int64{}
	consumer := func(index int) BatchConsumer {
		return func(_ context.Context, view BatchView) error {
			counts[index].Add(view.Len())
			if entered.Add(1) == 2 {
				close(release)
			}
			<-release
			return nil
		}
	}
	if err := session.FromSource(src).SinkBatches(context.Background(), BatchOptions{}, consumer(0), consumer(1)); err != nil {
		t.Fatal(err)
	}
	if counts[0].Load() != 3 || counts[1].Load() != 3 {
		t.Fatalf("consumer rows=(%d,%d)", counts[0].Load(), counts[1].Load())
	}

	consumerErr := errors.New("consumer failed")
	waitForCancel := func(ctx context.Context, _ BatchView) error {
		<-ctx.Done()
		return ctx.Err()
	}
	if err := session.FromSource(src).SinkBatches(context.Background(), BatchOptions{}, func(context.Context, BatchView) error {
		return consumerErr
	}, waitForCancel); !errors.Is(err, consumerErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer cancellation error=%v", err)
	}
	if err := session.FromSource(src).SinkBatches(context.Background(), BatchOptions{}, func(context.Context, BatchView) error {
		panic("consumer panic")
	}); err == nil || !strings.Contains(err.Error(), "batch consumer 0 panicked") || !strings.Contains(err.Error(), "consumer panic") {
		t.Fatalf("consumer panic error=%v", err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("consumer budget leak=%d", used)
	}
}

func TestPhase36IPCSinkCompletesWhenOutputExceedsMemoryLimit(t *testing.T) {
	const (
		rowsPerBatch = 1024
		batchCount   = 128
	)
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batches := make([]*memory.Batch, batchCount)
	for batchIndex := range batches {
		values := make([]int64, rowsPerBatch)
		for row := range values {
			values[row] = int64(batchIndex*rowsPerBatch + row)
		}
		batch := memory.NewBatch()
		batch.Int64s["x"] = &memory.Int64Vector{Values: values}
		batch.Reset(rowsPerBatch)
		batches[batchIndex] = batch
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 10
	session := NewSession(opts)
	defer session.Close()
	target := filepath.Join(t.TempDir(), "larger-than-memory.arrow")
	result, err := session.FromSource(src).
		WithExecution(ExecutionOptions{Policy: ExecutionBounded}).
		SinkIPC(context.Background(), target, IPCSinkOptions{SinkOptions: SinkOptions{Batch: BatchOptions{
			TargetBatchBytes: 8 << 10,
			MaxBatchBytes:    16 << 10,
			MaxInFlightBytes: 32 << 10,
		}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != rowsPerBatch*batchCount || result.Bytes <= opts.MemoryLimitBytes*10 {
		t.Fatalf("large sink result=%+v memory_limit=%d", result, opts.MemoryLimitBytes)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("large sink budget leak=%d", used)
	}
}

func TestPhase36IPCSinkPreservesNestedValuesAndSchema(t *testing.T) {
	schema, batches := phase36NestedBatches(t)
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	path := filepath.Join(t.TempDir(), "nested.arrow")
	result, err := session.FromSource(src).SinkIPC(context.Background(), path, IPCSinkOptions{Compression: IPCCompressionZstd})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 5 || result.Bytes <= 0 || len(result.Files) != 1 || result.Files[0] != path {
		t.Fatalf("sink result=%+v", result)
	}
	fileHandle, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fileHandle.Close()
	reader, err := ipc.NewFileReader(fileHandle)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.NumRecords() != len(batches) {
		t.Fatalf("IPC records=%d want=%d", reader.NumRecords(), len(batches))
	}
	for index, batch := range batches {
		want, err := memory.ArrowRecord(arrowmem.DefaultAllocator, schema, batch)
		if err != nil {
			t.Fatal(err)
		}
		got, err := reader.RecordBatchAt(index)
		if err != nil {
			want.Release()
			t.Fatal(err)
		}
		if !array.RecordEqual(want, got) {
			t.Fatalf("IPC record %d differs:\nwant=%s\ngot=%s", index, want, got)
		}
		got.Release()
		want.Release()
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("IPC sink budget leak=%d", used)
	}
}

func TestPhase36ParquetSinkPreservesNestedValuesAndSchema(t *testing.T) {
	schema, batches := phase36NestedBatches(t)
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	path := filepath.Join(t.TempDir(), "nested.parquet")
	result, err := session.FromSource(src).SinkParquet(context.Background(), path, ParquetSinkOptions{
		Compression:  ParquetCompressionSnappy,
		RowGroupRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 5 || result.Bytes <= 0 || len(result.Files) != 1 {
		t.Fatalf("sink result=%+v", result)
	}
	parquetReader, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer parquetReader.Close()
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, arrowmem.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	got, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer got.Release()
	if got.NumRows() != 5 || got.NumCols() != int64(len(schema.Fields)) {
		t.Fatalf("Parquet shape=(%d,%d)", got.NumRows(), got.NumCols())
	}
	for index, field := range schema.Fields {
		if got.Schema().Field(index).Name != field.Name {
			t.Fatalf("Parquet field %d=%q want=%q", index, got.Schema().Field(index).Name, field.Name)
		}
	}
	ids := got.Column(0).Data().Chunk(0).(*array.Int64)
	for row, want := range []int64{1, 2, 3, 4, 5} {
		if ids.Value(row) != want {
			t.Fatalf("Parquet id row %d=%d want=%d", row, ids.Value(row), want)
		}
	}
	names := got.Column(1).Data().Chunk(0).(*array.String)
	if names.Value(0) != "Alice" || !names.IsNull(1) || names.Value(2) != "" || names.Value(3) != "中" || names.Value(4) != "🙂" {
		t.Fatalf("Parquet names=%s", names)
	}
	tags := got.Column(2).Data().Chunk(0).(*array.List)
	if tags.IsNull(0) || tags.IsNull(1) || !tags.IsNull(2) || tags.IsNull(3) || tags.IsNull(4) {
		t.Fatalf("Parquet tag validity=%s", tags)
	}
	if start, end := tags.ValueOffsets(0); start != 0 || end != 2 {
		t.Fatalf("Parquet tags row0 offsets=%d:%d", start, end)
	}
	tagValues := tags.ListValues().(*array.String)
	if tagValues.Value(0) != "go" || !tagValues.IsNull(1) || tagValues.Value(3) != "SIMD" || tagValues.Value(4) != "中" {
		t.Fatalf("Parquet tag values=%s", tagValues)
	}
	profile := got.Column(3).Data().Chunk(0).(*array.Struct)
	if profile.IsNull(0) || !profile.IsNull(1) || profile.IsNull(2) || profile.IsNull(3) || profile.IsNull(4) {
		t.Fatalf("Parquet profile validity=%s", profile)
	}
	scores := profile.Field(0).(*array.Float64)
	if scores.Value(0) != 1.5 || !scores.IsNull(1) || scores.Value(2) != 3.5 || !scores.IsNull(3) || scores.Value(4) != 5.5 {
		t.Fatalf("Parquet profile scores=%s", scores)
	}
}

func TestPhase36SinkShardsInOrderAndCommitsAtomically(t *testing.T) {
	schema, batches := phase36NestedBatches(t)
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	target := filepath.Join(t.TempDir(), "parts")
	result, err := session.FromSource(src).SinkIPC(context.Background(), target, IPCSinkOptions{SinkOptions: SinkOptions{MaxRowsPerFile: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 5 || len(result.Files) != 3 {
		t.Fatalf("sharded result=%+v", result)
	}
	var ids []int64
	for index, path := range result.Files {
		if want := filepath.Join(target, fmt.Sprintf("part-%05d.arrow", index)); path != want {
			t.Fatalf("part %d path=%q want=%q", index, path, want)
		}
		fileHandle, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := ipc.NewFileReader(fileHandle)
		if err != nil {
			fileHandle.Close()
			t.Fatal(err)
		}
		var partRows int64
		for recordIndex := 0; recordIndex < reader.NumRecords(); recordIndex++ {
			record, err := reader.RecordBatchAt(recordIndex)
			if err != nil {
				reader.Close()
				fileHandle.Close()
				t.Fatal(err)
			}
			column := record.Column(0).(*array.Int64)
			for row := 0; row < column.Len(); row++ {
				ids = append(ids, column.Value(row))
			}
			partRows += record.NumRows()
			record.Release()
		}
		reader.Close()
		fileHandle.Close()
		if partRows == 0 || partRows > 2 {
			t.Fatalf("part %d rows=%d", index, partRows)
		}
	}
	if fmt.Sprint(ids) != "[1 2 3 4 5]" {
		t.Fatalf("sharded order=%v", ids)
	}
}

func TestPhase36SinkFailureLeavesNoTargetOrBudget(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "opaque", Type: memory.TypeArrow, Nullable: true})
	batch := memory.NewBatch()
	batch.Opaque["opaque"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: struct{}{}}
	batch.Reset(1)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	session := NewSession(opts)
	defer session.Close()
	parent := t.TempDir()
	target := filepath.Join(parent, "unsupported.arrow")
	if _, err := session.FromSource(src).SinkIPC(context.Background(), target, IPCSinkOptions{}); err == nil {
		t.Fatal("unsupported Arrow sink unexpectedly succeeded")
	}
	phase36RequireNoSinkArtifacts(t, parent, target)
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("failed sink budget leak=%d", used)
	}

	validSchema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	validBatch := memory.NewBatch()
	validBatch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	validBatch.Reset(3)
	validSource, _ := inmemory.New(validSchema, validBatch)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	canceledTarget := filepath.Join(parent, "canceled.parquet")
	if _, err := session.FromSource(validSource).SinkParquet(canceled, canceledTarget, ParquetSinkOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled sink error=%v", err)
	}
	phase36RequireNoSinkArtifacts(t, parent, canceledTarget)
}

func TestPhase36SinkBudgetFailureExistingTargetAndEmptyFile(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	batch.Reset(4)
	src, _ := inmemory.New(schema, batch)
	parent := t.TempDir()

	tinyOptions := engine.DefaultOptions()
	tinyOptions.MemoryLimitBytes = 64
	tiny := NewSession(tinyOptions)
	target := filepath.Join(parent, "budget.arrow")
	_, err := tiny.FromSource(src).SinkIPC(context.Background(), target, IPCSinkOptions{})
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("budget error=%v", err)
	}
	phase36RequireNoSinkArtifacts(t, parent, target)
	if used := tiny.Engine().Memory().Used(); used != 0 {
		t.Fatalf("budget failure leak=%d", used)
	}
	tiny.Close()

	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	existing := filepath.Join(parent, "existing.arrow")
	if err := os.WriteFile(existing, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := session.FromSource(src).SinkIPC(context.Background(), existing, IPCSinkOptions{}); !errors.Is(err, ErrSinkTargetExists) {
		t.Fatalf("existing target error=%v", err)
	}
	data, _ := os.ReadFile(existing)
	if string(data) != "keep" {
		t.Fatalf("existing target changed to %q", data)
	}
	result, err := session.FromSource(src).SinkIPC(context.Background(), existing, IPCSinkOptions{SinkOptions: SinkOptions{Overwrite: true}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 4 || result.Files[0] != existing {
		t.Fatalf("overwrite result=%+v", result)
	}
	fileHandle, err := os.Open(existing)
	if err != nil {
		t.Fatal(err)
	}
	overwritten, err := ipc.NewFileReader(fileHandle)
	if err != nil {
		fileHandle.Close()
		t.Fatal(err)
	}
	if overwritten.NumRecords() != 1 {
		t.Fatalf("overwrite records=%d", overwritten.NumRecords())
	}
	overwritten.Close()
	fileHandle.Close()

	empty := memory.NewBatch()
	empty.Int64s["x"] = &memory.Int64Vector{}
	empty.Reset(0)
	emptySource, _ := inmemory.New(schema, empty)
	emptyTarget := filepath.Join(parent, "empty.arrow")
	result, err = session.FromSource(emptySource).SinkIPC(context.Background(), emptyTarget, IPCSinkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 0 || len(result.Files) != 1 {
		t.Fatalf("empty result=%+v", result)
	}
	fileHandle, err = os.Open(emptyTarget)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewFileReader(fileHandle)
	if err != nil {
		fileHandle.Close()
		t.Fatal(err)
	}
	if reader.NumRecords() != 0 || !reader.Schema().Equal(phase36MustArrowSchema(t, schema)) {
		t.Fatalf("empty records=%d schema=%s", reader.NumRecords(), reader.Schema())
	}
	reader.Close()
	fileHandle.Close()
}

func TestPhase36TargetBytesPerFileIsAnUncompressedPlanningTarget(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5}}
	batch.Reset(5)
	options := SinkOptions{TargetBytesPerFile: 17}
	part := &sinkPart{}
	rows, estimated, err := chooseSinkRange(batch, 0, part, options)
	if err != nil || rows != 2 || estimated != 16 {
		t.Fatalf("first range rows=%d bytes=%d err=%v", rows, estimated, err)
	}
	part.rows, part.estimated = int64(rows), estimated
	rows, estimated, err = chooseSinkRange(batch, 2, part, options)
	if err != nil || rows != 0 || estimated != 0 {
		t.Fatalf("full part range rows=%d bytes=%d err=%v", rows, estimated, err)
	}
	if shouldRotateSinkPart(part, options) {
		t.Fatal("planning target incorrectly treated as a strict pre-write threshold")
	}
	oversized := &sinkPart{}
	rows, estimated, err = chooseSinkRange(batch, 0, oversized, SinkOptions{TargetBytesPerFile: 1})
	if err != nil || rows != 1 || estimated != 8 {
		t.Fatalf("oversized row rows=%d bytes=%d err=%v", rows, estimated, err)
	}
}

func phase36NestedBatches(t *testing.T) (memory.Schema, []*memory.Batch) {
	t.Helper()
	profileFields := []memory.Field{
		{Name: "score", Type: memory.TypeFloat64, Nullable: true},
		{Name: "nickname", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.ListStringField("labels", true, true),
	}
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "name", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.ListStringField("tags", true, true),
		memory.StructField("profile", true, profileFields...),
	)
	batch := memory.NewBatch()
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5}}
	batch.Strings["name"] = phase36StringVector([]string{"Alice", "", "", "中", "🙂"}, []bool{true, false, true, true, true})
	batch.StringLists["tags"] = &memory.StringListVector{
		Offsets:  []uint32{0, 2, 2, 2, 3, 5},
		Validity: memory.Bitmap{0b11011},
		Encoding: memory.EncodingView,
		Values: &memory.StringViewVector{
			Offsets:  []uint32{0, 2, 2, 2, 6, 9},
			Data:     []byte("goSIMD中"),
			Validity: memory.Bitmap{0b11101},
			Encoding: memory.EncodingView,
		},
	}
	profileValues := memory.NewBatch()
	profileValues.Reset(5)
	profileValues.Float64s["score"] = &memory.Float64Vector{Values: []float64{1.5, 0, 3.5, 0, 5.5}, Validity: memory.Bitmap{0b10101}}
	profileValues.Strings["nickname"] = phase36StringVector([]string{"a", "", "", "中", "z"}, []bool{true, false, true, true, true})
	profileValues.StringLists["labels"] = &memory.StringListVector{
		Offsets:  []uint32{0, 1, 1, 1, 2, 4},
		Validity: memory.Bitmap{0b11101},
		Encoding: memory.EncodingView,
		Values: &memory.StringViewVector{
			Offsets:  []uint32{0, 1, 1, 2, 3},
			Data:     []byte("xyz"),
			Validity: memory.Bitmap{0b1101},
			Encoding: memory.EncodingView,
		},
	}
	batch.Structs["profile"] = &memory.StructVector{Rows: 5, Fields: profileFields, Values: profileValues, Validity: memory.Bitmap{0b11101}, Encoding: memory.EncodingView}
	batch.Reset(5)
	if err := batch.Structs["profile"].Validate(schema.Fields[3], 5); err != nil {
		t.Fatal(err)
	}
	if err := batch.StringLists["tags"].Validate(5); err != nil {
		t.Fatal(err)
	}
	first, err := memory.CloneBatchRange(batch, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := memory.CloneBatchRange(batch, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	return schema, []*memory.Batch{first, second}
}

func phase36StringVector(values []string, validity []bool) *memory.StringViewVector {
	vector := &memory.StringViewVector{Offsets: make([]uint32, len(values)+1), Validity: memory.EnsureBitmap(nil, len(values)), Encoding: memory.EncodingView}
	for index, value := range values {
		if validity[index] {
			memory.Set(vector.Validity, index)
			vector.Data = append(vector.Data, value...)
		}
		vector.Offsets[index+1] = uint32(len(vector.Data))
	}
	return vector
}

func phase36RequireNoSinkArtifacts(t *testing.T, parent, target string) {
	t.Helper()
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed sink left target %q: %v", target, err)
	}
	matches, err := filepath.Glob(filepath.Join(parent, "."+filepath.Base(target)+".tmp-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("failed sink staging artifacts=%v err=%v", matches, err)
	}
}

func phase36MustArrowSchema(t *testing.T, schema memory.Schema) *arrow.Schema {
	t.Helper()
	converted, err := memory.ArrowSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	return converted
}
