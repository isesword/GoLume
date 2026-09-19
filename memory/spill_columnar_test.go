package memory

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestColumnarRunRoundTripPreservesNativeWidths(t *testing.T) {
	schema := NewSchema(
		Field{Name: "i8", Type: TypeInt8, Nullable: true}, Field{Name: "i16", Type: TypeInt16, Nullable: true},
		Field{Name: "u8", Type: TypeUint8, Nullable: true}, Field{Name: "u16", Type: TypeUint16, Nullable: true},
		Field{Name: "f32", Type: TypeFloat32, Nullable: true},
	)
	b := NewBatch()
	b.Rows = 3
	validity := Bitmap{0b101}
	b.Int8s["i8"] = &Int8Vector{Values: []int8{-1, 99, 2}, Validity: validity}
	b.Int16s["i16"] = &Int16Vector{Values: []int16{-3, 99, 4}, Validity: validity}
	b.Uint8s["u8"] = &Uint8Vector{Values: []uint8{5, 99, 6}, Validity: validity}
	b.Uint16s["u16"] = &Uint16Vector{Values: []uint16{7, 99, 8}, Validity: validity}
	b.Float32s["f32"] = &Float32Vector{Values: []float32{
		math.Float32frombits(0x80000000), math.Float32frombits(0x7fc01234), math.Float32frombits(0x7fa00001),
	}, Validity: validity}
	engine := NewEngineBudget(1 << 20)
	q, err := engine.BeginQuery(BudgetOptions{HardLimitBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithQueryBudget(context.Background(), q)
	encoded, err := EncodeColumnarRun(ctx, schema, b, q.Operator("encode", MemorySpillBuffer))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeColumnarRun(ctx, schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: encoded.Payload}}, q.Operator("decode", MemoryWorkspace))
	if err != nil {
		t.Fatal(err)
	}
	// Header plus one validity word and native-width values for every field.
	const wantPayloadBytes = columnarRunHeader + (4 + 8 + 3) + (4 + 8 + 6) + (4 + 8 + 3) + (4 + 8 + 6) + (4 + 8 + 12)
	if got := len(encoded.Payload); got != wantPayloadBytes {
		t.Fatalf("native-width payload bytes=%d want=%d", got, wantPayloadBytes)
	}
	if got := decoded.Batch.Int8s["i8"].Values; got[0] != -1 || got[1] != 99 || got[2] != 2 {
		t.Fatalf("i8=%v", got)
	}
	if got := decoded.Batch.Int16s["i16"].Values; got[0] != -3 || got[1] != 99 || got[2] != 4 {
		t.Fatalf("i16=%v", got)
	}
	if got := decoded.Batch.Uint8s["u8"].Values; got[0] != 5 || got[1] != 99 || got[2] != 6 {
		t.Fatalf("u8=%v", got)
	}
	if got := decoded.Batch.Uint16s["u16"].Values; got[0] != 7 || got[1] != 99 || got[2] != 8 {
		t.Fatalf("u16=%v", got)
	}
	for _, name := range []string{"i8", "i16", "u8", "u16", "f32"} {
		var got Bitmap
		switch name {
		case "i8":
			got = decoded.Batch.Int8s[name].Validity
		case "i16":
			got = decoded.Batch.Int16s[name].Validity
		case "u8":
			got = decoded.Batch.Uint8s[name].Validity
		case "u16":
			got = decoded.Batch.Uint16s[name].Validity
		case "f32":
			got = decoded.Batch.Float32s[name].Validity
		}
		if len(got) != 1 || got[0] != validity[0] {
			t.Fatalf("%s validity=%v want=%v", name, got, validity)
		}
	}
	for row, want := range b.Float32s["f32"].Values {
		if got := math.Float32bits(decoded.Batch.Float32s["f32"].Values[row]); got != math.Float32bits(want) {
			t.Fatalf("f32 row %d bits=%#08x want=%#08x", row, got, math.Float32bits(want))
		}
	}
	if err := encoded.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnarRunRejectsPreviousInternalLayout(t *testing.T) {
	schema := NewSchema(Field{Name: "value", Type: TypeInt8})
	batch := NewBatch()
	batch.Rows = 1
	batch.Int8s["value"] = &Int8Vector{Values: []int8{7}}
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	payload := append([]byte(nil), encoded.Payload...)
	binary.LittleEndian.PutUint16(payload[len(columnarRunMagic):], 1)
	manager := NewBudgetManager(1 << 20)
	_, err = DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}, manager)
	if !errors.Is(err, ErrSpillCorrupt) {
		t.Fatalf("decode previous layout error=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("decode previous layout reservation leak=%d", manager.Used())
	}
}

func TestColumnarRunRejectsTruncatedNativeValues(t *testing.T) {
	schema := NewSchema(
		Field{Name: "i8", Type: TypeInt8}, Field{Name: "i16", Type: TypeInt16},
		Field{Name: "u8", Type: TypeUint8}, Field{Name: "u16", Type: TypeUint16},
		Field{Name: "f32", Type: TypeFloat32},
	)
	batch := NewBatch()
	batch.Rows = 3
	batch.Int8s["i8"] = &Int8Vector{Values: []int8{1, 2, 3}}
	batch.Int16s["i16"] = &Int16Vector{Values: []int16{4, 5, 6}}
	batch.Uint8s["u8"] = &Uint8Vector{Values: []uint8{7, 8, 9}}
	batch.Uint16s["u16"] = &Uint16Vector{Values: []uint16{10, 11, 12}}
	batch.Float32s["f32"] = &Float32Vector{Values: []float32{13, 14, 15}}
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Close()
	for cut := columnarRunHeader; cut < len(encoded.Payload); cut++ {
		manager := NewBudgetManager(1 << 20)
		payload := encoded.Payload[:cut]
		_, err := DecodeColumnarRun(context.Background(), schema, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}, manager)
		if !errors.Is(err, ErrSpillCorrupt) {
			t.Fatalf("cut=%d decode error=%v", cut, err)
		}
		if manager.Used() != 0 {
			t.Fatalf("cut=%d reservation leak=%d", cut, manager.Used())
		}
	}
}

func TestColumnarRunRoundTripNestedVectorsAndReservations(t *testing.T) {
	schema, batch := nestedSpillFixture()
	engine := NewEngineBudget(1 << 20)
	query, err := engine.BeginQuery(BudgetOptions{HardLimitBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithQueryBudget(context.Background(), query)
	encodeManager := query.Operator("SpillEncode", MemorySpillBuffer)
	encoded, err := EncodeColumnarRun(ctx, schema, batch, encodeManager)
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes := int64(len(encoded.Payload))
	if encodeManager.Used() != payloadBytes {
		t.Fatalf("encode reservation=%d want=%d", encodeManager.Used(), payloadBytes)
	}

	store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fingerprint := SchemaFingerprint(schema)
	handle, err := store.Spill(ctx, SpillObject{Kind: SpillColumnarRun, Partition: 3, Codec: ColumnarRunCodec, SchemaFingerprint: fingerprint, Payload: encoded.Payload})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoded.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoded.Close(); err != nil {
		t.Fatal(err)
	}
	if encodeManager.Used() != 0 {
		t.Fatalf("encode close reservation=%d", encodeManager.Used())
	}

	restoreManager := query.Operator("SpillRestore", MemorySpillBuffer)
	restored, err := store.Restore(ctx, handle, fingerprint, restoreManager)
	if err != nil {
		t.Fatal(err)
	}
	decodeManager := query.Operator("SpillDecode", MemoryWorkspace)
	decoded, err := DecodeColumnarRun(ctx, schema, restored, decodeManager)
	if err != nil {
		t.Fatal(err)
	}
	wantRestoreBytes := payloadBytes + int64(len(ColumnarRunCodec))
	if restoreManager.Used() != wantRestoreBytes || decodeManager.Used() != payloadBytes {
		t.Fatalf("restore=%d decode=%d payload=%d", restoreManager.Used(), decodeManager.Used(), payloadBytes)
	}
	assertNestedSpillFixture(t, decoded.Batch)

	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Close(); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if query.Used() != 0 || engine.Used() != 0 || store.Used() != 0 {
		t.Fatalf("cleanup query=%d engine=%d disk=%d", query.Used(), engine.Used(), store.Used())
	}
}

func TestColumnarRunRejectsOpaqueAndInvalidVectorsBeforeReservation(t *testing.T) {
	manager := NewBudgetManager(1 << 20)
	opaqueSchema := NewSchema(Field{Name: "value", Type: TypeArrow, ArrowType: "large_string"})
	opaqueBatch := NewBatch()
	opaqueBatch.Rows = 1
	opaqueBatch.Opaque["value"] = &OpaqueVector{Rows: 1, Type: TypeArrow, ArrowType: "large_string", Value: struct{}{}}
	if _, err := EncodeColumnarRun(context.Background(), opaqueSchema, opaqueBatch, manager); err == nil || !strings.Contains(err.Error(), "unsupported opaque spill codec") {
		t.Fatalf("opaque encode error=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("opaque encode leaked=%d", manager.Used())
	}

	invalidSchema := NewSchema(Field{Name: "name", Type: TypeString, Nullable: false, Encoding: EncodingView})
	invalidBatch := NewBatch()
	invalidBatch.Rows = 1
	invalidBatch.Strings["name"] = &StringViewVector{Offsets: []uint32{0, 0}, Validity: Bitmap{0}, Encoding: EncodingView}
	if _, err := EncodeColumnarRun(context.Background(), invalidSchema, invalidBatch, manager); err == nil || !strings.Contains(err.Error(), "non-nullable") {
		t.Fatalf("invalid encode error=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("invalid encode leaked=%d", manager.Used())
	}
}

func TestColumnarRunRejectsMalformedPayloadWithoutReservationLeak(t *testing.T) {
	schema := NewSchema(Field{Name: "name", Type: TypeString, Nullable: true, Encoding: EncodingView})
	batch := NewBatch()
	batch.Rows = 1
	batch.Strings["name"] = &StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("x"), Encoding: EncodingView}
	encoded, err := EncodeColumnarRun(context.Background(), schema, batch, NewBudgetManager(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), encoded.Payload...)
	if err := encoded.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "truncated", mutate: func(payload []byte) []byte { return payload[:len(payload)-1] }},
		{name: "trailing", mutate: func(payload []byte) []byte { return append(payload, 0xff) }},
		{name: "huge-row-count", mutate: func(payload []byte) []byte {
			binary.LittleEndian.PutUint64(payload[10:18], uint64(math.MaxInt))
			return payload
		}},
		{name: "bad-terminal-offset", mutate: func(payload []byte) []byte {
			// Header, nil validity, offset count, then offsets[0] and offsets[1].
			binary.LittleEndian.PutUint32(payload[columnarRunHeader+12:], 4096)
			return payload
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.mutate(append([]byte(nil), original...))
			restored := &RestoredSpill{Object: SpillObject{Kind: SpillColumnarRun, Codec: ColumnarRunCodec, SchemaFingerprint: SchemaFingerprint(schema), Payload: payload}}
			manager := NewBudgetManager(1 << 20)
			_, err := DecodeColumnarRun(context.Background(), schema, restored, manager)
			if !errors.Is(err, ErrSpillCorrupt) {
				t.Fatalf("decode error=%T %v", err, err)
			}
			if manager.Used() != 0 {
				t.Fatalf("decode reservation leak=%d", manager.Used())
			}
		})
	}
}

func TestColumnarRunProcessesTenTimesMemoryBudgetWithBoundedPeak(t *testing.T) {
	const hardLimit = int64(4096)
	engine := NewEngineBudget(hardLimit)
	query, err := engine.BeginQuery(BudgetOptions{SoftLimitBytes: 3500, HardLimitBytes: hardLimit})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithQueryBudget(context.Background(), query)
	store, err := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	schema := NewSchema(Field{Name: "value", Type: TypeString, Encoding: EncodingView})
	batch := NewBatch()
	batch.Rows = 64
	batch.Strings["value"] = repeatedStringVector(64, 16)
	fingerprint := SchemaFingerprint(schema)
	encodeManager := query.Operator("EncodeLoop", MemorySpillBuffer)
	restoreManager := query.Operator("RestoreLoop", MemorySpillBuffer)
	decodeManager := query.Operator("DecodeLoop", MemoryWorkspace)
	var peak, processed int64
	for partition := 0; processed < 10*hardLimit; partition++ {
		encoded, err := EncodeColumnarRun(ctx, schema, batch, encodeManager)
		if err != nil {
			t.Fatal(err)
		}
		processed += int64(len(encoded.Payload))
		peak = maxInt64(peak, query.Used())
		handle, err := store.Spill(ctx, SpillObject{Kind: SpillColumnarRun, Partition: int32(partition), Codec: ColumnarRunCodec, SchemaFingerprint: fingerprint, Payload: encoded.Payload})
		if err != nil {
			t.Fatal(err)
		}
		_ = encoded.Close()
		restored, err := store.Restore(ctx, handle, fingerprint, restoreManager)
		if err != nil {
			t.Fatal(err)
		}
		peak = maxInt64(peak, query.Used())
		decoded, err := DecodeColumnarRun(ctx, schema, restored, decodeManager)
		if err != nil {
			t.Fatal(err)
		}
		peak = maxInt64(peak, query.Used())
		if got := string(decoded.Batch.Strings["value"].ValueBytes(63)); got != fmt.Sprintf("%016d", 63) {
			t.Fatalf("decoded tail=%q", got)
		}
		_ = restored.Close()
		_ = decoded.Close()
		if err := store.Delete(ctx, handle); err != nil {
			t.Fatal(err)
		}
	}
	if peak > hardLimit || processed < 10*hardLimit {
		t.Fatalf("processed=%d peak=%d hard=%d", processed, peak, hardLimit)
	}
	if query.Used() != 0 || engine.Used() != 0 || store.Used() != 0 {
		t.Fatalf("loop cleanup query=%d engine=%d disk=%d", query.Used(), engine.Used(), store.Used())
	}
}

func TestMinimalSpillOperatorArbitratesTenTimesBudgetWithoutRecursivePressure(t *testing.T) {
	const hardLimit = int64(4096)
	engine := NewEngineBudget(hardLimit)
	query, err := engine.BeginQuery(BudgetOptions{
		SoftLimitBytes: 2800,
		HardLimitBytes: hardLimit,
		Spill:          SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithQueryBudget(context.Background(), query)
	store, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	schema := NewSchema(Field{Name: "value", Type: TypeString, Encoding: EncodingView})
	op := &minimalSpillOperator{
		query:    query,
		schema:   schema,
		store:    store,
		retained: query.Operator("MinimalSpillState", MemoryRetainedState),
		encoder:  query.Operator("MinimalSpillEncode", MemorySpillBuffer),
		restorer: query.Operator("MinimalSpillRestore", MemorySpillBuffer),
		decoder:  query.Operator("MinimalSpillDecode", MemoryWorkspace),
	}
	unregister, err := op.retained.RegisterSpiller(op)
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	defer op.close(ctx)

	batch := NewBatch()
	batch.Rows = 64
	batch.Strings["value"] = repeatedStringVector(64, 16)
	for range 40 {
		if err := op.add(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := op.Spill(withSpillArbitration(ctx), SpillRequest{TargetBytes: math.MaxInt64}); err != nil {
		t.Fatal(err)
	}
	rows, err := op.restoreAndDrain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	processed := int64(40) * BatchBytes(batch)
	if rows != 40*batch.Rows || processed < 10*hardLimit {
		t.Fatalf("rows=%d processed=%d", rows, processed)
	}
	if op.peak > hardLimit {
		t.Fatalf("peak=%d hard=%d", op.peak, hardLimit)
	}
	t.Logf("processed_bytes=%d peak_bytes=%d hard_limit=%d spill_runs=%d", processed, op.peak, hardLimit, op.spillRuns)
	if snapshot := query.Snapshot(); snapshot.SpillRequests == 0 || snapshot.SpilledBytes == 0 {
		t.Fatalf("spill arbitration was not exercised: %+v", snapshot)
	}
	if query.Used() != 0 || engine.Used() != 0 || store.(*QuerySpillStore).Used() != 0 {
		t.Fatalf("cleanup query=%d engine=%d disk=%d", query.Used(), engine.Used(), store.(*QuerySpillStore).Used())
	}
	if err := query.Close(); err != nil {
		t.Fatal(err)
	}
}

func nestedSpillFixture() (Schema, *Batch) {
	tagField := ListStringField("tags", true, true)
	profileFields := []Field{
		{Name: "score", Type: TypeFloat64, Nullable: false, Encoding: EncodingPlain},
		{Name: "nickname", Type: TypeString, Nullable: true, Encoding: EncodingView},
		ListStringField("labels", true, true),
	}
	schema := NewSchema(
		Field{Name: "id", Type: TypeInt64, Encoding: EncodingPlain},
		Field{Name: "name", Type: TypeString, Nullable: true, Encoding: EncodingView},
		Field{Name: "kind", Type: TypeString, Nullable: true, Encoding: EncodingDictionary, DictionaryDomain: "kind-v1"},
		tagField,
		StructField("profile", true, profileFields...),
	)
	batch := NewBatch()
	batch.Rows = 4
	batch.Int64s["id"] = &Int64Vector{Values: []int64{1, 2, 3, 4}, Encoding: EncodingPlain}
	batch.Strings["name"] = &StringViewVector{Offsets: []uint32{2, 3, 3, 5, 6}, Data: []byte("__abbc"), Validity: Bitmap{0b1101}, Encoding: EncodingView}
	batch.DictStrings["kind"] = &DictionaryStringVector{Indices: []uint32{0, 1, 0, 1}, Validity: Bitmap{0b1101}, Offsets: []uint32{2, 3, 5}, Data: []byte("__abb"), DictionaryDomain: "kind-v1", Encoding: EncodingDictionary}
	batch.StringLists["tags"] = &StringListVector{
		Offsets: []uint32{0, 2, 2, 2, 3}, Validity: Bitmap{0b1011}, Encoding: EncodingView,
		Values: &StringViewVector{Offsets: []uint32{0, 1, 1, 2}, Data: []byte("ab"), Validity: Bitmap{0b101}, Encoding: EncodingView},
	}
	profileBatch := NewBatch()
	profileBatch.Rows = 4
	profileBatch.Float64s["score"] = &Float64Vector{Values: []float64{1.5, 0, 2.5, 3.5}, Validity: Bitmap{0b1101}, Encoding: EncodingPlain}
	profileBatch.Strings["nickname"] = &StringViewVector{Offsets: []uint32{0, 1, 1, 1, 2}, Data: []byte("xy"), Validity: Bitmap{0b1001}, Encoding: EncodingView}
	profileBatch.StringLists["labels"] = &StringListVector{
		Offsets: []uint32{0, 1, 1, 1, 3}, Validity: Bitmap{0b1101}, Encoding: EncodingView,
		Values: &StringViewVector{Offsets: []uint32{0, 1, 2, 2}, Data: []byte("pq"), Validity: Bitmap{0b011}, Encoding: EncodingView},
	}
	batch.Structs["profile"] = &StructVector{Rows: 4, Fields: profileFields, Values: profileBatch, Validity: Bitmap{0b1101}, Encoding: EncodingView}
	return schema, batch
}

func assertNestedSpillFixture(t *testing.T, batch *Batch) {
	t.Helper()
	if batch == nil || batch.Rows != 4 {
		t.Fatalf("batch=%+v", batch)
	}
	if got := batch.Int64s["id"].Values; fmt.Sprint(got) != "[1 2 3 4]" {
		t.Fatalf("id=%v", got)
	}
	name := batch.Strings["name"]
	if string(name.ValueBytes(0)) != "a" || name.IsValid(1) || string(name.ValueBytes(2)) != "bb" || string(name.ValueBytes(3)) != "c" {
		t.Fatalf("name=%+v", name)
	}
	kind := batch.DictStrings["kind"]
	if kind.DictionaryDomain != "kind-v1" || string(kind.DictionaryValue(kind.Indices[0])) != "a" || kind.IsValid(1) || string(kind.DictionaryValue(kind.Indices[3])) != "bb" {
		t.Fatalf("kind=%+v", kind)
	}
	tags := batch.StringLists["tags"]
	if start, end, ok := tags.ValueRange(0); !ok || start != 0 || end != 2 || !tags.Values.IsValid(0) || tags.Values.IsValid(1) {
		t.Fatalf("tags row0=%d:%d ok=%v vector=%+v", start, end, ok, tags)
	}
	if start, end, ok := tags.ValueRange(1); !ok || start != end {
		t.Fatalf("tags row1=%d:%d ok=%v", start, end, ok)
	}
	if _, _, ok := tags.ValueRange(2); ok {
		t.Fatal("tags row2 should be NULL")
	}
	profile := batch.Structs["profile"]
	if !profile.IsValid(0) || profile.IsValid(1) || profile.Values.Float64s["score"].Values[2] != 2.5 || string(profile.Values.Strings["nickname"].ValueBytes(3)) != "y" {
		t.Fatalf("profile=%+v", profile)
	}
	labels := profile.Values.StringLists["labels"]
	if start, end, ok := labels.ValueRange(3); !ok || start != 1 || end != 3 || labels.Values.IsValid(2) {
		t.Fatalf("labels row3=%d:%d ok=%v", start, end, ok)
	}
}

func repeatedStringVector(rows, width int) *StringViewVector {
	offsets := make([]uint32, rows+1)
	data := make([]byte, 0, rows*width)
	for row := 0; row < rows; row++ {
		data = append(data, fmt.Sprintf("%0*d", width, row)...)
		offsets[row+1] = uint32(len(data))
	}
	return &StringViewVector{Offsets: offsets, Data: data, Encoding: EncodingView}
}

type retainedSpillBatch struct {
	batch       *Batch
	reservation Reservation
}

type minimalSpillOperator struct {
	query     *QueryBudget
	schema    Schema
	store     SpillManager
	retained  *OperatorBudget
	encoder   *OperatorBudget
	restorer  *OperatorBudget
	decoder   *OperatorBudget
	buffered  []retainedSpillBatch
	handles   []SpillHandle
	peak      int64
	spillRuns int
}

func (o *minimalSpillOperator) recordPeak() {
	o.peak = maxInt64(o.peak, o.query.Used())
}

func (o *minimalSpillOperator) add(ctx context.Context, batch *Batch) error {
	reservation, err := o.retained.Reserve(ctx, BatchBytes(batch))
	if err != nil {
		return err
	}
	owned, err := CloneBatch(batch)
	if err != nil {
		reservation.Release()
		return err
	}
	o.buffered = append(o.buffered, retainedSpillBatch{batch: owned, reservation: reservation})
	o.recordPeak()
	return nil
}

func (o *minimalSpillOperator) SpillableBytes() int64 {
	var bytes int64
	for _, item := range o.buffered {
		bytes += item.reservation.Bytes()
	}
	return bytes
}

func (o *minimalSpillOperator) Spill(ctx context.Context, request SpillRequest) (int64, error) {
	var released int64
	for len(o.buffered) > 0 && released < request.TargetBytes {
		item := o.buffered[0]
		encoded, err := EncodeColumnarRun(ctx, o.schema, item.batch, o.encoder)
		if err != nil {
			return released, err
		}
		o.recordPeak()
		handle, err := o.store.Spill(ctx, SpillObject{
			Kind: SpillColumnarRun, Partition: int32(len(o.handles)), Codec: ColumnarRunCodec,
			SchemaFingerprint: SchemaFingerprint(o.schema), Payload: encoded.Payload,
		})
		_ = encoded.Close()
		if err != nil {
			return released, err
		}
		o.buffered = o.buffered[1:]
		released += item.reservation.Bytes()
		item.reservation.Release()
		item.batch.ReleaseOwned()
		o.handles = append(o.handles, handle)
		o.spillRuns++
	}
	return released, nil
}

func (o *minimalSpillOperator) restoreAndDrain(ctx context.Context) (int, error) {
	rows := 0
	fingerprint := SchemaFingerprint(o.schema)
	for len(o.handles) > 0 {
		handle := o.handles[0]
		restored, err := o.store.Restore(ctx, handle, fingerprint, o.restorer)
		if err != nil {
			return rows, err
		}
		decoded, err := DecodeColumnarRun(ctx, o.schema, restored, o.decoder)
		if err != nil {
			_ = restored.Close()
			return rows, err
		}
		o.recordPeak()
		rows += decoded.Batch.Rows
		_ = restored.Close()
		_ = decoded.Close()
		if err := o.store.Delete(ctx, handle); err != nil {
			return rows, err
		}
		o.handles = o.handles[1:]
	}
	for _, item := range o.buffered {
		rows += item.batch.Rows
		item.batch.ReleaseOwned()
		item.reservation.Release()
	}
	o.buffered = nil
	return rows, nil
}

func (o *minimalSpillOperator) close(ctx context.Context) {
	for _, item := range o.buffered {
		item.batch.ReleaseOwned()
		item.reservation.Release()
	}
	o.buffered = nil
	for _, handle := range o.handles {
		_ = o.store.Delete(ctx, handle)
	}
	o.handles = nil
}

var _ SpillableConsumer = (*minimalSpillOperator)(nil)
