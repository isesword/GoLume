package physical

import (
	"context"
	"math"
	"reflect"
	"testing"

	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestBindDecodedBatchIncludesEveryVectorMap(t *testing.T) {
	decoded, dst := memory.NewBatch(), memory.NewBatch()
	decoded.Rows = 3
	// Enumerate Batch's maps so adding a physical vector cannot silently leave
	// spill output binding behind again. The vectors themselves are borrowed.
	fields := reflect.ValueOf(decoded).Elem()
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		if field.Kind() != reflect.Map {
			continue
		}
		field.SetMapIndex(reflect.ValueOf("value"), reflect.New(field.Type().Elem().Elem()))
		reflect.ValueOf(dst).Elem().Field(i).SetMapIndex(reflect.ValueOf("stale"), reflect.New(field.Type().Elem().Elem()))
	}
	bindDecodedBatch(dst, decoded)
	if dst.Rows != decoded.Rows {
		t.Fatalf("rows=%d want=%d", dst.Rows, decoded.Rows)
	}
	bound := reflect.ValueOf(dst).Elem()
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		if field.Kind() != reflect.Map {
			continue
		}
		value := bound.Field(i).MapIndex(reflect.ValueOf("value"))
		if bound.Field(i).Len() != 1 || !value.IsValid() || value.Pointer() != field.MapIndex(reflect.ValueOf("value")).Pointer() {
			t.Errorf("%s did not replace stale bindings with the decoded vector", fields.Type().Field(i).Name)
		}
	}
}

type spillOutputReleaseCounter struct{ releases int }

func (c *spillOutputReleaseCounter) Release() { c.releases++ }

func TestBindDecodedBatchReleaseLifecycle(t *testing.T) {
	stale, current := &spillOutputReleaseCounter{}, &spillOutputReleaseCounter{}
	dst, decoded := memory.NewBatch(), memory.NewBatch()
	dst.Opaque["stale"] = &memory.OpaqueVector{Value: stale, Owned: true}
	children := memory.NewBatch()
	children.Opaque["value"] = &memory.OpaqueVector{Value: current, Owned: true}
	decoded.Structs["nested"] = &memory.StructVector{Values: children}
	bindDecodedBatch(dst, decoded)
	if stale.releases != 1 || current.releases != 0 {
		t.Fatalf("bind released stale=%d current=%d", stale.releases, current.releases)
	}
	// The next stream call clears the borrowed output before closing its owner.
	dst.ClearColumns()
	decoded.ReleaseOwned()
	dst.ClearColumns()
	if stale.releases != 1 || current.releases != 1 {
		t.Fatalf("clear released stale=%d current=%d", stale.releases, current.releases)
	}
}

var spillFloat32Bits = [...]uint32{0x00000000, 0x80000000, 0x7f800000, 0xff800000, 0x7fc01234, 0x00000001, 0x7f7fffff, 0xbfa00000}

func spillNarrowSource(t *testing.T, start, count, repeats int) *inmemory.Source {
	t.Helper()
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "i8", Type: memory.TypeInt8, Nullable: true},
		memory.Field{Name: "i16", Type: memory.TypeInt16, Nullable: true},
		memory.Field{Name: "u8", Type: memory.TypeUint8, Nullable: true},
		memory.Field{Name: "u16", Type: memory.TypeUint16, Nullable: true},
		memory.Field{Name: "f32", Type: memory.TypeFloat32, Nullable: true},
	)
	var batches []*memory.Batch
	const batchRows = 63 // Exercise validity offsets across word boundaries.
	for offset := 0; offset < count*repeats; offset += batchRows {
		rows := min(batchRows, count*repeats-offset)
		b := memory.NewBatch()
		b.Rows = rows
		valid := make(memory.Bitmap, memory.WordsFor(rows))
		b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, rows)}
		b.Int8s["i8"] = &memory.Int8Vector{Values: make([]int8, rows), Validity: valid}
		b.Int16s["i16"] = &memory.Int16Vector{Values: make([]int16, rows), Validity: valid}
		b.Uint8s["u8"] = &memory.Uint8Vector{Values: make([]uint8, rows), Validity: valid}
		b.Uint16s["u16"] = &memory.Uint16Vector{Values: make([]uint16, rows), Validity: valid}
		b.Float32s["f32"] = &memory.Float32Vector{Values: make([]float32, rows), Validity: valid}
		for row := 0; row < rows; row++ {
			id := start + count - 1 - (offset+row)/repeats
			b.Int64s["id"].Values[row] = int64(id)
			b.Int8s["i8"].Values[row] = int8(id)
			b.Int16s["i16"].Values[row] = int16(id*257 - 32768)
			b.Uint8s["u8"].Values[row] = uint8(id)
			b.Uint16s["u16"].Values[row] = uint16(id * 257)
			b.Float32s["f32"].Values[row] = math.Float32frombits(spillFloat32Bits[id%len(spillFloat32Bits)])
			if id%7 != 6 {
				memory.Set(valid, row)
			}
		}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func checkSpillNarrowRow(t *testing.T, b *memory.Batch, row, id int, suffix string) {
	t.Helper()
	i8, i16 := b.Int8s["i8"+suffix], b.Int16s["i16"+suffix]
	u8, u16 := b.Uint8s["u8"+suffix], b.Uint16s["u16"+suffix]
	f32 := b.Float32s["f32"+suffix]
	if i8 == nil || i16 == nil || u8 == nil || u16 == nil || f32 == nil {
		t.Fatalf("spill output is missing narrow columns (suffix=%q)", suffix)
	}
	if len(i8.Values) != b.Rows || len(i16.Values) != b.Rows || len(u8.Values) != b.Rows || len(u16.Values) != b.Rows || len(f32.Values) != b.Rows {
		t.Fatalf("narrow vector lengths differ from batch rows=%d", b.Rows)
	}
	valid := id >= 0 && id%7 != 6
	if i8.IsValid(row) != valid || i16.IsValid(row) != valid || u8.IsValid(row) != valid || u16.IsValid(row) != valid || f32.IsValid(row) != valid {
		t.Fatalf("id=%d suffix=%q: narrow validity changed", id, suffix)
	}
	if !valid {
		return
	}
	if i8.Values[row] != int8(id) || i16.Values[row] != int16(id*257-32768) || u8.Values[row] != uint8(id) || u16.Values[row] != uint16(id*257) || math.Float32bits(f32.Values[row]) != spillFloat32Bits[id%len(spillFloat32Bits)] {
		t.Fatalf("id=%d suffix=%q: narrow payload changed: i8=%d i16=%d u8=%d u16=%d f32=%08x", id, suffix, i8.Values[row], i16.Values[row], u8.Values[row], u16.Values[row], math.Float32bits(f32.Values[row]))
	}
}

func spillOutputQuery(t *testing.T) (context.Context, *memory.EngineBudget, *memory.QueryBudget) {
	t.Helper()
	const hard = 64 << 10
	engineBudget := memory.NewEngineBudget(hard)
	query, err := engineBudget.BeginQuery(memory.BudgetOptions{
		SoftLimitBytes: 48 << 10, HardLimitBytes: hard,
		Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := query.Close(); err != nil {
			t.Error(err)
		}
	})
	return memory.WithQueryBudget(context.Background(), query), engineBudget, query
}

func checkSpillOutputCleanup(t *testing.T, stream source.BatchStream, query *memory.QueryBudget) {
	t.Helper()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot := query.Snapshot(); snapshot.UsedBytes != 0 || snapshot.PeakBytes > query.Limit() {
		t.Fatalf("spill memory accounting: %+v", snapshot)
	}
	store, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if bytes := store.(*memory.QuerySpillStore).Used(); bytes != 0 {
		t.Fatalf("spill bytes after close=%d", bytes)
	}
}

func TestExternalSortSpillPreservesNarrowColumns(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx, budget, query := spillOutputQuery(t)
			const rows = 2048
			ordered, err := NewSortSourceWithOptions(spillNarrowSource(t, 0, rows, 1), []SortKey{{Column: "id"}}, budget, SortSourceOptions{Spillable: true})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := ordered.(*SortSource).NewBatchStream(ctx, source.ReadOptions{Kernels: kernel.New(mode)})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			total, batches := 0, 0
			for {
				ok, err := stream.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				batches++
				for row := 0; row < out.Rows; row++ {
					if id := out.Int64s["id"].Values[row]; id != int64(total) {
						t.Fatalf("row=%d id=%d", total, id)
					}
					checkSpillNarrowRow(t, out, row, total, "")
					total++
				}
			}
			external := stream.(*externalSortStream)
			if total != rows || batches < 2 || external.spillRuns <= externalSortFanIn || external.mergeLevels == 0 {
				t.Fatalf("rows=%d batches=%d diagnostics=%v", total, batches, external.StreamDiagnostics())
			}
			if out.Rows != 0 || len(out.Int8s) != 0 || len(out.Float32s) != 0 {
				t.Fatal("EOF retained borrowed output")
			}
			checkSpillOutputCleanup(t, stream, query)
		})
	}
}

func TestJoinSpillPreservesNarrowColumns(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
		for _, typ := range []join.Type{join.Inner, join.Full} {
			t.Run(mode.String()+"/"+typ.String(), func(t *testing.T) {
				ctx, budget, query := spillOutputQuery(t)
				kernels := kernel.New(mode)
				joined, err := NewJoinSourceWithOptions(ctx, spillNarrowSource(t, 0, 256, 1), spillNarrowSource(t, 128, 256, 2), typ, []JoinKey{{Left: "id", Right: "id"}}, "_r", budget, JoinSourceOptions{
					Kernels: kernels, RadixBits: 4, Spillable: true, RequireBounded: true, SpillConfigured: true, MemoryLimitBytes: query.Limit(),
				})
				if err != nil {
					t.Fatal(err)
				}
				defer joined.Close()
				stream, err := joined.NewBatchStream(ctx, source.ReadOptions{Kernels: kernels})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				spilled, ok := stream.(*spillableJoinStream)
				if !ok {
					t.Fatalf("join used %T instead of real spill", stream)
				}
				seen := make(map[[2]int]int)
				out, batches := memory.NewBatch(), 0
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					batches++
					for row := 0; row < out.Rows; row++ {
						left, right := -1, -1
						if out.Int64s["id"].IsValid(row) {
							left = int(out.Int64s["id"].Values[row])
						}
						if out.Int64s["id_r"].IsValid(row) {
							right = int(out.Int64s["id_r"].Values[row])
						}
						checkSpillNarrowRow(t, out, row, left, "")
						checkSpillNarrowRow(t, out, row, right, "_r")
						seen[[2]int{left, right}]++
					}
				}
				want := make(map[[2]int]int)
				for id := 0; id < 384; id++ {
					switch {
					case id >= 128 && id < 256:
						want[[2]int{id, id}] = 2
					case typ == join.Full && id < 128:
						want[[2]int{id, -1}] = 1
					case typ == join.Full:
						want[[2]int{-1, id}] = 2
					}
				}
				if !reflect.DeepEqual(seen, want) {
					t.Fatalf("join pair multiplicities differ: got=%v want=%v", seen, want)
				}
				if batches < 2 || spilled.spillRuns == 0 || spilled.spillBytes == 0 {
					t.Fatalf("batches=%d diagnostics=%v", batches, spilled.StreamDiagnostics())
				}
				if out.Rows != 0 || len(out.Int8s) != 0 || len(out.Float32s) != 0 {
					t.Fatal("EOF retained borrowed output")
				}
				checkSpillOutputCleanup(t, stream, query)
			})
		}
	}
}
