package pipeline

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
)

type reviewThinArray struct{ arrow.Array }
type reviewPrivateExtension struct {
	arrow.ExtensionBase
	private    []byte
	references map[string]any
}
type reviewPrivateExtensionArray struct{ array.ExtensionArrayBase }

func (a *reviewPrivateExtensionArray) ValueStr(i int) string { return a.Storage().ValueStr(i) }

func (*reviewPrivateExtension) ArrayType() reflect.Type {
	return reflect.TypeOf(reviewPrivateExtensionArray{})
}
func (*reviewPrivateExtension) ExtensionName() string { return "golume.review.private" }
func (*reviewPrivateExtension) Serialize() string {
	panic("owned admission must not serialize private extension state")
}
func (t *reviewPrivateExtension) Deserialize(arrow.DataType, string) (arrow.ExtensionType, error) {
	return t, nil
}
func (t *reviewPrivateExtension) ExtensionEquals(other arrow.ExtensionType) bool { return t == other }

// Opaque compatibility is intentionally a legacy logical-size contribution,
// not a bound on extension metadata/backing. No Array/Type package whitelist or
// reflection/serialization of private custom state is part of native admission.
func TestOwnedReviewOpaqueCompatibility(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	boolBuilder := extensions.NewBool8Builder(allocator)
	boolBuilder.Append(true)
	boolArray := boolBuilder.NewArray()
	boolBuilder.Release()
	defer boolArray.Release()
	intBuilder := array.NewInt8Builder(allocator)
	intBuilder.Append(7)
	primitive := intBuilder.NewArray()
	intBuilder.Release()
	defer primitive.Release()
	customType := &reviewPrivateExtension{ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int8}, private: make([]byte, 1<<20), references: make(map[string]any)}
	customType.references["cycle"] = customType
	custom := array.NewExtensionArrayWithStorage(customType, primitive)
	defer custom.Release()
	timestampType := &arrow.TimestampType{Unit: arrow.Second, TimeZone: "+08:00"}
	tsBuilder := array.NewTimestampBuilder(allocator, timestampType)
	tsBuilder.Append(42)
	timestamp := tsBuilder.NewArray()
	tsBuilder.Release()
	defer timestamp.Release()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			timestampType.GetZone()
			timestampType.ClearCachedLocation()
			customType.references["counter"] = i
		}
	}()
	defer func() { <-done }()
	for _, tc := range []struct {
		name string
		a    arrow.Array
		want string
	}{
		{"official_bool8", boolArray, "true"},
		{"thin_standard_array_wrapper", &reviewThinArray{primitive}, "7"},
		{"private_extension", custom, "7"},
		{"timestamp", timestamp, timestamp.ValueStr(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := memory.NewBatch()
			src.Rows = 1
			src.Opaque["x"] = &memory.OpaqueVector{Rows: 1, Value: tc.a}
			src.Int64s["native"] = &memory.Int64Vector{Values: []int64{99}}
			estimate, err := memory.EstimateNativeOwnedBatchRangeBytes(src, 0, 1)
			if err != nil {
				t.Fatal(err)
			}
			charge := estimate.Bytes
			if !estimate.OpaquePresent || estimate.NativeBytes <= 8 || estimate.LegacyOpaqueBytes <= 0 || estimate.Bytes != estimate.NativeBytes+estimate.LegacyOpaqueBytes {
				t.Fatal("mixed components missing")
			}
			if charge >= 1<<20 {
				t.Fatal("private Opaque graph unexpectedly participates in native admission")
			}
			manager := memory.NewBudgetManager(charge)
			e, err := NewExchange(manager, charge, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 100; i++ {
				if err := e.enqueueRange(context.Background(), Sequence{}, src, MorselRange{Rows: 1, Bytes: 1}); err != nil {
					t.Fatal(err)
				}
				item := <-e.items
				out := item.batch.Opaque["x"].Value.(arrow.Array)
				if out.DataType() != tc.a.DataType() || out.ValueStr(0) != tc.want || item.batch.Int64s["native"].Values[0] != 99 {
					e.release(item)
					t.Fatal("legacy Opaque/native semantics changed")
				}
				if item.bytes != charge || manager.Used() != charge || e.Stats().CurrentInFlightBytes != charge {
					e.release(item)
					t.Fatal("admission charge differs across owners")
				}
				e.release(item)
				if manager.Used() != 0 || e.Stats().CurrentInFlightBytes != 0 {
					t.Fatal("reservation leaked")
				}
				if !e.bytes.TryAcquire(charge) {
					t.Fatal("global semaphore leaked")
				}
				e.bytes.Release(charge)
				if !e.groupBytes[0].TryAcquire(charge) {
					t.Fatal("group semaphore leaked")
				}
				e.groupBytes[0].Release(charge)
			}
		})
	}
}

// Owned probe selection may use interpolation, but every result must still
// equal the largest exact fit. These are tiny correctness fixtures, not timing
// benchmarks; abrupt variable payload and plateaus also bound probe progress.
func TestOwnedReviewLargestFitOracle(t *testing.T) {
	const rows = 257
	cases := []struct {
		name string
		base int64
		row  func(int) int64
	}{
		{"linear", 4096, func(int) int64 { return 32 }},
		{"fixed_only", 4096, func(int) int64 { return 0 }},
		{"zero", 0, func(int) int64 { return 0 }},
		{"late_spike", 4096, func(i int) int64 {
			if i == 250 {
				return 1 << 40
			}
			return 1
		}},
		{"early_spike", 4096, func(i int) int64 {
			if i == 8 {
				return 1 << 40
			}
			return 1
		}},
		{"alternating", 4096, func(i int) int64 {
			if i%2 == 0 {
				return 1024
			}
			return 0
		}},
		{"near_int64", math.MaxInt64 - 512, func(int) int64 { return 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prefix := make([]int64, rows+1)
			for i := 0; i < rows; i++ {
				prefix[i+1] = prefix[i] + tc.row(i)
			}
			cost := func(offset, length int) int64 { return tc.base + prefix[offset+length] - prefix[offset] }
			opts := Options{Workers: 1, TargetMorselBytes: 1, MaxMorselBytes: 2, MaxInFlightBytes: 1 << 20, MinMorselRows: 1, MaxMorselRows: rows}
			s, err := newOwnedByteSizer(memory.Schema{}, opts)
			if err != nil {
				t.Fatal(err)
			}
			batch := memory.NewBatch()
			batch.Rows = rows
			for _, offset := range []int{0, 7} {
				for _, limit := range []int64{tc.base - 1, tc.base, tc.base + 1, tc.base + 37, math.MaxInt64} {
					for _, guess := range []int{offset + 1, offset + 31, rows + 10} {
						probes := 0
						s.estimate = func(_ *memory.Batch, off, length int) (int64, error) {
							probes++
							if off != offset || length < 0 || length > rows-offset {
								t.Fatalf("invalid probe: offset=%d length=%d", off, length)
							}
							return cost(off, length), nil
						}
						wantEnd, wantBytes := offset, int64(0)
						for n := 1; n <= rows-offset; n++ {
							if got := cost(offset, n); got <= limit {
								wantEnd = offset + n
								wantBytes = got
							}
						}
						gotEnd, gotBytes, err := s.largestEndWithin(batch, offset, rows, guess, limit)
						if err != nil || gotEnd != wantEnd || gotBytes != wantBytes {
							t.Fatalf("offset=%d guess=%d limit=%d: got=(%d,%d,%v) want=(%d,%d)", offset, guess, limit, gotEnd, gotBytes, err, wantEnd, wantBytes)
						}
						if probes > 48 {
							t.Fatalf("probe selection failed bounded progress: %d probes", probes)
						}
					}
				}
			}
		})
	}
}

func TestOwnedReviewSmallBatchFastPathParity(t *testing.T) {
	for _, rows := range []int{1, 17, 64} {
		for _, cost := range []int64{0, 10, 100, 150} {
			opts := Options{Workers: 1, TargetMorselBytes: 100, MaxMorselBytes: 200, MaxInFlightBytes: 1 << 20, MinMorselRows: 32, MaxMorselRows: 64}
			owned, err := newOwnedByteSizer(memory.Schema{}, opts)
			if err != nil {
				t.Fatal(err)
			}
			reference, err := NewByteSizer(memory.Schema{}, opts)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			owned.estimate = func(_ *memory.Batch, _, _ int) (int64, error) { calls++; return cost, nil }
			reference.estimate = func(_ *memory.Batch, _, _ int) (int64, error) { return cost, nil }
			batch := memory.NewBatch()
			batch.Rows = rows
			got, err := owned.Split(batch)
			if err != nil {
				t.Fatal(err)
			}
			want, err := reference.Split(batch)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) || owned.ObservedBytesPerRow() != reference.ObservedBytesPerRow() {
				t.Fatalf("rows=%d cost=%d: got=%v want=%v or EWMA changed", rows, cost, got, want)
			}
			if (cost <= opts.TargetMorselBytes || rows <= opts.MinMorselRows) && calls != 1 {
				t.Fatalf("whole-batch fit must reuse its exact initial estimate: calls=%d", calls)
			}
		}
	}
}

// Bulk offset failures occur only after a bounded temporary allocation. The
// numeric map is cloned first, so this exercises rollback of partial output too.
func TestOwnedReviewMalformedBulkRollback(t *testing.T) {
	for _, kind := range []string{"string", "list_parent", "list_child"} {
		for _, start := range []int{0, 1} {
			t.Run(kind+string(rune('0'+start)), func(t *testing.T) {
				src := memory.NewBatch()
				src.Rows = start + 3
				src.Int64s["first"] = &memory.Int64Vector{Values: make([]int64, src.Rows)}
				bad := []uint32{0, 100000, 0, 1}
				good := []uint32{0, 0, 0, 1}
				if start == 1 {
					bad = append([]uint32{0}, bad...)
					good = append([]uint32{0}, good...)
				}
				original := append([]uint32(nil), bad...)
				var repair func()
				switch kind {
				case "string":
					v := &memory.StringViewVector{Offsets: bad, Data: []byte{'x'}}
					src.Strings["bad"] = v
					repair = func() { v.Offsets = good }
				case "list_parent":
					v := &memory.StringListVector{Offsets: bad, Values: &memory.StringViewVector{Offsets: []uint32{0, 1}, Data: []byte{'x'}}}
					src.StringLists["bad"] = v
					repair = func() { v.Offsets = good }
				case "list_child":
					parent := []uint32{0, 1, 2, 3}
					if start == 1 {
						parent = append([]uint32{0}, parent...)
					}
					child := &memory.StringViewVector{Offsets: []uint32{0, 100000, 0, 1}, Data: []byte{'x'}}
					src.StringLists["bad"] = &memory.StringListVector{Offsets: parent, Values: child}
					bad = child.Offsets
					original = append([]uint32(nil), bad...)
					repair = func() { child.Offsets = []uint32{0, 0, 0, 1} }
				}
				estimate, err := memory.EstimateNativeOwnedBatchRangeBytes(src, start, 3)
				if err != nil {
					t.Fatalf("bulk endpoint admission: %v", err)
				}
				// The numeric copy and at most two four-offset temporary buffers are
				// independent lower bounds, not a second call to the admission formula.
				if estimate.NativeBytes < 24+2*4*4 {
					t.Fatal("temporary buffers not covered")
				}
				manager := memory.NewBudgetManager(estimate.Bytes)
				e, err := NewExchange(manager, estimate.Bytes, 1, 1)
				if err != nil {
					t.Fatal(err)
				}
				if err = e.enqueueRange(context.Background(), Sequence{}, src, MorselRange{Offset: start, Rows: 3, Bytes: 1}); err == nil {
					t.Fatal("malformed copy succeeded")
				}
				if !reflect.DeepEqual(bad, original) {
					t.Fatal("source offsets mutated")
				}
				if manager.Used() != 0 || e.Stats().CurrentInFlightBytes != 0 || len(e.items) != 0 {
					t.Fatal("failed clone retained reservation/output")
				}
				if !e.bytes.TryAcquire(estimate.Bytes) {
					t.Fatal("global semaphore leaked")
				}
				e.bytes.Release(estimate.Bytes)
				if !e.groupBytes[0].TryAcquire(estimate.Bytes) {
					t.Fatal("group semaphore leaked")
				}
				e.groupBytes[0].Release(estimate.Bytes)
				// The same endpoint-valid corruption must remain a preflight
				// failure under row-appending Struct, even with an all-NULL parent.
				for _, validity := range []memory.Bitmap{nil, {0}} {
					parent := memory.NewBatch()
					parent.Rows = src.Rows
					field := memory.Field{Name: "bad", Type: memory.TypeString}
					if kind != "string" {
						field = memory.ListStringField("bad", true, true)
					}
					parent.Structs["s"] = &memory.StructVector{Rows: src.Rows, Values: src, Fields: []memory.Field{field}, Validity: validity}
					if _, err := memory.EstimateNativeOwnedBatchRangeBytes(parent, start, 3); err == nil {
						t.Fatal("Struct append lost full preflight validation")
					}
				}
				repair()
				if err = e.enqueueRange(context.Background(), Sequence{}, src, MorselRange{Offset: start, Rows: 3, Bytes: 1}); err != nil {
					t.Fatalf("reuse after rollback: %v", err)
				}
				e.release(<-e.items)
				if manager.Used() != 0 || e.Stats().CurrentInFlightBytes != 0 {
					t.Fatal("successful reuse leaked")
				}
			})
		}
	}
}
