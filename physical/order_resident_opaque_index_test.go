package physical

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
)

type opaqueRefCounts struct{ retains, releases int }
type countedOpaqueData struct {
	arrow.ArrayData
	counts *opaqueRefCounts
}

func (d *countedOpaqueData) Retain()  { d.counts.retains++; d.ArrayData.Retain() }
func (d *countedOpaqueData) Release() { d.counts.releases++; d.ArrayData.Release() }

type opaqueValueData struct {
	arrow.ArrayData
	marker any
}
type opaqueFloatData struct {
	arrow.ArrayData
	marker float64
}
type opaqueDataArray struct {
	arrow.Array
	data arrow.ArrayData
}

func (a opaqueDataArray) Data() arrow.ArrayData { return a.data }

func opaqueIndexTestArray(t *testing.T, alloc arrowmem.Allocator) arrow.Array {
	t.Helper()
	builder := array.NewLargeStringBuilder(alloc)
	builder.Append("payload")
	a := builder.NewArray()
	builder.Release()
	t.Cleanup(a.Release)
	return a
}
func opaqueIndexTestInsert(t *testing.T, s *residentOpaqueSourceIndex, data arrow.ArrayData, hash uint64, comparable bool) bool {
	t.Helper()
	_, found := s.find(data, hash, comparable)
	if found {
		return false
	}
	n, _, err := s.growth()
	if err != nil {
		t.Fatal(err)
	}
	if n != len(s.slots) {
		s.resize(n)
	}
	at, _ := s.find(data, hash, false)
	data.Retain()
	s.slots[at] = residentOpaqueSourceSlot{data: data, hash: hash}
	s.count++
	return true
}

func TestResidentOpaqueIndexIdentityAndCollision(t *testing.T) {
	a := opaqueIndexTestArray(t, arrowmem.DefaultAllocator)
	counts := &opaqueRefCounts{}
	root := &countedOpaqueData{ArrayData: a.Data(), counts: counts}
	s := residentOpaqueSourceIndex{seed: maphash.MakeSeed()}
	defer s.close()
	// Every test key deliberately collides in all 64 bits, not just bucket bits.
	hash := uint64(0xeadf14b89430bc79)
	comparable := opaqueValueData{ArrayData: root, marker: "comparable"}
	noncomparable := opaqueValueData{ArrayData: root, marker: []int{1}}
	if !reflect.TypeOf(noncomparable).Comparable() || reflect.ValueOf(noncomparable).Comparable() {
		t.Fatal("fixture must require value-level Comparable")
	}
	for _, data := range []arrow.ArrayData{root, comparable, noncomparable} {
		_, eq, err := s.key(data)
		if err != nil {
			t.Fatal(err)
		}
		if !opaqueIndexTestInsert(t, &s, data, hash, eq) {
			t.Fatal("distinct data was merged")
		}
	}
	if opaqueIndexTestInsert(t, &s, root, hash, true) || opaqueIndexTestInsert(t, &s, comparable, hash, true) {
		t.Fatal("equal identity not deduplicated")
	}
	if !opaqueIndexTestInsert(t, &s, noncomparable, hash, false) {
		t.Fatal("non-comparable value incorrectly deduplicated")
	}
	last := opaqueValueData{ArrayData: root, marker: "after-non-comparable"}
	if !opaqueIndexTestInsert(t, &s, last, hash, true) {
		t.Fatal("new colliding identity merged")
	}
	if _, found := s.find(last, hash, true); !found {
		t.Fatal("lookup past non-comparable slots failed")
	}
	if _, found := s.find(opaqueValueData{ArrayData: root, marker: "absent"}, hash, true); found {
		t.Fatal("missing identity matched")
	}
	if counts.retains != 5 || counts.releases != 0 || s.count != 5 {
		t.Fatal(counts, s.count)
	}
	// A matching bucket with different high hash bits must not compare as equal.
	if _, ok := s.find(root, hash^(uint64(1)<<63), true); ok {
		t.Fatal("64-bit hash truncated")
	}
	before := *counts
	s.resize(32)
	if *counts != before {
		t.Fatal("rehash changed reference counts")
	}
	for _, data := range []arrow.ArrayData{root, comparable} {
		if _, ok := s.find(data, hash, true); !ok {
			t.Fatal("rehash lost identity")
		}
	}
	s.close()
	s.close()
	if counts.retains != 5 || counts.releases != 5 || len(s.slots) != 0 || s.count != 0 {
		t.Fatal("unbalanced close", counts)
	}
}

func TestResidentOpaqueIndexComparableValuesAndNaN(t *testing.T) {
	a := opaqueIndexTestArray(t, arrowmem.DefaultAllocator)
	counts := &opaqueRefCounts{}
	root := &countedOpaqueData{ArrayData: a.Data(), counts: counts}
	s := residentOpaqueSourceIndex{seed: maphash.MakeSeed()}
	defer s.close()
	for _, marker := range []float64{1, 1, math.NaN(), math.NaN()} {
		data := opaqueFloatData{ArrayData: root, marker: marker}
		hash, eq, err := s.key(data)
		if err != nil || !eq {
			t.Fatal(eq, err)
		}
		opaqueIndexTestInsert(t, &s, data, hash, eq)
	}
	if s.count != 3 || counts.retains != 3 {
		t.Fatal("Go value identity changed", s.count, counts)
	}
	s.close()
	if counts.releases != 3 {
		t.Fatal(counts)
	}
}

func TestResidentOpaqueIndexGrowthPlan(t *testing.T) {
	width := int64(unsafe.Sizeof(residentOpaqueSourceSlot{}))
	for _, tc := range []struct {
		capacity, count, want int
		bytes                 int64
	}{{0, 0, 8, 8 * width}, {8, 3, 8, 0}, {8, 4, 16, 16 * width}, {16, 8, 32, 32 * width}} {
		n, bytes, err := residentOpaqueSourceGrowth(tc.capacity, tc.count)
		if err != nil || n != tc.want || bytes != tc.bytes {
			t.Fatal(tc, n, bytes, err)
		}
	}
	for _, tc := range [][2]int{{-1, 0}, {0, -1}, {math.MaxInt, math.MaxInt}, {math.MaxInt / 2, math.MaxInt / 2}, {int(math.MaxInt64 / width), math.MaxInt}} {
		if _, _, err := residentOpaqueSourceGrowth(tc[0], tc[1]); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal("overflow accepted", tc, err)
		}
	}
}

func TestResidentOpaqueIndexDeniedGrowthUnchanged(t *testing.T) {
	for _, existing := range []int{0, 4, 8, 16, 32, 64} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			ctx := context.Background()
			w, parent := externalAccumulatorTestWorkspace(t, 0)
			w.bulkCredits = true
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			t.Cleanup(func() { alloc.AssertSize(t, 0) })
			arrays := make([]arrow.Array, existing+1)
			counts := make([]*opaqueRefCounts, len(arrays))
			data := make([]arrow.ArrayData, len(arrays))
			for i := range arrays {
				arrays[i] = opaqueIndexTestArray(t, alloc)
				counts[i] = &opaqueRefCounts{}
				data[i] = &countedOpaqueData{ArrayData: arrays[i].Data(), counts: counts[i]}
			}
			batch := memory.NewBatch()
			batch.Rows = 1
			v := &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: arrays[0]}
			batch.Opaque["raw"] = v
			owner, err := newResidentOpaqueRows(ctx, memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow}), batch, w)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.close()
			for i := 0; i < existing; i++ {
				v.Value = arrays[i]
				if err = owner.appendValue(ctx, &owner.fields[0], v, 0, data[i]); err != nil {
					t.Fatal(err)
				}
				owner.rows++
			}
			field := &owner.fields[0]
			slots := append([]residentOpaqueSourceSlot(nil), field.sources.slots...)
			bytes, used, capacity := owner.reservation.Bytes(), w.Used(), w.Limit()
			valuesCap, seed := cap(field.values), field.sources.seed
			parent.deny = true
			v.Value = arrays[existing]
			if err = owner.appendValue(ctx, &owner.fields[0], v, 0, data[existing]); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
			if owner.rows != existing || len(field.values) != existing || cap(field.values) != valuesCap || field.sources.count != existing || !reflect.DeepEqual(slots, field.sources.slots) || seed != field.sources.seed || owner.reservation.Bytes() != bytes || w.Used() != used || w.Limit() != capacity {
				t.Fatal("denied admission mutated state")
			}
			if counts[existing].retains != 0 || counts[existing].releases != 0 {
				t.Fatal("denied source touched", counts[existing])
			}
			parent.deny = false
			if err = owner.appendValue(ctx, &owner.fields[0], v, 0, data[existing]); err != nil {
				t.Fatal(err)
			}
			owner.close()
			w.close()
			if w.Used() != 0 || parent.Used() != 0 {
				t.Fatal("budget leak")
			}
			for _, c := range counts {
				if c.retains != 1 || c.releases != 1 {
					t.Fatal("reference leak", c)
				}
			}
		})
	}
}

func TestResidentOpaqueIndexOwnerNonComparableAndReorder(t *testing.T) {
	for _, noncomparable := range []bool{false, true} {
		t.Run(fmt.Sprint(noncomparable), func(t *testing.T) {
			ctx := context.Background()
			w, _ := externalAccumulatorTestWorkspace(t, 0)
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			t.Cleanup(func() { alloc.AssertSize(t, 0) })
			builder := array.NewLargeStringBuilder(alloc)
			builder.Append("payload")
			var a arrow.Array = builder.NewArray()
			builder.Release()
			defer func() {
				if a != nil {
					a.Release()
				}
			}()
			counts := &opaqueRefCounts{}
			tracked := &countedOpaqueData{ArrayData: a.Data(), counts: counts}
			var marker any = "stable"
			if noncomparable {
				marker = []int{1}
			}
			data := opaqueValueData{ArrayData: tracked, marker: marker}
			batch := memory.NewBatch()
			batch.Rows = 1
			batch.Opaque["raw"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: a}
			schema := memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow})
			owner, err := newResidentOpaqueRows(ctx, schema, batch, w)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.close()
			for i := 0; i < 20; i++ {
				if err = owner.appendValue(ctx, &owner.fields[0], batch.Opaque["raw"], 0, data); err != nil {
					t.Fatal(err)
				}
				owner.rows++
			}
			want := 1
			if noncomparable {
				want = 20
			}
			if counts.retains != want {
				t.Fatal("wrong owner retains", counts, want)
			}
			out, err := newResidentOpaqueRows(ctx, schema, batch, w)
			if err != nil {
				t.Fatal(err)
			}
			defer out.close()
			for i := 19; i >= 0; i-- {
				if err = out.appendFrom(ctx, owner, i); err != nil {
					t.Fatal(err)
				}
			}
			if counts.retains != 2*want {
				t.Fatal("origin not preserved", counts, want)
			}
			a.Release()
			a = nil
			batch.Opaque["raw"].Value = nil
			rowRef := owner.fields[0].values[0]
			owner.close()
			if rowRef.source != nil || rowRef.Value != nil {
				t.Fatal("closed row retains origin")
			}
			dst := memory.NewBatch()
			if err = out.bindRow(dst, 0); err != nil {
				t.Fatal(err)
			}
			if dst.Opaque["raw"].Value.(*array.LargeString).Value(0) != "payload" {
				t.Fatal("reorder backing lost")
			}
			borrowed := dst.Opaque["raw"]
			out.close()
			out.close()
			if borrowed.Value != nil {
				t.Fatal("borrowed output not invalidated")
			}
			if counts.retains != 2*want || counts.releases != 2*want || w.Used() != 0 {
				t.Fatal("release imbalance", counts, w.Used())
			}
		})
	}
}

func TestResidentOpaqueIndexCapacityAdmission(t *testing.T) {
	ctx := context.Background()
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	a := opaqueIndexTestArray(t, arrowmem.DefaultAllocator)
	batch := memory.NewBatch()
	batch.Rows = 1
	v := &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: a}
	batch.Opaque["raw"] = v
	owner, err := newResidentOpaqueRows(ctx, memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow}), batch, w)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	expected := owner.reservation.Bytes()
	valueCapacity, indexCapacity := 0, 0
	for i := 0; i < 129; i++ {
		// Fresh Data roots share actual buffers but must remain distinct identities.
		sliced := array.NewSlice(a, 0, 1)
		v.Value = sliced
		backing, e := topKArrowDataBytes(sliced.Data())
		if e != nil {
			t.Fatal(e)
		}
		headers, e := residentOpaqueHeaderBytes(sliced.Data())
		if e != nil {
			t.Fatal(e)
		}
		expected += backing + headers + int64(unsafe.Sizeof(residentOpaqueValue{})) + 8
		if i == valueCapacity {
			valueCapacity = max(1, 2*valueCapacity)
			expected += int64(valueCapacity) * int64(unsafe.Sizeof((*residentOpaqueValue)(nil)))
		}
		if i >= indexCapacity/2 {
			indexCapacity = max(8, 2*indexCapacity)
			expected += int64(indexCapacity) * int64(unsafe.Sizeof(residentOpaqueSourceSlot{}))
		}
		err = owner.appendRow(ctx, batch, 0)
		sliced.Release()
		if err != nil {
			t.Fatal(err)
		}
		if owner.reservation.Bytes() != expected || len(owner.fields[0].sources.slots) != indexCapacity || owner.fields[0].sources.count != i+1 {
			t.Fatal("capacity/backing admission mismatch", i, owner.reservation.Bytes(), expected)
		}
	}
}

func TestResidentOpaqueIndexTypedNilRejected(t *testing.T) {
	var data *countedOpaqueData
	s := residentOpaqueSourceIndex{seed: maphash.MakeSeed()}
	if _, _, err := s.key(data); err == nil {
		t.Fatal("typed nil data accepted")
	}
	var a *array.LargeString
	if !residentOpaqueIsNil(a) || !residentOpaqueIsNil(map[int]int(nil)) || !residentOpaqueIsNil([]int(nil)) || !residentOpaqueIsNil((func())(nil)) {
		t.Fatal("nil kind not recognized")
	}
	ctx := context.Background()
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	b := memory.NewBatch()
	b.Rows = 1
	b.Opaque["raw"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: a}
	if _, err := newResidentOpaqueRows(ctx, memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow}), b, w); err == nil || w.Used() != 0 {
		t.Fatal("nil array admitted", err, w.Used())
	}
}

func TestResidentOpaqueIndexPointerLookupNoAllocation(t *testing.T) {
	a := opaqueIndexTestArray(t, arrowmem.DefaultAllocator)
	s := residentOpaqueSourceIndex{seed: maphash.MakeSeed()}
	hash, eq, err := s.key(a.Data())
	if err != nil {
		t.Fatal(err)
	}
	opaqueIndexTestInsert(t, &s, a.Data(), hash, eq)
	defer s.close()
	if n := testing.AllocsPerRun(1000, func() {
		hash, eq, err := s.key(a.Data())
		if err != nil {
			panic(err)
		}
		if _, found := s.find(a.Data(), hash, eq); !found {
			panic("identity lost")
		}
	}); n != 0 {
		t.Fatalf("pointer lookup allocated: %v", n)
	}
}

func TestResidentOpaqueIndexUnsupportedArrayDataRejectedBeforeRetain(t *testing.T) {
	ctx := context.Background()
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	a := opaqueIndexTestArray(t, alloc)
	counts := &opaqueRefCounts{}
	data := &countedOpaqueData{ArrayData: a.Data(), counts: counts}
	batch := memory.NewBatch()
	batch.Rows = 1
	v := &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: opaqueDataArray{Array: a, data: data}}
	batch.Opaque["raw"] = v
	owner, err := newResidentOpaqueRows(ctx, memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow}), batch, w)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	before := owner.reservation.Bytes()
	if err = owner.appendRow(ctx, batch, 0); err == nil {
		t.Fatal("unsupported array accepted")
	}
	if owner.reservation.Bytes() != before || owner.rows != 0 || counts.retains != 0 || counts.releases != 0 || len(owner.fields[0].values) != 0 || len(owner.fields[0].sources.slots) != 0 {
		t.Fatal("unsupported array touched owner", counts)
	}
}

type opaqueIndexLifecycleSource struct {
	*opaqueScalingSource
	closes     int
	opens      int
	errorOpen  int
	onClose    func(int)
	nextErr    error
	closeErr   error
	errorAfter int
}

func (s *opaqueIndexLifecycleSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	stream, err := s.opaqueScalingSource.NewBatchStream(ctx, opts)
	if err != nil {
		return nil, err
	}
	s.opens++
	return &opaqueIndexLifecycleStream{BatchStream: stream, owner: s, open: s.opens}, nil
}

type opaqueIndexLifecycleStream struct {
	source.BatchStream
	owner *opaqueIndexLifecycleSource
	rows  int
	open  int
}

func (s *opaqueIndexLifecycleStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.owner.nextErr != nil && s.rows == s.owner.errorAfter && (s.owner.errorOpen == 0 || s.owner.errorOpen == s.open) {
		return false, s.owner.nextErr
	}
	ok, err := s.BatchStream.Next(ctx, dst)
	if ok {
		s.rows += dst.Rows
	}
	return ok, err
}
func (s *opaqueIndexLifecycleStream) Close() error {
	err := s.BatchStream.Close()
	s.owner.closes++
	if s.owner.onClose != nil {
		s.owner.onClose(s.owner.closes)
	}
	if s.owner.errorOpen == 0 || s.owner.errorOpen == s.open {
		return errors.Join(err, s.owner.closeErr)
	}
	return err
}

func TestResidentOpaqueIndexManyBatchLifecycle(t *testing.T) {
	for _, topK := range []bool{false, true} {
		for _, ending := range []string{"eof", "close", "cancel", "nil-destination", "nil-context", "source-error", "close-error", "replay-error", "replay-close-error"} {
			if !topK && (ending == "replay-error" || ending == "replay-close-error") {
				continue
			}
			t.Run(fmt.Sprintf("topk=%v/%s", topK, ending), func(t *testing.T) {
				const n = 2048
				alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				arrays := make([]arrow.Array, n)
				release := func() {
					for i, a := range arrays {
						if a != nil {
							a.Release()
							arrays[i] = nil
						}
					}
				}
				defer release()
				for i := range arrays {
					builder := array.NewLargeStringBuilder(alloc)
					builder.Append("hidden-prefix")
					if i%7 == 0 {
						builder.AppendNull()
					} else {
						builder.Append(fmt.Sprint(i))
					}
					root := builder.NewArray()
					builder.Release()
					arrays[i] = array.NewSlice(root, 1, 2)
					root.Release()
				}
				input := &opaqueIndexLifecycleSource{opaqueScalingSource: &opaqueScalingSource{arrays: arrays, rows: n, schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow})}}
				want, closes := n, 1
				if topK {
					want, closes = n/2, 2
				}
				input.onClose = func(got int) {
					if got == closes {
						release()
					}
				}
				sentinel := errors.New("injected source failure")
				if ending == "source-error" {
					input.nextErr = sentinel
					input.errorAfter = 1024
					closes = 1
				}
				if ending == "close-error" {
					input.closeErr = sentinel
					closes = 1
				}
				if ending == "replay-error" {
					input.nextErr = sentinel
					input.errorAfter = 1536
					input.errorOpen = 2
				}
				if ending == "replay-close-error" {
					input.closeErr = sentinel
					input.errorOpen = 2
				}
				manager := memory.NewBudgetManager(0)
				var ordered source.Source
				var err error
				if topK {
					ordered, err = NewTopKSourceWithManager(input, want, []SortKey{{Column: "key"}}, manager)
				} else {
					ordered, err = NewSortSource(input, []SortKey{{Column: "key"}}, manager)
				}
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				stream, err := source.OpenBatchStream(ctx, ordered, testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				ok, err := stream.Next(ctx, out)
				if ending == "source-error" || ending == "close-error" || ending == "replay-error" || ending == "replay-close-error" {
					if ok || !errors.Is(err, sentinel) || manager.Used() != 0 || input.closes != closes {
						t.Fatal("failure cleanup", ok, err, manager.Used(), input.closes)
					}
					return
				}
				if !ok || err != nil || input.closes != closes {
					t.Fatal("prepare", ok, err, input.closes)
				}
				verify := func(expected int) {
					t.Helper()
					if out.Rows != 1 || out.Int64s["key"].Values[0] != int64(expected) || out.Opaque["raw"].Owned {
						t.Fatal("ordered output shape")
					}
					a := out.Opaque["raw"].Value.(*array.LargeString)
					origin := n - expected
					if a.IsNull(0) != (origin%7 == 0) || origin%7 != 0 && a.Value(0) != fmt.Sprint(origin) {
						t.Fatal("ordered payload mismatch", expected)
					}
				}
				verify(1)
				borrowed := out.Opaque["raw"]
				switch ending {
				case "eof":
					for row := 2; row <= want; row++ {
						ok, err = stream.Next(ctx, out)
						if !ok || err != nil {
							t.Fatal(ok, err)
						}
						verify(row)
					}
					ok, err = stream.Next(ctx, out)
					if ok || err != nil {
						t.Fatal("EOF", ok, err)
					}
				case "close":
					if err = stream.Close(); err != nil {
						t.Fatal(err)
					}
				case "cancel":
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					ok, err = stream.Next(canceled, out)
					if ok || !errors.Is(err, context.Canceled) {
						t.Fatal(ok, err)
					}
				case "nil-destination":
					ok, err = stream.Next(ctx, nil)
					if ok || err == nil {
						t.Fatal(ok, err)
					}
				case "nil-context":
					ok, err = stream.Next(nil, out)
					if ok || err == nil {
						t.Fatal(ok, err)
					}
				}
				if borrowed.Value != nil || manager.Used() != 0 || input.closes != closes {
					t.Fatal("terminal lifecycle", borrowed.Value, manager.Used(), input.closes)
				}
				if err = stream.Close(); err != nil {
					t.Fatal(err)
				}
				if input.closes != closes {
					t.Fatal("double close")
				}
			})
		}
	}
}

func TestResidentOpaqueIndexMultiFieldPartialFailure(t *testing.T) {
	ctx := context.Background()
	w, parent := externalAccumulatorTestWorkspace(t, 0)
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	a := opaqueIndexTestArray(t, alloc)
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Opaque["one"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: a}
	batch.Opaque["two"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: a}
	owner, err := newResidentOpaqueRows(ctx, memory.NewSchema(memory.Field{Name: "one", Type: memory.TypeArrow}, memory.Field{Name: "two", Type: memory.TypeArrow}), batch, w)
	if err != nil {
		t.Fatal(err)
	}
	batch.Opaque["two"].Value = struct{}{}
	if err = owner.appendRow(ctx, batch, 0); err == nil {
		t.Fatal("invalid second field accepted")
	}
	if owner.rows != 0 || owner.fields[0].sources.count != 1 || owner.fields[1].sources.count != 0 {
		t.Fatal("unexpected partial append")
	}
	owner.close()
	owner.close()
	w.close()
	if parent.Used() != 0 {
		t.Fatal("partial error leaked budget")
	}
}

func TestResidentOpaqueIndexTypedNilOriginUnchanged(t *testing.T) {
	ctx := context.Background()
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	a := opaqueIndexTestArray(t, arrowmem.DefaultAllocator)
	batch := memory.NewBatch()
	batch.Rows = 1
	v := &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: a}
	batch.Opaque["raw"] = v
	owner, err := newResidentOpaqueRows(ctx, memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow}), batch, w)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	before := owner.reservation.Bytes()
	var origin *countedOpaqueData
	if err = owner.appendValue(ctx, &owner.fields[0], v, 0, origin); err == nil {
		t.Fatal("typed nil origin accepted")
	}
	if owner.reservation.Bytes() != before || len(owner.fields[0].values) != 0 || owner.fields[0].sources.count != 0 || len(owner.fields[0].sources.slots) != 0 {
		t.Fatal("typed nil origin mutated owner")
	}
}
