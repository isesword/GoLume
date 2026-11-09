package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func TestExternalSortInitialGatherRuntimeMetadataAdmission(t *testing.T) {
	src := memory.NewBatch()
	src.Rows = 1
	src.Int64s["key"] = &memory.Int64Vector{Values: []int64{7}}
	alias := &memory.Int8Vector{Values: []int8{3}}
	const aliases = 4096
	for i := 0; i < aliases; i++ {
		src.Int8s[fmt.Sprintf("extra_%d", i)] = alias
	}
	src.Uint8s["extra_0"] = &memory.Uint8Vector{Values: []uint8{9}}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	keys := []SortKey{{Column: "key"}}
	plan, err := planExternalSortInitialGather(schema, src, 0, 1, len(keys), 0)
	if err != nil {
		t.Fatal(err)
	}
	round := func(n uintptr) int64 { return (int64(n) + 15) &^ 15 }
	metadata := externalSortInitialGatherBatchBytes + 4*128 +
		int64(aliases)*(512+round(unsafe.Sizeof(memory.Int8Vector{}))) +
		512 + round(unsafe.Sizeof(memory.Uint8Vector{})) + 512 + round(unsafe.Sizeof(memory.Int64Vector{})) +
		512 + round(unsafe.Sizeof(memory.Uint64Vector{})) + round(unsafe.Sizeof(externalSortInitialGather{})) +
		round(unsafe.Sizeof(sortWorkspaceReservation{})) + sortBindingKeyBytes
	if plan.metadataBytes != metadata {
		t.Fatalf("metadata=%d want=%d", plan.metadataBytes, metadata)
	}
	if plan.gatherBytes != plan.capacityBytes+metadata {
		t.Fatalf("runtime metadata was not additive: %+v", plan)
	}
	legacy, err := externalSortInitialGatherLegacyBytes(plan.rangeBytes, 1, sortBindingKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if legacy >= plan.gatherBytes {
		t.Fatal("fixture does not expose former undercount")
	}
	for _, limit := range []int64{legacy, plan.gatherBytes - 1, plan.gatherBytes} {
		manager := memory.NewBudgetManager(limit)
		owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, keys, "ordinal", 0, plan)
		if limit < plan.gatherBytes {
			if owner != nil || !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 {
				t.Fatalf("limit=%d owner=%v error=%v used=%d", limit, owner, err, manager.Used())
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if owner.batch.Int8s["extra_0"] == owner.batch.Int8s["extra_1"] || owner.batch.Int8s["extra_0"] == alias {
			t.Fatal("runtime aliases coalesced")
		}
		if len(owner.batch.Int8s) != aliases || owner.batch.Uint8s["extra_0"].Values[0] != 9 {
			t.Fatal("runtime binding lost")
		}
		if owner.batch.Strings != nil || owner.batch.Structs != nil || owner.batch.Opaque != nil {
			t.Fatal("unused maps allocated")
		}
		owner.close()
		if manager.Used() != 0 {
			t.Fatal("reservation leaked")
		}
	}
	if allocs := testing.AllocsPerRun(10, func() {
		if _, err := planExternalSortInitialGather(schema, src, 0, 1, len(keys), 0); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("successful plan allocated %v times", allocs)
	}
}

func initialGatherOwnershipFixture() *memory.Batch {
	src := joinGatherTestFixture()
	src.Int8s["alias"] = src.Int8s["Int8s"]
	src.Uint64s["ordinal"] = &memory.Uint64Vector{Values: []uint64{91, 92, 93}, Validity: memory.Bitmap{7}}
	return src
}

func TestExternalSortInitialGatherExactCapacitiesAndClose(t *testing.T) {
	src := initialGatherOwnershipFixture()
	original := initialGatherOwnershipFixture()
	schema := memory.NewSchema(memory.Field{Name: "Int64s", Type: memory.TypeInt64})
	keys := []SortKey{{Column: "Int64s", Descending: true, NullsLast: true}}
	plan, err := planExternalSortInitialGather(schema, src, 0, 3, len(keys), 10)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(plan.gatherBytes)
	owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, keys, "ordinal", 10, plan)
	if err != nil {
		t.Fatal(err)
	}
	b := owner.batch
	indices, bindings := owner.indices, owner.comparator.keys
	displaced := owner.displacedOrdinal
	if !reflect.DeepEqual(indices, []int{2, 0, 1}) || !reflect.DeepEqual(b.Uint64s["ordinal"].Values, []uint64{12, 10, 11}) {
		t.Fatalf("indices=%v ordinal=%v", indices, b.Uint64s["ordinal"].Values)
	}
	if displaced == nil || displaced == src.Uint64s["ordinal"] || !reflect.DeepEqual(displaced.Values, []uint64{93, 91, 92}) {
		t.Fatal("runtime ordinal collision ownership lost")
	}
	actual := joinGatherTestOwnedCapacity(b, false) + int64(cap(indices))*int64(unsafe.Sizeof(int(0))) + int64(cap(displaced.Values)+cap(displaced.Validity))*8
	if actual != plan.capacityBytes {
		t.Fatalf("actual capacities=%d planned=%d", actual, plan.capacityBytes)
	}
	if b.Decimals["decimal"].Precision != 18 || b.Decimals["decimal"].Scale != 4 || b.Decimals["decimal"].Values[0] != src.Decimals["decimal"].Values[2] {
		t.Fatal("extra native decimal changed")
	}
	if b.Int8s["alias"] == b.Int8s["Int8s"] || b.Int8s["alias"] == src.Int8s["alias"] {
		t.Fatal("alias not independently gathered")
	}
	record := b.Structs["record"]
	fields, childBatch := record.Fields, record.Values
	childText, childDictionary := childBatch.Strings["text"], childBatch.DictStrings["dict"]
	list, listValues := b.StringLists["list"], b.StringLists["list"].Values
	dictionary := b.DictStrings["dict"]
	rootMap, childMap := b.Int8s, childBatch.Strings
	ordinal := b.Uint64s["ordinal"]
	if cap(fields) != len(fields) || &fields[0] == &src.Structs["record"].Fields[0] || childBatch.Int64s != nil {
		t.Fatal("struct fields or sparse child maps incorrect")
	}
	if &dictionary.Data[0] != &src.DictStrings["dict"].Data[0] || &childDictionary.Data[0] == &src.DictStrings["dict"].Data[0] {
		t.Fatal("dictionary table borrow/copy contract changed")
	}
	if &listValues.Data[0] == &src.StringLists["list"].Values.Data[0] {
		t.Fatal("list child aliases source")
	}
	owner.reservation = &initialGatherCheckRelease{Reservation: owner.reservation, check: func() {
		for name, v := range map[string]any{"batch": *b, "struct": *record, "child batch": *childBatch, "text": *childText, "dictionary": *dictionary, "child dictionary": *childDictionary, "list": *list, "list child": *listValues, "ordinal": *ordinal, "displaced": *displaced, "owner": *owner} {
			if !reflect.ValueOf(v).IsZero() {
				t.Errorf("%s survives reservation release: %+v", name, v)
			}
		}
		if len(rootMap) != 0 || len(childMap) != 0 {
			t.Error("map bindings survive release")
		}
		for _, f := range fields {
			if !reflect.ValueOf(f).IsZero() {
				t.Error("field reference survives release")
			}
		}
		for _, i := range indices {
			if i != 0 {
				t.Error("index storage not cleared")
			}
		}
		for _, key := range bindings {
			if key.compare != nil || key.validity != nil {
				t.Error("comparator borrows survive release")
			}
		}
	}}
	owner.close()
	owner.close()
	if manager.Used() != 0 {
		t.Fatal("reservation leaked")
	}
	if !reflect.DeepEqual(src, original) {
		t.Fatal("close changed borrowed source storage")
	}
}

type initialGatherCheckRelease struct {
	memory.Reservation
	check func()
}

func (r *initialGatherCheckRelease) Release() { r.check(); r.Reservation.Release() }

type initialGatherErrContext struct {
	context.Context
	check func() error
}

func (c initialGatherErrContext) Err() error { return c.check() }

func TestExternalSortInitialGatherPartialGraphClose(t *testing.T) {
	src := joinGatherTestFixture()
	schema := memory.NewSchema(memory.Field{Name: "Int64s", Type: memory.TypeInt64})
	plan, err := planExternalSortInitialGather(schema, src, 0, 3, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(plan.gatherBytes)
	reservation, err := manager.Reserve(context.Background(), plan.gatherBytes)
	if err != nil {
		t.Fatal(err)
	}
	owner := &externalSortInitialGather{reservation: reservation, indices: []int{2, 1, 0}, batch: newExternalSortInitialGatherBatch(externalSortInitialGatherCounts(src))}
	var partial *memory.StructVector
	var child *memory.Int8Vector
	var fields []memory.Field
	ctx := initialGatherErrContext{Context: context.Background(), check: func() error {
		if v := owner.batch.Structs["record"]; v != nil && v.Values != nil && v.Values.Int8s["number"] != nil {
			partial = v
			child = v.Values.Int8s["number"]
			fields = v.Fields
			return context.Canceled
		}
		return nil
	}}
	if err := takeExternalSortInitialRows(ctx, owner.batch, src, owner.indices); !errors.Is(err, context.Canceled) {
		t.Fatalf("partial fill error=%v", err)
	}
	if partial == nil || child == nil || len(fields) == 0 || len(owner.batch.StringLists) == 0 {
		t.Fatal("failure did not exercise partial nested graph")
	}
	owner.reservation = &initialGatherCheckRelease{Reservation: reservation, check: func() {
		if !reflect.ValueOf(*partial).IsZero() || !reflect.ValueOf(*child).IsZero() {
			t.Error("partial descriptors retained at release")
		}
		for _, f := range fields {
			if !reflect.ValueOf(f).IsZero() {
				t.Error("partial Fields retained at release")
			}
		}
	}}
	owner.close()
	if manager.Used() != 0 {
		t.Fatal("partial reservation leaked")
	}
	if !reflect.DeepEqual(src, joinGatherTestFixture()) {
		t.Fatal("partial close changed source")
	}
}

func TestExternalSortInitialGatherComparatorErrorCloses(t *testing.T) {
	src := joinGatherTestFixture()
	schema := memory.NewSchema(memory.Field{Name: "Int64s", Type: memory.TypeInt64})
	keys := []SortKey{{Column: "Int64s"}, {Column: "missing"}}
	plan, err := planExternalSortInitialGather(schema, src, 0, 3, len(keys), 0)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(plan.gatherBytes)
	owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, keys, "ordinal", 0, plan)
	if owner != nil || err == nil || manager.Used() != 0 {
		t.Fatalf("owner=%v err=%v used=%d", owner, err, manager.Used())
	}
}

func initialGatherAllValidFixture(rows int) (memory.Schema, *memory.Batch) {
	src := memory.NewBatch()
	src.Rows = rows
	src.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, rows)}
	str := &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, rows)}
	list := &memory.StringListVector{Offsets: make([]uint32, rows+1), Values: &memory.StringViewVector{Offsets: make([]uint32, rows*2+1), Data: make([]byte, rows*2)}}
	for i := 0; i <= rows; i++ {
		str.Offsets[i] = uint32(i)
		list.Offsets[i] = uint32(i * 2)
		if i < rows {
			src.Int64s["key"].Values[i] = int64(rows - i)
		}
	}
	for i := range list.Values.Offsets {
		list.Values.Offsets[i] = uint32(i)
	}
	src.StringLists["list"] = list
	children := memory.NewBatch()
	children.Rows = rows
	children.Int8s["number"] = &memory.Int8Vector{Values: make([]int8, rows)}
	children.Strings["text"] = str
	children.StringLists["list"] = list
	fields := []memory.Field{{Name: "number", Type: memory.TypeInt8}, {Name: "text", Type: memory.TypeString}, memory.ListStringField("list", false, false)}
	src.Structs["record"] = &memory.StructVector{Rows: rows, Fields: fields, Values: children}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.ListStringField("list", false, false), memory.Field{Name: "record", Type: memory.TypeStruct, Children: fields})
	return schema, src
}

func TestExternalSortInitialGatherSynthesizedBitmapEncodeBound(t *testing.T) {
	for _, n := range []int{1, 7, 64, 65, 129} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			schema, src := initialGatherAllValidFixture(n)
			keys := []SortKey{{Column: "key"}}
			plan, err := planExternalSortInitialGather(schema, src, 0, n, len(keys), 0)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := newExternalSortInitialGather(context.Background(), memory.NewBudgetManager(plan.gatherBytes), schema, src, keys, "ordinal", 0, plan)
			if err != nil {
				t.Fatal(err)
			}
			defer owner.close()
			actual := joinGatherTestOwnedCapacity(owner.batch, false) + int64(cap(owner.indices))*int64(unsafe.Sizeof(int(0)))
			if actual != plan.capacityBytes {
				t.Fatalf("actual=%d plan=%d", actual, plan.capacityBytes)
			}
			fields := make([]memory.Field, len(schema.Fields)+1)
			copy(fields, schema.Fields)
			fields[len(schema.Fields)] = memory.Field{Name: "ordinal", Type: memory.TypeUint64}
			encoded, err := memory.EncodeColumnarRun(context.Background(), memory.NewSchema(fields...), owner.batch, memory.NewBudgetManager(plan.encodeBytes))
			if err != nil {
				t.Fatal(err)
			}
			defer encoded.Close()
			if int64(len(encoded.Payload)) > plan.encodeBytes {
				t.Fatalf("encoded=%d plan=%d", len(encoded.Payload), plan.encodeBytes)
			}
			baseline, err := memory.EstimateColumnarRunRangeBytes(schema, src, 0, n)
			if err != nil {
				t.Fatal(err)
			}
			expectedBitmaps := int64(5*joinGatherWords(n)+2*joinGatherWords(n*2)) * 8
			if plan.encodeBytes != baseline+int64(n)*8+4+expectedBitmaps {
				t.Fatalf("encode=%d baseline=%d bitmaps=%d", plan.encodeBytes, baseline, expectedBitmaps)
			}
			if src.StringLists["list"].Validity != nil || src.StringLists["list"].Values.Validity != nil || src.Structs["record"].Validity != nil {
				t.Fatal("source bitmaps synthesized in place")
			}
		})
	}
}

func TestExternalSortInitialGatherSelectedRangeNotFullSourceFloor(t *testing.T) {
	src := memory.NewBatch()
	src.Rows = 1 << 18
	src.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, src.Rows)}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	plan, err := planExternalSortInitialGather(schema, src, 17, 18, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan.gatherBytes >= int64(src.Rows)*8 {
		t.Fatalf("single row inherits whole-source floor: %d", plan.gatherBytes)
	}
	for _, start := range []int{0, 17, src.Rows - 1} {
		got, err := planExternalSortInitialGatherValidated(schema, src, start, start+1, 1, 0)
		if err != nil || got.gatherBytes != plan.gatherBytes {
			t.Fatalf("start=%d plan=%+v err=%v", start, got, err)
		}
	}
}

func TestExternalSortInitialGatherRangeArithmeticAndCollisions(t *testing.T) {
	src := memory.NewBatch()
	src.Rows = 3
	src.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 2, 1}}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	for _, tc := range []struct {
		start, end, keys int
		base             uint64
	}{{-1, 1, 0, 0}, {1, 0, 0, 0}, {0, 4, 0, 0}, {0, 1, -1, 0}, {1, 3, 0, math.MaxUint64 - 1}, {0, 3, 0, math.MaxUint64}, {0, 1, math.MaxInt, 0}} {
		if _, err := planExternalSortInitialGather(schema, src, tc.start, tc.end, tc.keys, tc.base); err == nil {
			t.Fatalf("accepted %+v", tc)
		}
	}
	if _, err := planExternalSortInitialGather(schema, src, 0, 1, 0, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		payload  int64
		rows     int
		bindings int64
	}{{-1, 0, 0}, {0, -1, 0}, {0, 0, -1}, {math.MaxInt64/2 + 1, 0, 0}, {0, math.MaxInt, 0}, {0, 0, math.MaxInt64}} {
		if _, err := externalSortInitialGatherLegacyBytes(tc.payload, tc.rows, tc.bindings); err == nil {
			t.Fatalf("accepted overflow %+v", tc)
		}
	}
	if _, err := externalSortInitialGatherRounded(math.MaxInt64); err == nil {
		t.Fatal("rounding overflow accepted")
	}
	plan, err := planExternalSortInitialGather(schema, src, 0, 3, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(plan.gatherBytes)
	if owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, nil, "key", 0, plan); owner != nil || err == nil || !strings.Contains(err.Error(), "collides") || manager.Used() != 0 {
		t.Fatalf("declared collision owner=%v error=%v used=%d", owner, err, manager.Used())
	}
	if owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, nil, "ordinal", math.MaxUint64, plan); owner != nil || err == nil || manager.Used() != 0 {
		t.Fatal("ordinal overflow passed admission")
	}
	if owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, []SortKey{{Column: "key"}}, "ordinal", 0, plan); owner != nil || err == nil || manager.Used() != 0 {
		t.Fatal("plan with wrong key count passed admission")
	}
}

func initialGatherEmptyNullDictionaryFixture(nested bool) *memory.Batch {
	src := memory.NewBatch()
	src.Rows = 3
	src.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
	dictionary := &memory.DictionaryStringVector{
		Indices: []uint32{math.MaxUint32, 7, 99}, Validity: memory.Bitmap{0},
		Data: []byte("unused"), DictionaryDomain: "empty-null-domain", Encoding: memory.EncodingDictionary,
	}
	if nested {
		children := memory.NewBatch()
		children.Rows = src.Rows
		children.DictStrings["empty"] = dictionary
		children.DictStrings["ungathered"] = dictionary
		src.Structs["extra_record"] = &memory.StructVector{Rows: src.Rows, Values: children,
			Fields: []memory.Field{{Name: "empty", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true}}}
	} else {
		src.DictStrings["extra"] = dictionary
		src.DictStrings["alias"] = dictionary
	}
	return src
}

func TestExternalSortInitialGatherEmptyNullDictionaryExtras(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("nested=%v", nested), func(t *testing.T) {
			src := initialGatherEmptyNullDictionaryFixture(nested)
			original := initialGatherEmptyNullDictionaryFixture(nested)
			schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
			keys := []SortKey{{Column: "key"}}
			var dictionary *memory.DictionaryStringVector
			if nested {
				dictionary = src.Structs["extra_record"].Values.DictStrings["empty"]
			} else {
				dictionary = src.DictStrings["extra"]
			}
			if err := dictionary.Validate(src.Rows); err != nil {
				t.Fatal(err)
			}
			if _, err := joinGatherPayload(src, nil); err == nil || !strings.Contains(err.Error(), "missing offsets") {
				t.Fatalf("join's strict validation changed: %v", err)
			}
			if err := joinGatherDictionaryPayload(new(int64), dictionary, src.Rows); err == nil {
				t.Fatal("strict dictionary wrapper accepted empty offsets")
			}
			plan, err := planExternalSortInitialGather(schema, src, 0, src.Rows, len(keys), 0)
			if err != nil {
				t.Fatal(err)
			}
			if allocs := testing.AllocsPerRun(10, func() {
				if _, err := planExternalSortInitialGather(schema, src, 0, src.Rows, len(keys), 0); err != nil {
					panic(err)
				}
			}); allocs != 0 {
				t.Fatalf("empty-dictionary plan allocated %v times", allocs)
			}
			manager := memory.NewBudgetManager(plan.gatherBytes)
			owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, keys, "ordinal", 0, plan)
			if err != nil {
				t.Fatal(err)
			}
			actual := joinGatherTestOwnedCapacity(owner.batch, false) + int64(cap(owner.indices))*int64(unsafe.Sizeof(int(0)))
			if actual != plan.capacityBytes {
				t.Fatalf("actual=%d planned=%d", actual, plan.capacityBytes)
			}
			var out *memory.DictionaryStringVector
			if nested {
				children := owner.batch.Structs["extra_record"].Values
				out = children.DictStrings["empty"]
				if len(children.DictStrings) != 1 || children.Strings != nil {
					t.Fatal("nested runtime fallback or Fields selection changed")
				}
				if !reflect.DeepEqual(out.Indices, []uint32{0, 0, 0}) || &out.Data[0] == &dictionary.Data[0] {
					t.Fatal("nested all-NULL codes/table were not copied canonically")
				}
			} else {
				out = owner.batch.DictStrings["extra"]
				if !reflect.DeepEqual(out.Indices, []uint32{7, 99, math.MaxUint32}) || &out.Data[0] != &dictionary.Data[0] {
					t.Fatal("top-level hidden codes/table changed")
				}
				if out == owner.batch.DictStrings["alias"] || &out.Indices[0] == &owner.batch.DictStrings["alias"].Indices[0] {
					t.Fatal("dictionary aliases coalesced")
				}
			}
			if len(out.Offsets) != 0 || out.DictionaryDomain != dictionary.DictionaryDomain || !reflect.DeepEqual(out.Validity, memory.Bitmap{0}) {
				t.Fatal("empty dictionary metadata changed")
			}
			// Extras are still gathered and charged, but schema projection by the codec
			// ignores them, as it did before bounded initial-gather admission.
			internal := memory.NewSchema(schema.Fields[0], memory.Field{Name: "ordinal", Type: memory.TypeUint64})
			encoded, err := memory.EncodeColumnarRun(context.Background(), internal, owner.batch, memory.NewBudgetManager(plan.encodeBytes))
			if err != nil {
				owner.close()
				t.Fatal(err)
			}
			_ = encoded.Close()
			owner.close()
			if manager.Used() != 0 || !reflect.ValueOf(*out).IsZero() {
				t.Fatal("empty dictionary owner leaked")
			}
			if !reflect.DeepEqual(src, original) {
				t.Fatal("empty dictionary source changed")
			}
		})
	}
}

func TestExternalSortInitialGatherEmptyDictionaryValidRowsReject(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, validity := range []memory.Bitmap{nil, {}, {1}, {2}, {4}} {
			src := initialGatherEmptyNullDictionaryFixture(nested)
			var dictionary *memory.DictionaryStringVector
			if nested {
				dictionary = src.Structs["extra_record"].Values.DictStrings["empty"]
			} else {
				dictionary = src.DictStrings["extra"]
			}
			dictionary.Validity = validity
			// Code zero must fail too: an empty table has no valid dictionary values.
			for i := range dictionary.Indices {
				dictionary.Indices[i] = 0
			}
			if err := validateExternalSortInitialGather(src); err == nil {
				t.Fatalf("nested=%v validity=%v accepted a valid or malformed row", nested, validity)
			}
			if _, err := planExternalSortInitialGather(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}), src, 0, 1, 0, 0); err == nil {
				t.Fatalf("nested=%v validity=%v accepted invalid data outside the selected range", nested, validity)
			}
		}
	}
}

func TestExternalSortInitialGatherDeclaredEmptyDictionaryStillFailsCodec(t *testing.T) {
	src := initialGatherEmptyNullDictionaryFixture(false)
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "extra", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true})
	plan, err := planExternalSortInitialGather(schema, src, 0, src.Rows, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(plan.gatherBytes)
	owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, nil, "ordinal", 0, plan)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := memory.EncodeColumnarRun(context.Background(), schema, owner.batch, memory.NewBudgetManager(plan.encodeBytes))
	if encoded != nil {
		_ = encoded.Close()
	}
	owner.close()
	if err == nil || !strings.Contains(err.Error(), "offset vector is empty") {
		t.Fatalf("declared empty dictionary codec error=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatal("declared dictionary failure leaked owner")
	}
}

func TestExternalSortInitialGatherNilDictionaryTable(t *testing.T) {
	for _, nested := range []bool{false, true} {
		src := initialGatherEmptyNullDictionaryFixture(nested)
		var dictionary *memory.DictionaryStringVector
		if nested {
			dictionary = src.Structs["extra_record"].Values.DictStrings["empty"]
		} else {
			dictionary = src.DictStrings["extra"]
		}
		dictionary.Data = nil
		// Non-logical tail bits are irrelevant; every actual row remains NULL.
		dictionary.Validity = memory.Bitmap{^uint64(7)}
		schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
		plan, err := planExternalSortInitialGather(schema, src, 0, src.Rows, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		manager := memory.NewBudgetManager(plan.gatherBytes)
		owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, nil, "ordinal", 0, plan)
		if err != nil {
			t.Fatal(err)
		}
		out := owner.batch.DictStrings["extra"]
		if nested {
			out = owner.batch.Structs["extra_record"].Values.DictStrings["empty"]
		}
		if len(out.Data) != 0 || len(out.Offsets) != 0 || !reflect.DeepEqual(out.Validity, memory.Bitmap{0}) {
			t.Fatalf("nested=%v nil table changed: %+v", nested, out)
		}
		owner.close()
		if manager.Used() != 0 || dictionary.Data != nil || dictionary.Offsets != nil || dictionary.Validity[0] != ^uint64(7) || dictionary.Indices[0] != math.MaxUint32 {
			t.Fatalf("nested=%v nil table source changed or charge leaked", nested)
		}
	}
}

func TestExternalSortInitialGatherOffsetRepresentationLimits(t *testing.T) {
	maxOffsetCount := uint64(math.MaxUint32)
	maxAddressable := uint64(math.MaxInt)
	for _, tc := range []struct {
		elements, bytes uint64
		valid           bool
	}{
		{0, 0, true},
		{maxOffsetCount - 1, 0, maxOffsetCount <= maxAddressable},
		{maxOffsetCount, 0, false},
		{maxOffsetCount + 1, 0, false},
		{0, maxOffsetCount, maxOffsetCount <= maxAddressable},
		{0, maxOffsetCount + 1, false},
		{maxAddressable, 0, false},
		{0, maxAddressable, maxAddressable <= maxOffsetCount},
		{math.MaxUint64, 0, false},
		{0, math.MaxUint64, false},
	} {
		err := joinGatherListSizeLimits(tc.elements, tc.bytes)
		if (err == nil) != tc.valid {
			t.Fatalf("elements=%d bytes=%d error=%v valid=%v", tc.elements, tc.bytes, err, tc.valid)
		}
	}
	src := &memory.Batch{Rows: math.MaxInt}
	if err := externalSortInitialGatherRange(src, 0, math.MaxInt, 0, 0); err == nil {
		t.Fatal("addressable offset count overflow accepted")
	}
	if maxOffsetCount <= maxAddressable {
		if err := externalSortInitialGatherRange(src, 0, int(maxOffsetCount-1), 0, 0); err != nil {
			t.Fatalf("largest representable offset count rejected: %v", err)
		}
		if err := externalSortInitialGatherRange(src, 0, int(maxOffsetCount), 0, 0); err == nil {
			t.Fatal("uint32 offset count overflow accepted")
		}
	}
}

func TestExternalSortInitialGatherEmptyListChildWithoutOffsets(t *testing.T) {
	cases := []struct {
		name                         string
		nested                       bool
		listValidity, structValidity memory.Bitmap
	}{
		{name: "top_valid_empty"},
		{name: "top_null_and_empty", listValidity: memory.Bitmap{5}},
		{name: "struct_valid_empty", nested: true},
		{name: "struct_null_list", nested: true, listValidity: memory.Bitmap{5}},
		{name: "struct_null_parent", nested: true, structValidity: memory.Bitmap{5}},
		{name: "struct_mixed_nulls", nested: true, listValidity: memory.Bitmap{3}, structValidity: memory.Bitmap{6}},
	}
	for _, tc := range cases {
		for _, start := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/start=%d", tc.name, start), func(t *testing.T) {
				fixture := func() (memory.Schema, *memory.Batch, *memory.StringListVector) {
					src := memory.NewBatch()
					src.Rows = 3
					src.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
					list := &memory.StringListVector{Offsets: []uint32{0, 0, 0, 0}, Values: &memory.StringViewVector{}, Validity: append(memory.Bitmap(nil), tc.listValidity...)}
					field := memory.ListStringField("list", true, true)
					if tc.nested {
						children := memory.NewBatch()
						children.Rows = src.Rows
						children.StringLists["list"] = list
						fields := []memory.Field{field}
						src.Structs["record"] = &memory.StructVector{Rows: src.Rows, Fields: fields, Values: children, Validity: append(memory.Bitmap(nil), tc.structValidity...)}
						field = memory.Field{Name: "record", Type: memory.TypeStruct, Nullable: true, Children: fields}
					} else {
						src.StringLists["list"] = list
					}
					return memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, field), src, list
				}
				schema, src, list := fixture()
				_, original, _ := fixture()
				if err := list.Validate(src.Rows); err != nil {
					t.Fatal(err)
				}
				if err := validateExternalSortInitialGather(src); err != nil {
					t.Fatal(err)
				}
				keys := []SortKey{{Column: "key"}}
				plan, err := planExternalSortInitialGather(schema, src, start, src.Rows, len(keys), 0)
				if err != nil {
					t.Fatal(err)
				}
				rows := src.Rows - start
				// Even with no child offsets in the input, the output owns its canonical
				// zero sentinel. The source-range floor must retain that four-byte charge.
				minimumRange := int64(rows)*8 + int64(rows+1)*4 + 4
				if plan.rangeBytes < minimumRange {
					t.Fatalf("range=%d missing child sentinel floor=%d", plan.rangeBytes, minimumRange)
				}
				manager := memory.NewBudgetManager(plan.gatherBytes)
				owner, err := newExternalSortInitialGather(context.Background(), manager, schema, src, keys, "ordinal", 0, plan)
				if err != nil {
					t.Fatal(err)
				}
				defer owner.close()
				out := owner.batch.StringLists["list"]
				if tc.nested {
					out = owner.batch.Structs["record"].Values.StringLists["list"]
				}
				if out == list || out.Values == list.Values {
					t.Fatal("empty list child aliases source")
				}
				if cap(out.Offsets) != rows+1 || cap(out.Values.Offsets) != 1 || !reflect.DeepEqual(out.Values.Offsets, []uint32{0}) || cap(out.Values.Data) != 0 || cap(out.Values.Validity) != 0 {
					t.Fatalf("empty child not exactly allocated: %+v", out.Values)
				}
				if err := out.Validate(rows); err != nil {
					t.Fatal(err)
				}
				for i, sourceRow := range owner.indices {
					expected := list.IsValid(sourceRow)
					if tc.nested {
						expected = expected && src.Structs["record"].IsValid(sourceRow)
					}
					lo, hi, valid := out.ValueRange(i)
					if valid != expected || out.IsValid(i) != expected || lo != 0 || hi != 0 {
						t.Fatalf("output row=%d source=%d valid=%v expected=%v range=[%d,%d)", i, sourceRow, valid, expected, lo, hi)
					}
				}
				actual := joinGatherTestOwnedCapacity(owner.batch, false) + int64(cap(owner.indices))*int64(unsafe.Sizeof(int(0)))
				if actual != plan.capacityBytes {
					t.Fatalf("actual capacity=%d planned=%d", actual, plan.capacityBytes)
				}
				fields := make([]memory.Field, len(schema.Fields)+1)
				copy(fields, schema.Fields)
				fields[len(schema.Fields)] = memory.Field{Name: "ordinal", Type: memory.TypeUint64}
				encoded, err := memory.EncodeColumnarRun(context.Background(), memory.NewSchema(fields...), owner.batch, memory.NewBudgetManager(plan.encodeBytes))
				if err != nil {
					t.Fatal(err)
				}
				if int64(len(encoded.Payload)) > plan.encodeBytes {
					t.Fatalf("encoded=%d bound=%d", len(encoded.Payload), plan.encodeBytes)
				}
				_ = encoded.Close()
				child := out.Values
				owner.close()
				if manager.Used() != 0 || !reflect.ValueOf(*out).IsZero() || !reflect.ValueOf(*child).IsZero() {
					t.Fatal("empty list owner leaked")
				}
				if !reflect.DeepEqual(src, original) || list.Values.Offsets != nil || list.Values.Data != nil || list.Values.Validity != nil {
					t.Fatal("empty source child was canonicalized in place")
				}
			})
		}
	}
}
