package kernel

import (
	"gocolumn/memory"
	"testing"
)

// referenceInt8Compare deliberately does not call kernel code. It is the
// semantic oracle for the native-width path, including SQL NULL filtering.
func referenceInt8Compare(values []int8, validity memory.Bitmap, literal int8, op CompareOp) (memory.Bitmap, int) {
	out := make(memory.Bitmap, memory.WordsFor(len(values)))
	selected := 0
	for row, value := range values {
		if !validAt(validity, row) {
			continue
		}
		match := false
		switch op {
		case CompareGT:
			match = value > literal
		case CompareGE:
			match = value >= literal
		case CompareLT:
			match = value < literal
		case CompareLE:
			match = value <= literal
		case CompareEQ:
			match = value == literal
		}
		if match {
			memory.Set(out, row)
			selected++
		}
	}
	return out, selected
}

func TestCompareInt8MatchesIndependentReferenceAtTailsAndNulls(t *testing.T) {
	for _, rows := range []int{0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127} {
		values := make([]int8, rows)
		for row := range values {
			values[row] = int8(row*37 - 120)
		}
		validity := make(memory.Bitmap, memory.WordsFor(rows))
		for row := range values {
			if row%5 != 1 {
				memory.Set(validity, row)
			}
		}
		for _, op := range []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ} {
			want, wantCount := referenceInt8Compare(values, validity, -3, op)
			for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
				out := make(memory.Bitmap, memory.WordsFor(rows))
				gotCount := New(mode).CompareInt8(values, validity, out, -3, op)
				if gotCount != wantCount || !equalBitmap(out, want) {
					t.Fatalf("rows=%d mode=%s op=%s got=(%d,%x) want=(%d,%x)", rows, mode, op, gotCount, out, wantCount, want)
				}
			}
		}
	}
}

func TestCompareInt8NilValidityAndTailBits(t *testing.T) {
	values := []int8{-2, -1, 0, 1, 2, 3, 4}
	out := memory.Bitmap{^uint64(0)}
	if got := New(ModeScalar).CompareInt8(values, nil, out, 1, CompareGE); got != 4 {
		t.Fatalf("selected=%d, want 4", got)
	}
	// prepareComparison must clear stale bits beyond the seven logical rows.
	if out[0] != 0b1111000 {
		t.Fatalf("tail bits or result leaked: %064b", out[0])
	}
}

func TestCompareInt8CapabilityIsScalarAndPhysical(t *testing.T) {
	id, ok := ComparisonID(memory.TypeInt8, CompareGT)
	if !ok || id.typ != memory.TypeInt8 || id.String() != "compare-int8-gt" {
		t.Fatalf("id=%s ok=%t", id, ok)
	}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		capability, ok := New(mode).Capability(id)
		if !ok || capability.Backend != "scalar" || capability.HardwareSIMD || capability.VectorBits != 0 {
			t.Fatalf("mode=%s capability=%+v ok=%t", mode, capability, ok)
		}
	}
}
