package kernel

import (
	"gocolumn/memory"
	"testing"
)

func FuzzFloat32ColumnComparison(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6}, []byte{6, 5, 4, 3, 2, 1, 0}, uint8(0x7f))
	f.Fuzz(func(t *testing.T, leftBytes, rightBytes []byte, validByte uint8) {
		rows := len(leftBytes)
		if len(rightBytes) < rows {
			rows = len(rightBytes)
		}
		left := make([]float32, rows)
		right := make([]float32, rows)
		for i := 0; i < rows; i++ {
			left[i] = float32(int8(leftBytes[i]))
			right[i] = float32(int8(rightBytes[i]))
		}
		valid := memory.Bitmap{}
		if rows > 0 {
			valid = memory.Bitmap{uint64(validByte)}
		}
		for _, op := range []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ} {
			want := make(memory.Bitmap, memory.WordsFor(rows))
			got := make(memory.Bitmap, memory.WordsFor(rows))
			New(ModeScalar).CompareFloat32Columns(left, right, valid, nil, want, op)
			New(ModeSIMD).CompareFloat32Columns(left, right, valid, nil, got, op)
			if len(got) != len(want) || (len(got) > 0 && got[0] != want[0]) {
				t.Fatalf("op=%s got=%v want=%v", op, got, want)
			}
		}
	})
}
