package pipeline

import (
	"gocolumn/memory"
	"testing"
)

// Compare public logical sizing with owned sizing on the same 64K-row input.
// This measures admission work, not cloning or end-to-end query throughput.
func BenchmarkOwnedAdmissionSplit(b *testing.B) {
	const rows = 64 << 10
	for _, kind := range []string{"scalar", "string", "struct_list"} {
		input := memory.NewBatch()
		input.Rows = rows
		switch kind {
		case "scalar":
			input.Int64s["x"] = &memory.Int64Vector{Values: make([]int64, rows)}
		case "string":
			offsets := make([]uint32, rows+1)
			for i := range offsets {
				offsets[i] = uint32(i * 8)
			}
			input.Strings["x"] = &memory.StringViewVector{Offsets: offsets, Data: make([]byte, rows*8)}
		case "struct_list":
			input = ownedAdmissionFixture(rows)
		}
		for _, owned := range []bool{false, true} {
			mode := "logical"
			if owned {
				mode = "owned"
			}
			b.Run(kind+"/"+mode, func(b *testing.B) {
				opts := Options{Workers: 1, TargetMorselBytes: 64 << 10, MaxMorselBytes: 256 << 10, MaxInFlightBytes: 1 << 20, MinMorselRows: 256, MaxMorselRows: rows}
				s, err := NewByteSizer(memory.Schema{}, opts)
				if owned {
					s, err = newOwnedByteSizer(memory.Schema{}, opts)
				}
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := s.Split(input); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
