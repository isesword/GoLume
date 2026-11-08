package main

import (
	"fmt"
	"strconv"
	"strings"
)

// generateAMD64SIMD emits fixed-width kernels: runtime width selection happens
// only during installation, never in the vector loop. At 128/256 bits Mask16
// is reinterpreted as bytes before extraction: its own ToBits requires AVX512.
func generateAMD64SIMD() []byte {
	b := header("goexperiment.simd && amd64")
	b.WriteString(`import (
 "gocolumn/memory"
 "math/bits"
 "simd"
 "simd/archsimd"
)

func amd64ComparisonWidthSupported(vectorBits int) bool {
 if !nativeSIMDAvailable() || vectorBits != (simd.Int64s{}).Len()*64 { return false }
 switch vectorBits {
 // Even 128-bit scalar broadcasts use AVX2 VPBROADCAST instructions.
 case 128: return archsimd.X86.AVX2()
 case 256: return archsimd.X86.AVX2()
 case 512: return archsimd.X86.AVX512()
 default: return false
 }
}

// packAMD64Mask16 compresses the duplicate byte sign bits from 16-bit lanes.
// It uses ordinary integer operations, not BMI2 or AVX512-only mask extraction.
func packAMD64Mask16(x uint32) uint32 {
 x &= 0x55555555
 x = (x | x >> 1) & 0x33333333
 x = (x | x >> 2) & 0x0f0f0f0f
 x = (x | x >> 4) & 0x00ff00ff
 return (x | x >> 8) & 0x0000ffff
}

func installGeneratedArchSIMD(set *Set, mode Mode, vectorBits int) {
 if mode == ModeScalar || !amd64ComparisonWidthSupported(vectorBits) { return }
 capability := KernelCapability{HardwareSIMD: true, VectorBits: vectorBits, Backend: "archsimd-amd64-mask"}
 switch vectorBits {
`)
	literalTypes := append([]numericType{{"Float32", "float32", "compareFloat32"}}, comparisonTypes...)
	columnTypes := []numericType{{"Int32", "int32", "compareInt32"}, {"Uint32", "uint32", "compareUint32"}, {"Float32", "float32", "compareFloat32"}, {"Float64", "float64", "compareFloat64"}}
	for _, width := range []int{128, 256, 512} {
		fmt.Fprintf(b, "case %d:\n", width)
		for _, typ := range literalTypes {
			for _, op := range comparisonOps {
				fmt.Fprintf(b, "set.d.%s[%s] = simdAMD%d%s%s\nid%s%s, _ := ComparisonID(memory.Type%s, %s)\nset.setCapability(id%s%s, capability)\n", typ.field, op.constant, width, typ.name, op.name, typ.name, op.name, typ.name, op.constant, typ.name, op.name)
			}
		}
		for _, typ := range columnTypes {
			for _, op := range comparisonOps {
				fmt.Fprintf(b, "set.d.%sColumns[%s] = simdAMD%d%sColumns%s\nset.d.caps.columns[id%s%s] = capability\n", typ.field, op.constant, width, typ.name, op.name, typ.name, op.name)
			}
		}
	}
	b.WriteString("}\n}\n\n")
	for _, width := range []int{128, 256, 512} {
		for _, columns := range []bool{false, true} {
			types := literalTypes
			if columns {
				types = columnTypes
			}
			for _, typ := range types {
				elemBits, _ := strconv.Atoi(strings.TrimLeft(typ.name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"))
				lanes := width / elemBits
				vector := fmt.Sprintf("%sx%d", typ.name, lanes)
				maskBits := "uint64(mask.ToBits())"
				if elemBits == 16 && width < 512 {
					maskBits = fmt.Sprintf("uint64(packAMD64Mask16(uint32(mask.ToInt16x%d().AsInt8x%d().Less(archsimd.Int8x%d{}).ToBits())))", lanes, width/8, width/8)
				}
				for _, op := range comparisonOps {
					if columns {
						fmt.Fprintf(b, `func simdAMD%d%sColumns%s(left, right []%s, leftValid, rightValid, out memory.Bitmap) int {
 if len(left) != len(right) { panic("kernel: %s column comparison length mismatch") }
 if len(left) < %d { return scalar%sColumns%s(left, right, leftValid, rightValid, out) }
 words := prepareComparison(len(left), out)
 selected := 0
 for wi := 0; wi < words; wi++ {
  base := wi * 64
  rows := min(64, len(left)-base)
  validWord := bitmapWord(leftValid, wi) & bitmapWord(rightValid, wi) & memory.TailMask(rows)
  // prepareComparison cleared every logical output word, including skipped words.
  if validWord == 0 { continue }
  var word uint64
  off := 0
  for ; off+%d <= rows; off += %d {
   mask := archsimd.Load%s(left[base+off:]).%s(archsimd.Load%s(right[base+off:]))
   word |= %s << uint(off)
  }
  for ; off < rows; off++ {
   if left[base+off] %s right[base+off] { word |= uint64(1) << uint(off) }
  }
  word &= validWord
  out[wi] = word
  selected += bits.OnesCount64(word)
 }
 return selected
}

`, width, typ.name, op.name, typ.goType, typ.goType, lanes, typ.name, op.name, lanes, lanes, vector, op.nameForArch(), vector, maskBits, op.symbol)
					} else {
						fmt.Fprintf(b, `func simdAMD%d%s%s(values []%s, validity, out memory.Bitmap, threshold %s) int {
 if len(values) < %d { return scalar%s%s(values, validity, out, threshold) }
 words := prepareComparison(len(values), out)
 vectorThreshold := archsimd.Broadcast%s(threshold)
 selected := 0
 for wi := 0; wi < words; wi++ {
  base := wi * 64
  rows := min(64, len(values)-base)
  validWord := bitmapWord(validity, wi) & memory.TailMask(rows)
  // prepareComparison cleared every logical output word, including skipped words.
  if validWord == 0 { continue }
  var word uint64
  off := 0
  for ; off+%d <= rows; off += %d {
   mask := archsimd.Load%s(values[base+off:]).%s(vectorThreshold)
   word |= %s << uint(off)
  }
  for ; off < rows; off++ {
   if values[base+off] %s threshold { word |= uint64(1) << uint(off) }
  }
  word &= validWord
  out[wi] = word
  selected += bits.OnesCount64(word)
 }
 return selected
}

`, width, typ.name, op.name, typ.goType, typ.goType, lanes, typ.name, op.name, vector, lanes, lanes, vector, op.nameForArch(), maskBits, op.symbol)
					}
				}
			}
		}
	}
	return []byte(b.String())
}
