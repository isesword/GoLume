package main

import (
	"bytes"
	"fmt"
	"strings"
)

// generateCheckedInteger32 uses full-width operations and widened multiplication
// to check every product without division, scratch buffers, or scalar lane loads.
func generateCheckedInteger32(b *bytes.Buffer, arch string, widths []int) {
	for _, width := range widths {
		lanes := width / 32
		for _, typ := range []string{"Int32", "Uint32"} {
			for _, op := range []string{"Add", "Sub", "Mul"} {
				fmt.Fprintf(b, `func checked%s%s%s%d(left,right,out []%s, validity,overflow memory.Bitmap) int {
prepareIntegerArithmetic(len(left),len(left),len(right),len(out),overflow)
`, arch, typ, op, width, strings.ToLower(typ))
				// Short inputs keep scalar sequential alias semantics without an overlap
				// check. The all-invalid case still avoids reading either input.
				b.WriteString("if len(left)<16 {\nif len(left)==0 {return -1}\nif validity==nil && overflow==nil {\nfirst:=-1\nfor row,a:=range left {\nb:=right[row]\n")
				symbol := map[string]string{"Add": "+", "Sub": "-", "Mul": "*"}[op]
				emitCheckedInteger32Scalar(b, typ, op, symbol)
				b.WriteString("if failed && first<0 {first=row}\nout[row]=value\n}\nreturn first\n}\n")
				b.WriteString("validWord:=bitmapWord(validity,0)&memory.TailMask(len(left))\nif validWord==0 {clear(out[:len(left)]);return -1}\nvar failedWord uint64\nfor row,a:=range left {\nb:=right[row]\n")
				emitCheckedInteger32Scalar(b, typ, op, symbol)
				b.WriteString("if failed {failedWord |= uint64(1)<<uint(row)}\nout[row]=value\n}\nfailedWord &= validWord\nif overflow!=nil {overflow[0]=failedWord}\ninvalid:= ^validWord & memory.TailMask(len(left))\nfor invalid!=0 {row:=bits.TrailingZeros64(invalid);out[row]=0;invalid &= invalid-1}\nif failedWord!=0 {return bits.TrailingZeros64(failedWord)}\nreturn -1\n}\n")
				fmt.Fprintf(b, `if checkedIntegerPartialOverlap(left,right,out) { return scalar%s%s(left,right,out,validity,overflow) }
first:=-1
for wi:=0; wi<memory.WordsFor(len(left)); wi++ {
base:=wi*64
rows:=min(64,len(left)-base)
validWord:=bitmapWord(validity,wi)&memory.TailMask(rows)
if validWord==0 { clear(out[base:base+rows]); continue }
var failedWord uint64
off:=0
`, typ, op)
				// Unroll to sixteen rows at narrow widths, keeping bounds checks outside.
				if arch == "amd64" && width < 512 {
					b.WriteString("for ; off+16<=rows; off+=16 {\nleftBlock:=left[base+off:base+off+16]\nrightBlock:=right[base+off:base+off+16]\noutBlock:=out[base+off:base+off+16]\n")
					for part := 0; part < 16; part += lanes {
						fmt.Fprintf(b, "{\nlaneOffset:=off+%d\na:=archsimd.Load%sx%d(leftBlock[%d:])\nb:=archsimd.Load%sx%d(rightBlock[%d:])\n", part, typ, lanes, part, typ, lanes, part)
						emitCheckedInteger32Vector(b, arch, typ, op, width)
						fmt.Fprintf(b, "result.Store(outBlock[%d:])\n}\n", part)
					}
					b.WriteString("}\n")
				}
				fmt.Fprintf(b, "for ; off+%d<=rows; off+=%d {\nlaneOffset:=off\na:=archsimd.Load%sx%d(left[base+off:])\nb:=archsimd.Load%sx%d(right[base+off:])\n", lanes, lanes, typ, lanes, typ, lanes)
				emitCheckedInteger32Vector(b, arch, typ, op, width)
				b.WriteString("result.Store(out[base+off:])\n}\n")
				if arch == "amd64" {
					b.WriteString("archsimd.ClearAVXUpperBits()\n")
				}
				b.WriteString("for ; off<rows; off++ {\nrow:=base+off\na,b:=left[row],right[row]\n")
				symbol = map[string]string{"Add": "+", "Sub": "-", "Mul": "*"}[op]
				fmt.Fprintf(b, "value:=a %s b\n", symbol)
				switch {
				case typ == "Int32" && op == "Add":
					b.WriteString("failed:=(a^value)&(b^value)<0\n")
				case typ == "Int32" && op == "Sub":
					b.WriteString("failed:=(a^b)&(a^value)<0\n")
				case typ == "Int32" && op == "Mul":
					b.WriteString("failed:=int64(a)*int64(b)!=int64(value)\n")
				case op == "Add":
					b.WriteString("failed:=value<a\n")
				case op == "Sub":
					b.WriteString("failed:=a<b\n")
				default:
					b.WriteString("failed:=uint64(a)*uint64(b)>0xffffffff\n")
				}
				b.WriteString(`if failed { failedWord |= uint64(1)<<uint(off) }
out[row]=value
}
failedWord &= validWord
if overflow!=nil { overflow[wi]=failedWord }
if first<0 && failedWord!=0 { first=base+bits.TrailingZeros64(failedWord) }
invalid:= ^validWord & memory.TailMask(rows)
for invalid!=0 { lane:=bits.TrailingZeros64(invalid); out[base+lane]=0; invalid &= invalid-1 }
}
return first
}

`)
			}
		}
	}
}

func emitCheckedInteger32Vector(b *bytes.Buffer, arch, typ, op string, width int) {
	lanes := width / 32
	fmt.Fprintf(b, "result:=a.%s(b)\n", op)
	switch op {
	case "Add", "Sub":
		if typ == "Int32" {
			formula := "a.Xor(result).And(b.Xor(result))"
			if op == "Sub" {
				formula = "a.Xor(b).And(a.Xor(result))"
			}
			fmt.Fprintf(b, "failed:=%s.Less(archsimd.Int32x%d{})\n", formula, lanes)
		} else if op == "Add" {
			b.WriteString("failed:=result.Less(a)\n")
		} else {
			b.WriteString("failed:=a.Less(b)\n")
		}
	case "Mul":
		if arch == "arm64" {
			b.WriteString("even:=a.ConcatEven(a).MulWidenLo(b.ConcatEven(b))\nodd:=a.ConcatOdd(a).MulWidenLo(b.ConcatOdd(b))\n")
		} else if width < 512 {
			b.WriteString("even:=a.MulWidenEven(b)\n")
			shift := ".ReshapeToUint64s().ShiftAllRight(32).ReshapeToUint32s()"
			if typ == "Int32" {
				shift = ".ToBits()" + shift + ".BitsToInt32()"
			}
			fmt.Fprintf(b, "odd:=a%s.MulWidenEven(b%s)\n", shift, shift)
		} else if typ == "Int32" {
			// Arithmetic 64-bit shifts and packed 64-bit products require AVX512.
			b.WriteString("a64,b64:=a.ToBits().ReshapeToUint64s().BitsToInt64(),b.ToBits().ReshapeToUint64s().BitsToInt64()\neven:=a64.ShiftAllLeft(32).ShiftAllRight(32).Mul(b64.ShiftAllLeft(32).ShiftAllRight(32))\nodd:=a64.ShiftAllRight(32).Mul(b64.ShiftAllRight(32))\n")
		} else {
			fmt.Fprintf(b, "a64,b64:=a.ReshapeToUint64s(),b.ReshapeToUint64s()\nlowMask:=archsimd.BroadcastUint64x%d(0xffffffff)\neven:=a64.And(lowMask).Mul(b64.And(lowMask))\nodd:=a64.ShiftAllRight(32).Mul(b64.ShiftAllRight(32))\n", lanes/2)
		}
		suffix := ""
		if typ == "Int32" {
			suffix = ".ToBits()"
		}
		fmt.Fprintf(b, "// Reassemble the high 32 bits of each exact 64-bit product in row order.\nhigh:=even%s.ShiftAllRight(32).Or(odd%s.And(archsimd.BroadcastUint64x%d(0xffffffff00000000))).ReshapeToUint32s()\n", suffix, suffix, lanes/2)
		if typ == "Int32" {
			b.WriteString("failed:=high.BitsToInt32().NotEqual(result.ShiftAllRight(31))\n")
		} else {
			fmt.Fprintf(b, "failed:=high.NotEqual(archsimd.Uint32x%d{})\n", lanes)
		}
	}
	if arch == "amd64" {
		b.WriteString("failedWord |= uint64(failed.ToBits()) << uint(laneOffset)\n")
	} else {
		b.WriteString("mask:=failed.ToInt32x4()\nfailedWord |= ((uint64(uint32(mask.GetElem(0)))>>31) | ((uint64(uint32(mask.GetElem(1)))>>31)<<1) | ((uint64(uint32(mask.GetElem(2)))>>31)<<2) | ((uint64(uint32(mask.GetElem(3)))>>31)<<3)) << uint(laneOffset)\n")
	}
}

func emitCheckedInteger32Scalar(b *bytes.Buffer, typ, op, symbol string) {
	if typ == "Int32" {
		fmt.Fprintf(b, "wide:=int64(a) %s int64(b)\nvalue:=int32(wide)\nfailed:=wide < -2147483648 || wide > 2147483647\n", symbol)
	} else {
		fmt.Fprintf(b, "value:=a %s b\n", symbol)
		switch op {
		case "Add":
			b.WriteString("failed:=value<a\n")
		case "Sub":
			b.WriteString("failed:=a<b\n")
		case "Mul":
			b.WriteString("failed:=uint64(a)*uint64(b)>0xffffffff\n")
		}
	}
}
