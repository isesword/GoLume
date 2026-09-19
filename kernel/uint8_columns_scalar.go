package kernel

import "gocolumn/memory"

func installUint8ColumnScalar(d *dispatch) {
	d.compareUint8Columns[CompareGT] = scalarUint8ColumnsGT
	d.compareUint8Columns[CompareGE] = scalarUint8ColumnsGE
	d.compareUint8Columns[CompareLT] = scalarUint8ColumnsLT
	d.compareUint8Columns[CompareLE] = scalarUint8ColumnsLE
	d.compareUint8Columns[CompareEQ] = scalarUint8ColumnsEQ
}

func scalarUint8Columns(left, right []uint8, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: uint8 column comparison length mismatch")
	}
	prepareComparison(len(left), out)
	selected := 0
	for row, a := range left {
		if !validAt(leftValid, row) || !validAt(rightValid, row) {
			continue
		}
		b := right[row]
		match := false
		switch op {
		case CompareGT:
			match = a > b
		case CompareGE:
			match = a >= b
		case CompareLT:
			match = a < b
		case CompareLE:
			match = a <= b
		case CompareEQ:
			match = a == b
		}
		if match {
			setSelected(out, row, &selected)
		}
	}
	return selected
}

func scalarUint8ColumnsGT(a, b []uint8, av, bv, out memory.Bitmap) int {
	return scalarUint8Columns(a, b, av, bv, out, CompareGT)
}
func scalarUint8ColumnsGE(a, b []uint8, av, bv, out memory.Bitmap) int {
	return scalarUint8Columns(a, b, av, bv, out, CompareGE)
}
func scalarUint8ColumnsLT(a, b []uint8, av, bv, out memory.Bitmap) int {
	return scalarUint8Columns(a, b, av, bv, out, CompareLT)
}
func scalarUint8ColumnsLE(a, b []uint8, av, bv, out memory.Bitmap) int {
	return scalarUint8Columns(a, b, av, bv, out, CompareLE)
}
func scalarUint8ColumnsEQ(a, b []uint8, av, bv, out memory.Bitmap) int {
	return scalarUint8Columns(a, b, av, bv, out, CompareEQ)
}
