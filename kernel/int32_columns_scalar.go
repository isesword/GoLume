package kernel

import "gocolumn/memory"

func installInt32ColumnScalar(d *dispatch) {
	d.compareInt32Columns[CompareGT] = scalarInt32ColumnsGT
	d.compareInt32Columns[CompareGE] = scalarInt32ColumnsGE
	d.compareInt32Columns[CompareLT] = scalarInt32ColumnsLT
	d.compareInt32Columns[CompareLE] = scalarInt32ColumnsLE
	d.compareInt32Columns[CompareEQ] = scalarInt32ColumnsEQ
}

func scalarInt32Columns(left, right []int32, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: int32 column comparison length mismatch")
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

func scalarInt32ColumnsGT(a, b []int32, av, bv, out memory.Bitmap) int {
	return scalarInt32Columns(a, b, av, bv, out, CompareGT)
}
func scalarInt32ColumnsGE(a, b []int32, av, bv, out memory.Bitmap) int {
	return scalarInt32Columns(a, b, av, bv, out, CompareGE)
}
func scalarInt32ColumnsLT(a, b []int32, av, bv, out memory.Bitmap) int {
	return scalarInt32Columns(a, b, av, bv, out, CompareLT)
}
func scalarInt32ColumnsLE(a, b []int32, av, bv, out memory.Bitmap) int {
	return scalarInt32Columns(a, b, av, bv, out, CompareLE)
}
func scalarInt32ColumnsEQ(a, b []int32, av, bv, out memory.Bitmap) int {
	return scalarInt32Columns(a, b, av, bv, out, CompareEQ)
}
