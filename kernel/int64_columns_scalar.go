package kernel

import "gocolumn/memory"

func installInt64ColumnScalar(d *dispatch) {
	d.compareInt64Columns[CompareGT] = scalarInt64ColumnsGT
	d.compareInt64Columns[CompareGE] = scalarInt64ColumnsGE
	d.compareInt64Columns[CompareLT] = scalarInt64ColumnsLT
	d.compareInt64Columns[CompareLE] = scalarInt64ColumnsLE
	d.compareInt64Columns[CompareEQ] = scalarInt64ColumnsEQ
}

func scalarInt64Columns(left, right []int64, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: int64 column comparison length mismatch")
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

func scalarInt64ColumnsGT(a, b []int64, av, bv, out memory.Bitmap) int {
	return scalarInt64Columns(a, b, av, bv, out, CompareGT)
}
func scalarInt64ColumnsGE(a, b []int64, av, bv, out memory.Bitmap) int {
	return scalarInt64Columns(a, b, av, bv, out, CompareGE)
}
func scalarInt64ColumnsLT(a, b []int64, av, bv, out memory.Bitmap) int {
	return scalarInt64Columns(a, b, av, bv, out, CompareLT)
}
func scalarInt64ColumnsLE(a, b []int64, av, bv, out memory.Bitmap) int {
	return scalarInt64Columns(a, b, av, bv, out, CompareLE)
}
func scalarInt64ColumnsEQ(a, b []int64, av, bv, out memory.Bitmap) int {
	return scalarInt64Columns(a, b, av, bv, out, CompareEQ)
}
