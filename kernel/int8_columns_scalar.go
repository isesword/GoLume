package kernel

import "gocolumn/memory"

func installInt8ColumnScalar(d *dispatch) {
	d.compareInt8Columns[CompareGT] = scalarInt8ColumnsGT
	d.compareInt8Columns[CompareGE] = scalarInt8ColumnsGE
	d.compareInt8Columns[CompareLT] = scalarInt8ColumnsLT
	d.compareInt8Columns[CompareLE] = scalarInt8ColumnsLE
	d.compareInt8Columns[CompareEQ] = scalarInt8ColumnsEQ
}

func scalarInt8Columns(left, right []int8, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: int8 column comparison length mismatch")
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

func scalarInt8ColumnsGT(a, b []int8, av, bv, out memory.Bitmap) int {
	return scalarInt8Columns(a, b, av, bv, out, CompareGT)
}
func scalarInt8ColumnsGE(a, b []int8, av, bv, out memory.Bitmap) int {
	return scalarInt8Columns(a, b, av, bv, out, CompareGE)
}
func scalarInt8ColumnsLT(a, b []int8, av, bv, out memory.Bitmap) int {
	return scalarInt8Columns(a, b, av, bv, out, CompareLT)
}
func scalarInt8ColumnsLE(a, b []int8, av, bv, out memory.Bitmap) int {
	return scalarInt8Columns(a, b, av, bv, out, CompareLE)
}
func scalarInt8ColumnsEQ(a, b []int8, av, bv, out memory.Bitmap) int {
	return scalarInt8Columns(a, b, av, bv, out, CompareEQ)
}
