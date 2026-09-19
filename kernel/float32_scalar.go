package kernel

import "gocolumn/memory"

func installFloat32Scalar(d *dispatch) {
	d.compareFloat32[CompareGT] = scalarFloat32GT
	d.compareFloat32[CompareGE] = scalarFloat32GE
	d.compareFloat32[CompareLT] = scalarFloat32LT
	d.compareFloat32[CompareLE] = scalarFloat32LE
	d.compareFloat32[CompareEQ] = scalarFloat32EQ
	d.compareFloat32Columns[CompareGT] = scalarFloat32ColumnsGT
	d.compareFloat32Columns[CompareGE] = scalarFloat32ColumnsGE
	d.compareFloat32Columns[CompareLT] = scalarFloat32ColumnsLT
	d.compareFloat32Columns[CompareLE] = scalarFloat32ColumnsLE
	d.compareFloat32Columns[CompareEQ] = scalarFloat32ColumnsEQ
}

func scalarFloat32Columns(left, right []float32, leftValid, rightValid, out memory.Bitmap, op CompareOp) int {
	if len(left) != len(right) {
		panic("kernel: float32 column comparison length mismatch")
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

func scalarFloat32ColumnsGT(a, b []float32, av, bv, out memory.Bitmap) int {
	return scalarFloat32Columns(a, b, av, bv, out, CompareGT)
}
func scalarFloat32ColumnsGE(a, b []float32, av, bv, out memory.Bitmap) int {
	return scalarFloat32Columns(a, b, av, bv, out, CompareGE)
}
func scalarFloat32ColumnsLT(a, b []float32, av, bv, out memory.Bitmap) int {
	return scalarFloat32Columns(a, b, av, bv, out, CompareLT)
}
func scalarFloat32ColumnsLE(a, b []float32, av, bv, out memory.Bitmap) int {
	return scalarFloat32Columns(a, b, av, bv, out, CompareLE)
}
func scalarFloat32ColumnsEQ(a, b []float32, av, bv, out memory.Bitmap) int {
	return scalarFloat32Columns(a, b, av, bv, out, CompareEQ)
}

func scalarFloat32Compare(values []float32, validity, out memory.Bitmap, threshold float32, op CompareOp) int {
	prepareComparison(len(values), out)
	selected := 0
	for row, value := range values {
		if !validAt(validity, row) {
			continue
		}
		match := false
		switch op {
		case CompareGT:
			match = value > threshold
		case CompareGE:
			match = value >= threshold
		case CompareLT:
			match = value < threshold
		case CompareLE:
			match = value <= threshold
		case CompareEQ:
			match = value == threshold
		}
		if match {
			setSelected(out, row, &selected)
		}
	}
	return selected
}
func scalarFloat32GT(v []float32, m, o memory.Bitmap, t float32) int {
	return scalarFloat32Compare(v, m, o, t, CompareGT)
}
func scalarFloat32GE(v []float32, m, o memory.Bitmap, t float32) int {
	return scalarFloat32Compare(v, m, o, t, CompareGE)
}
func scalarFloat32LT(v []float32, m, o memory.Bitmap, t float32) int {
	return scalarFloat32Compare(v, m, o, t, CompareLT)
}
func scalarFloat32LE(v []float32, m, o memory.Bitmap, t float32) int {
	return scalarFloat32Compare(v, m, o, t, CompareLE)
}
func scalarFloat32EQ(v []float32, m, o memory.Bitmap, t float32) int {
	return scalarFloat32Compare(v, m, o, t, CompareEQ)
}
