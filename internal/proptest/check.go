package proptest

import (
	"fmt"
	"strings"

	"gocolumn/memory"
)

// CheckEqual verifies that want and got are identical relations: same schema
// (column names and types in order), same row count, and bitwise-equal cells
// including NULL flags. The returned error names the first diverging cell.
func CheckEqual(want, got *Relation) error {
	if want == nil || got == nil {
		return fmt.Errorf("nil relation (want nil=%v, got nil=%v)", want == nil, got == nil)
	}
	if err := checkSchemaEqual(want.Fields, got.Fields); err != nil {
		return err
	}
	if len(want.Rows) != len(got.Rows) {
		return fmt.Errorf("row count mismatch: want %d rows, got %d rows", len(want.Rows), len(got.Rows))
	}
	for i := range want.Rows {
		for j, field := range want.Fields {
			w, g := want.Rows[i][j], got.Rows[i][j]
			if !cellsEqual(w, g) {
				return fmt.Errorf("row %d column %q (%s): want %s, got %s", i, field.Name, field.Type, formatCell(w), formatCell(g))
			}
		}
	}
	return nil
}

func checkSchemaEqual(want, got []memory.Field) error {
	if len(want) != len(got) {
		return fmt.Errorf("column count mismatch: want %d columns, got %d columns", len(want), len(got))
	}
	for i := range want {
		if want[i].Name != got[i].Name || want[i].Type != got[i].Type {
			return fmt.Errorf("column %d mismatch: want %q (%s), got %q (%s)", i, want[i].Name, want[i].Type, got[i].Name, got[i].Type)
		}
	}
	return nil
}

// CheckRowPermutation verifies that output holds exactly the rows of input in
// any order: same schema and the same multiset of rows, duplicates included.
// It is the row-content half of a sorted-output correctness argument.
func CheckRowPermutation(input, output *Relation) error {
	if input == nil || output == nil {
		return fmt.Errorf("nil relation (input nil=%v, output nil=%v)", input == nil, output == nil)
	}
	if err := checkSchemaEqual(input.Fields, output.Fields); err != nil {
		return err
	}
	if len(input.Rows) != len(output.Rows) {
		return fmt.Errorf("row count mismatch: input has %d rows, output has %d rows", len(input.Rows), len(output.Rows))
	}
	counts := make(map[string]int, len(input.Rows))
	inputExample := make(map[string]string, len(input.Rows))
	for _, row := range input.Rows {
		key := rowKey(row)
		counts[key]++
		inputExample[key] = formatRow(input.Fields, row)
	}
	outputExample := make(map[string]string, len(output.Rows))
	for _, row := range output.Rows {
		key := rowKey(row)
		counts[key]--
		if _, ok := outputExample[key]; !ok {
			outputExample[key] = formatRow(output.Fields, row)
		}
	}
	// Report deterministically: the first missing input row, then the first
	// unexpected output row.
	for _, row := range input.Rows {
		key := rowKey(row)
		if counts[key] > 0 {
			return fmt.Errorf("output is missing %d occurrence(s) of row %s", counts[key], inputExample[key])
		}
	}
	for _, row := range output.Rows {
		key := rowKey(row)
		if counts[key] < 0 {
			return fmt.Errorf("output contains %d unexpected occurrence(s) of row %s", -counts[key], outputExample[key])
		}
	}
	return nil
}

// rowKey serializes a row for multiset comparison. Type tags prevent
// collisions between distinct dynamic types whose %v spelling matches.
func rowKey(row []Cell) string {
	var b strings.Builder
	for _, c := range row {
		if !c.Valid {
			b.WriteString("N;")
			continue
		}
		fmt.Fprintf(&b, "%T:%#v;", c.Value, c.Value)
	}
	return b.String()
}

func formatRow(fields []memory.Field, row []Cell) string {
	parts := make([]string, len(row))
	for i, c := range row {
		name := "?"
		if i < len(fields) {
			name = fields[i].Name
		}
		parts[i] = name + "=" + formatCell(c)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// CheckSorted verifies that rel is ordered on column: non-decreasing when
// descending is false, non-increasing otherwise. When nullsLast is true every
// NULL must follow all valid values; when false every NULL must precede them.
func CheckSorted(rel *Relation, column string, descending, nullsLast bool) error {
	if rel == nil {
		return fmt.Errorf("nil relation")
	}
	idx, ok := rel.ColumnIndex(column)
	if !ok {
		return fmt.Errorf("sort column %q not present in relation", column)
	}
	direction := "ascending"
	if descending {
		direction = "descending"
	}
	seenNull := false
	var prev Cell
	for i, row := range rel.Rows {
		cur := row[idx]
		if !cur.Valid {
			seenNull = true
			if !nullsLast && i != 0 && prev.Valid {
				return fmt.Errorf("row %d: NULL follows valid value but nullsLast=false", i)
			}
			prev = cur
			continue
		}
		if seenNull && nullsLast {
			return fmt.Errorf("row %d: valid value %s follows NULL but nullsLast=true", i, formatCell(cur))
		}
		if prev.Value != nil && prev.Valid {
			cmp, err := compareValues(prev.Value, cur.Value)
			if err != nil {
				return fmt.Errorf("row %d: %w", i, err)
			}
			if !descending && cmp > 0 {
				return fmt.Errorf("row %d: %s order violated, %s > %s", i, direction, formatCell(prev), formatCell(cur))
			}
			if descending && cmp < 0 {
				return fmt.Errorf("row %d: %s order violated, %s < %s", i, direction, formatCell(prev), formatCell(cur))
			}
		}
		prev = cur
	}
	return nil
}

// CheckTopKPrefix verifies that top holds exactly the first min(k, len(full))
// rows of full, row by row. full is expected to be the completely sorted
// relation that the top-k query approximates; ties at the cut boundary are
// the caller's responsibility (use unique ordering keys).
func CheckTopKPrefix(full, top *Relation, k int) error {
	if k < 0 {
		return fmt.Errorf("top-k prefix requires a non-negative k, got %d", k)
	}
	if full == nil || top == nil {
		return fmt.Errorf("nil relation (full nil=%v, top nil=%v)", full == nil, top == nil)
	}
	want := k
	if want > len(full.Rows) {
		want = len(full.Rows)
	}
	if len(top.Rows) != want {
		return fmt.Errorf("top-%d result has %d rows, want %d", k, len(top.Rows), want)
	}
	prefix := &Relation{Fields: full.Fields, Rows: full.Rows[:want]}
	if err := CheckEqual(prefix, top); err != nil {
		return fmt.Errorf("top-%d result diverges from full sort prefix: %w", k, err)
	}
	return nil
}
