package lazy

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/rivo/uniseg"

	"gocolumn/memory"
)

// DisplayOptions bounds DataFrame and FrameView rendering. Non-positive values
// use the documented defaults.
type DisplayOptions struct {
	MaxRows       int
	MaxColumns    int
	MaxCellWidth  int
	MaxTableWidth int
}

func normalizeDisplayOptions(opts DisplayOptions) DisplayOptions {
	if opts.MaxRows <= 0 {
		opts.MaxRows = 20
	}
	if opts.MaxColumns <= 0 {
		opts.MaxColumns = 12
	}
	if opts.MaxCellWidth <= 0 {
		opts.MaxCellWidth = 32
	}
	if opts.MaxTableWidth <= 0 {
		opts.MaxTableWidth = 120
	}
	return opts
}

// Format renders bounded materialized data without executing a lazy query.
func (f DataFrame) Format(opts DisplayOptions) (string, error) {
	if f.state == nil {
		return "", ErrDataFrameClosed
	}
	return formatFrameRange(f.state, 0, f.Height(), opts)
}

// String renders the default bounded representation.
func (f DataFrame) String() string {
	text, err := f.Format(DisplayOptions{})
	if err != nil {
		return "<closed DataFrame>"
	}
	return text
}

// Format renders the bounded borrowed view.
func (v FrameView) Format(opts DisplayOptions) (string, error) {
	if v.state == nil {
		return "", ErrDataFrameClosed
	}
	return formatFrameRange(v.state, v.offset, v.length, opts)
}

// String renders the default bounded representation of the view.
func (v FrameView) String() string {
	text, err := v.Format(DisplayOptions{})
	if err != nil {
		return "<closed DataFrame>"
	}
	return text
}

type displayColumn struct {
	field    *memory.Field
	ellipsis bool
}
type displayRow struct {
	row      int64
	ellipsis bool
}

func formatFrameRange(state *dataFrameState, offset, length int64, raw DisplayOptions) (string, error) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	if state.closed {
		return "", ErrDataFrameClosed
	}
	opts := normalizeDisplayOptions(raw)
	columns := selectDisplayColumns(state.schema.Fields, opts.MaxColumns)
	rows := selectDisplayRows(offset, length, opts.MaxRows)
	cells := make([][]string, len(rows)+1)
	cells[0] = make([]string, len(columns))
	for i, column := range columns {
		if column.ellipsis {
			cells[0][i] = "…"
		} else {
			cells[0][i] = column.field.Name
		}
	}
	for ri, selected := range rows {
		cells[ri+1] = make([]string, len(columns))
		for ci, column := range columns {
			if selected.ellipsis || column.ellipsis {
				cells[ri+1][ci] = "…"
				continue
			}
			value, err := displayValueAt(state, *column.field, selected.row)
			if err != nil {
				return "", err
			}
			cells[ri+1][ci] = value
		}
	}
	widths := make([]int, len(columns))
	for _, row := range cells {
		for i, cell := range row {
			width := uniseg.StringWidth(cell)
			if width > opts.MaxCellWidth {
				width = opts.MaxCellWidth
			}
			if width > widths[i] {
				widths[i] = width
			}
		}
	}
	shrinkWidths(widths, opts.MaxTableWidth)
	var b strings.Builder
	fmt.Fprintf(&b, "shape: (%d, %d)\n", length, len(state.schema.Fields))
	writeBorder(&b, "┌", "┬", "┐", widths)
	writeDisplayRow(&b, cells[0], widths)
	writeBorder(&b, "├", "┼", "┤", widths)
	for _, row := range cells[1:] {
		writeDisplayRow(&b, row, widths)
	}
	writeBorder(&b, "└", "┴", "┘", widths)
	return b.String(), nil
}

func selectDisplayColumns(fields []memory.Field, max int) []displayColumn {
	if len(fields) <= max {
		out := make([]displayColumn, len(fields))
		for i := range fields {
			out[i].field = &fields[i]
		}
		return out
	}
	left := max / 2
	right := max - left
	out := make([]displayColumn, 0, max+1)
	for i := 0; i < left; i++ {
		out = append(out, displayColumn{field: &fields[i]})
	}
	out = append(out, displayColumn{ellipsis: true})
	for i := len(fields) - right; i < len(fields); i++ {
		out = append(out, displayColumn{field: &fields[i]})
	}
	return out
}

func selectDisplayRows(offset, length int64, max int) []displayRow {
	if length <= int64(max) {
		out := make([]displayRow, length)
		for i := range out {
			out[i].row = offset + int64(i)
		}
		return out
	}
	first := max / 2
	last := max - first
	out := make([]displayRow, 0, max+1)
	for i := 0; i < first; i++ {
		out = append(out, displayRow{row: offset + int64(i)})
	}
	out = append(out, displayRow{ellipsis: true})
	for i := last; i > 0; i-- {
		out = append(out, displayRow{row: offset + length - int64(i)})
	}
	return out
}

func shrinkWidths(widths []int, max int) {
	for tableWidth(widths) > max {
		largest := -1
		for i, w := range widths {
			if w > 3 && (largest < 0 || w > widths[largest]) {
				largest = i
			}
		}
		if largest < 0 {
			return
		}
		widths[largest]--
	}
}
func tableWidth(widths []int) int {
	n := 1
	for _, w := range widths {
		n += w + 3
	}
	return n
}
func writeBorder(b *strings.Builder, left, middle, right string, widths []int) {
	b.WriteString(left)
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i+1 < len(widths) {
			b.WriteString(middle)
		}
	}
	b.WriteString(right)
	b.WriteByte('\n')
}
func writeDisplayRow(b *strings.Builder, cells []string, widths []int) {
	b.WriteString("│")
	for i, cell := range cells {
		cell = truncateDisplay(cell, widths[i])
		b.WriteByte(' ')
		b.WriteString(cell)
		b.WriteString(strings.Repeat(" ", widths[i]-uniseg.StringWidth(cell)+1))
		b.WriteString("│")
	}
	b.WriteByte('\n')
}
func truncateDisplay(value string, width int) string {
	if uniseg.StringWidth(value) <= width {
		return value
	}
	if width <= 1 {
		return "…"
	}
	var b strings.Builder
	used := 0
	g := uniseg.NewGraphemes(value)
	for g.Next() {
		part := g.Str()
		w := uniseg.StringWidth(part)
		if used+w > width-1 {
			break
		}
		b.WriteString(part)
		used += w
	}
	b.WriteRune('…')
	return b.String()
}

func displayValueAt(state *dataFrameState, field memory.Field, row int64) (string, error) {
	position := int64(0)
	for _, batch := range state.batches {
		end := position + int64(batch.Rows)
		if row < end {
			return displayBatchValue(batch, field, int(row-position))
		}
		position = end
	}
	return "", fmt.Errorf("display row %d outside data frame", row)
}

func displayBatchValue(batch *memory.Batch, field memory.Field, row int) (string, error) {
	switch {
	case isI32Type(field.Type):
		v, e := batch.Int32(field.Name)
		if e != nil {
			return "", e
		}
		if !v.IsValid(row) {
			return "NULL", nil
		}
		return strconv.FormatInt(int64(v.Values[row]), 10), nil
	case isI64Type(field.Type):
		v, e := batch.Int64(field.Name)
		if e != nil {
			return "", e
		}
		if !v.IsValid(row) {
			return "NULL", nil
		}
		return strconv.FormatInt(v.Values[row], 10), nil
	case isU32Type(field.Type):
		v, e := batch.Uint32(field.Name)
		if e != nil {
			return "", e
		}
		if !v.IsValid(row) {
			return "NULL", nil
		}
		if field.Type == memory.TypeBool {
			return strconv.FormatBool(v.Values[row] != 0), nil
		}
		return strconv.FormatUint(uint64(v.Values[row]), 10), nil
	case field.Type == memory.TypeUint64:
		v, e := batch.Uint64(field.Name)
		if e != nil {
			return "", e
		}
		if !v.IsValid(row) {
			return "NULL", nil
		}
		return strconv.FormatUint(v.Values[row], 10), nil
	case field.Type == memory.TypeFloat32 || field.Type == memory.TypeFloat64:
		v, e := batch.Float64(field.Name)
		if e != nil {
			return "", e
		}
		if !v.IsValid(row) {
			return "NULL", nil
		}
		return strconv.FormatFloat(v.Values[row], 'g', -1, 64), nil
	case field.Type == memory.TypeString:
		var value []byte
		valid := false
		if v := batch.Strings[field.Name]; v != nil {
			valid = v.IsValid(row)
			value = v.ValueBytes(row)
		} else if v := batch.DictStrings[field.Name]; v != nil {
			valid = v.IsValid(row)
			if valid {
				value = v.DictionaryValue(v.Indices[row])
			}
		} else {
			return "", fmt.Errorf("string column %q not present", field.Name)
		}
		if !valid {
			return "NULL", nil
		}
		return displayString(string(value)), nil
	case field.Type == memory.TypeList:
		if v := batch.Int64Lists[field.Name]; v != nil {
			start, end, ok := v.ValueRange(row)
			if !ok {
				return "NULL", nil
			}
			items := make([]string, 0, end-start)
			for i := start; i < end; i++ {
				if !v.Values.IsValid(i) {
					items = append(items, "NULL")
				} else {
					items = append(items, fmt.Sprint(v.Values.Values[i]))
				}
			}
			return "[" + strings.Join(items, ", ") + "]", nil
		}
		v, e := batch.StringList(field.Name)
		if e != nil {
			return "", e
		}
		start, end, ok := v.ValueRange(row)
		if !ok {
			return "NULL", nil
		}
		items := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			if !v.Values.IsValid(i) {
				items = append(items, "NULL")
			} else {
				items = append(items, displayString(string(v.Values.ValueBytes(i))))
			}
		}
		return "[" + strings.Join(items, ", ") + "]", nil
	case field.Type == memory.TypeStruct:
		v, e := batch.Struct(field.Name)
		if e != nil {
			return "", e
		}
		if !v.IsValid(row) {
			return "NULL", nil
		}
		items := make([]string, 0, len(v.Fields))
		for _, child := range v.Fields {
			value, err := displayBatchValue(v.Values, child, row)
			if err != nil {
				return "", err
			}
			items = append(items, child.Name+": "+value)
		}
		return "{" + strings.Join(items, ", ") + "}", nil
	default:
		return "<" + field.Type.String() + ">", nil
	}
}

func displayString(value string) string {
	if value == "" {
		return `""`
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return strconv.QuoteToGraphic(value)
		}
	}
	return value
}
