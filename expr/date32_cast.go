package expr

import (
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"strconv"
	"time"
)

const date32NanosPerDay int64 = 86_400_000_000_000

var (
	errDate32Syntax         = errors.New("expected a valid ISO calendar date")
	errDate32Range          = errors.New("date is outside the date32 epoch-day range")
	errDate32TimestampRange = errors.New("UTC midnight is outside the timestamp nanosecond range")
)

// date32CastSupported is the single admission contract shared by strict and
// tolerant casts. Text formatting remains in the common scalar text writer.
func date32CastSupported(from, to memory.DataType) bool {
	if to == memory.TypeDate32 {
		return from == memory.TypeString || from == memory.TypeDate32 || from == memory.TypeTimestamp || from == memory.TypeInt32 || from == memory.TypeInt64
	}
	return from == memory.TypeDate32 && (to == memory.TypeTimestamp || to == memory.TypeInt32 || to == memory.TypeInt64)
}

func evalDate32Cast(out, in *scalarRegister, from, to memory.DataType, rows int, tolerant bool) error {
	copyRegisterValidity(out, in, rows)
	for row := 0; row < rows; row++ {
		if row&255 == 0 && out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		var value int64
		var err error
		if isValidRegister(out, row) {
			if from == memory.TypeString {
				text, readErr := castStringBytes(in, row)
				if readErr != nil {
					return readErr // Structural/storage failures never become NULL.
				}
				var days int32
				days, err = parseDate32Text(text)
				value = int64(days)
			} else {
				value, err = registerInt64(in, row)
				if err != nil {
					return err
				}
				switch {
				case from == memory.TypeTimestamp:
					// Go integer division truncates. Calendar days require floor,
					// including the sub-day interval immediately before the epoch.
					days := value / date32NanosPerDay
					if value%date32NanosPerDay < 0 {
						days--
					}
					value = days
				case to == memory.TypeTimestamp:
					if value < math.MinInt64/date32NanosPerDay || value > math.MaxInt64/date32NanosPerDay {
						err = errDate32TimestampRange
					} else {
						value *= date32NanosPerDay
					}
				case to == memory.TypeDate32 && (value < math.MinInt32 || value > math.MaxInt32):
					err = errDate32Range
				}
			}
			if err != nil {
				if !tolerant {
					return fmt.Errorf("cast %s to %s at row %d: %w", from, to, row, err)
				}
				invalidateCastRow(out, rows, row)
				value = 0
			}
		}
		if to == memory.TypeDate32 || to == memory.TypeInt32 {
			out.i32[row] = int32(value)
		} else {
			out.i64[row] = value
		}
	}
	return nil
}

// parseDate32Text accepts one canonical spelling per date in the proleptic
// Gregorian calendar, with astronomical year numbering (0000 is 1 BCE).
// Years 0000..9999 use four digits, negative years use '-' and at least four
// digits, and positive extended years use '+' and at least five digits. The
// entire Date32 domain fits in seven year digits. Bounds are checked before
// calendar construction, so arbitrarily long invalid strings take O(1) work
// and do not allocate a copied error token.
func parseDate32Text(text []byte) (int32, error) {
	if len(text) < 10 || len(text) > 14 {
		return 0, errDate32Syntax
	}
	yearEnd := len(text) - 6
	if text[yearEnd] != '-' || text[yearEnd+3] != '-' {
		return 0, errDate32Syntax
	}
	start, negative := 0, false
	if text[0] == '+' || text[0] == '-' {
		start, negative = 1, text[0] == '-'
	}
	digits := yearEnd - start
	if start == 0 && digits != 4 || start == 1 && (digits < 4 || digits > 7) || digits > 4 && text[start] == '0' {
		return 0, errDate32Syntax
	}
	year := 0
	for _, c := range text[start:yearEnd] {
		if c < '0' || c > '9' {
			return 0, errDate32Syntax
		}
		year = year*10 + int(c-'0')
	}
	if start == 1 && ((!negative && year <= 9999) || negative && year == 0) {
		return 0, errDate32Syntax
	}
	if negative {
		year = -year
	}
	for _, i := range [...]int{yearEnd + 1, yearEnd + 2, yearEnd + 4, yearEnd + 5} {
		if text[i] < '0' || text[i] > '9' {
			return 0, errDate32Syntax
		}
	}
	month := int(text[yearEnd+1]-'0')*10 + int(text[yearEnd+2]-'0')
	day := int(text[yearEnd+4]-'0')*10 + int(text[yearEnd+5]-'0')
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return 0, errDate32Syntax
	}
	instant := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if instant.Day() != day { // time.Date normalizes impossible month/day pairs.
		return 0, errDate32Syntax
	}
	days := instant.Unix() / 86400 // UTC midnight is an exact multiple for every year.
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, errDate32Range
	}
	return int32(days), nil
}

func appendDate32Text(dst []byte, days int32) []byte {
	year, month, day := time.Unix(int64(days)*86400, 0).UTC().Date()
	if year < 0 {
		dst = append(dst, '-')
		year = -year
	} else if year > 9999 {
		dst = append(dst, '+')
	}
	switch {
	case year < 10:
		dst = append(dst, '0', '0', '0')
	case year < 100:
		dst = append(dst, '0', '0')
	case year < 1000:
		dst = append(dst, '0')
	}
	dst = strconv.AppendInt(dst, int64(year), 10)
	return append(dst, '-', byte(month/10)+'0', byte(month%10)+'0', '-', byte(day/10)+'0', byte(day%10)+'0')
}
