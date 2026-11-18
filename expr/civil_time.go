package expr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// CivilFoldPolicy selects how a resolver treats multiple matching instants.
type CivilFoldPolicy uint8

const (
	// FoldReject rejects ambiguous civil times.
	FoldReject CivilFoldPolicy = iota + 1
	// FoldEarlier selects the earliest matching instant.
	FoldEarlier
	// FoldLater selects the latest matching instant.
	FoldLater
)

// CivilGapPolicy selects how a resolver treats nonexistent civil times.
type CivilGapPolicy uint8

// GapReject rejects nonexistent civil times.
const GapReject CivilGapPolicy = 1

// CivilResolveOptions requires explicit fold and gap policies.
type CivilResolveOptions struct {
	Fold CivilFoldPolicy
	Gap  CivilGapPolicy
}

var (
	// ErrCivilConfiguration identifies invalid resolver configuration or provenance.
	ErrCivilConfiguration = errors.New("invalid civil-time configuration")
	// ErrCivilSyntax identifies text outside the exact civil-time grammar.
	ErrCivilSyntax = errors.New("invalid civil-time syntax")
	// ErrCivilGap identifies a civil time with no matching instant.
	ErrCivilGap = errors.New("nonexistent civil time")
	// ErrCivilFold identifies a rejected ambiguous civil time.
	ErrCivilFold = errors.New("ambiguous civil time")
	// ErrCivilRange identifies a selected instant outside Timestamp nanoseconds.
	ErrCivilRange = errors.New("civil instant outside timestamp range")
)

// CivilTimeResolver is an immutable handle to captured raw rules and explicit policies.
// Its zero value is invalid. Copies share identity; construction is plan setup,
// outside Workspace accounting. Existing standard-loader handles are unsupported.
type CivilTimeResolver struct{ state *civilTimeResolverState }
type civilTimeResolverState struct {
	zone    *timeZoneState
	options CivilResolveOptions
	offsets []int32
}

const civilOffsetSlots = 514

// NewCivilTimeResolver builds a complete offset catalogue from the exact raw
// TimeZoneFromTZData capture. It never reloads a zone name. LoadTimeZone handles
// (including UTC) are rejected. Scratch and exact index together contain at most
// 4112 bytes of offsets, apart from descriptors and retained rule storage.
func NewCivilTimeResolver(ctx context.Context, zone TimeZone, options CivilResolveOptions) (CivilTimeResolver, error) {
	if ctx == nil {
		return CivilTimeResolver{}, ErrCivilConfiguration
	}
	if err := ctx.Err(); err != nil {
		return CivilTimeResolver{}, err
	}
	if zone.state == nil || zone.state.location == nil || zone.state.source != "TZif" || options.Fold < FoldReject || options.Fold > FoldLater || options.Gap != GapReject {
		return CivilTimeResolver{}, ErrCivilConfiguration
	}
	var scratch [civilOffsetSlots]int32
	n, err := civilCatalogue(ctx, zone.state.data, &scratch)
	if err != nil {
		return CivilTimeResolver{}, err
	}
	// Bounded insertion sort, checking even during shifts; no sorting heap scratch.
	for i := 1; i < n; i++ {
		x := scratch[i]
		j := i
		for j > 0 && scratch[j-1] > x {
			if err := ctx.Err(); err != nil {
				return CivilTimeResolver{}, err
			}
			scratch[j] = scratch[j-1]
			j--
		}
		scratch[j] = x
	}
	unique := 0
	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return CivilTimeResolver{}, err
		}
		if unique == 0 || scratch[i] != scratch[unique-1] {
			scratch[unique] = scratch[i]
			unique++
		}
	}
	offsets := make([]int32, unique)
	copy(offsets, scratch[:unique])
	if err := ctx.Err(); err != nil {
		return CivilTimeResolver{}, err
	}
	return CivilTimeResolver{&civilTimeResolverState{zone.state, options, offsets}}, nil
}

// Parse resolves exact YYYY-MM-DDTHH:MM:SS[.1..9 digits] text to UTC nanoseconds.
// It selects among all matching instants before checking Timestamp range.
// Context errors and configuration errors are not row-value errors.
func (r CivilTimeResolver) Parse(ctx context.Context, text string) (int64, error) {
	if ctx == nil {
		return 0, ErrCivilConfiguration
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.state == nil {
		return 0, ErrCivilConfiguration
	}
	c, err := parseCivilText(text)
	if err != nil {
		return 0, err
	}
	return r.resolveFields(ctx, c)
}

// resolveFields receives validated civil fields. Parse retains its original
// configuration, context and syntax check order before entering this matcher.
func (r CivilTimeResolver) resolveFields(ctx context.Context, c civilFields) (int64, error) {
	wall := time.Date(c.year, time.Month(c.month), c.day, c.hour, c.minute, c.second, 0, time.UTC).Unix()
	var chosen int64
	count := 0
	for _, offset := range r.state.offsets {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		sec := wall - int64(offset)
		t := time.Unix(sec, int64(c.nano)).In(r.state.zone.location)
		y, m, d := t.Date()
		h, mi, s := t.Clock()
		if y != c.year || int(m) != c.month || d != c.day || h != c.hour || mi != c.minute || s != c.second || t.Nanosecond() != c.nano {
			continue
		}
		if count == 0 || r.state.options.Fold == FoldEarlier && sec < chosen || r.state.options.Fold == FoldLater && sec > chosen {
			chosen = sec
		}
		count++
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, ErrCivilGap
	}
	if count > 1 && r.state.options.Fold == FoldReject {
		return 0, ErrCivilFold
	}
	min, max := time.Unix(0, math.MinInt64), time.Unix(0, math.MaxInt64)
	if chosen < min.Unix() || chosen == min.Unix() && c.nano < min.Nanosecond() || chosen > max.Unix() || chosen == max.Unix() && c.nano > max.Nanosecond() {
		return 0, ErrCivilRange
	}
	return time.Unix(chosen, int64(c.nano)).UnixNano(), nil
}

func civilCatalogue(ctx context.Context, data []byte, out *[civilOffsetSlots]int32) (int, error) {
	// Data is immutable and was fully validated at capture. Recheck block spans
	// contextually before indexing; no repeated copying or legacy parser change.
	if len(data) > maxTimeZoneDataBytes {
		return 0, ErrCivilConfiguration
	}
	n := 0
	block := func(at, width int) (int, byte, error) {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if at < 0 || at > len(data) || len(data)-at < 44 || string(data[at:at+4]) != "TZif" {
			return 0, 0, ErrCivilConfiguration
		}
		v := data[at+4]
		if v != 0 && v != '2' && v != '3' {
			return 0, 0, ErrCivilConfiguration
		}
		var c [6]uint64
		for i := range c {
			c[i] = uint64(binary.BigEndian.Uint32(data[at+20+i*4 : at+24+i*4]))
		}
		size := c[0] + c[1] + c[2]*uint64(width+4) + c[3]*uint64(width+1) + c[4]*6 + c[5]
		if c[2] != 0 || c[4] < 1 || c[4] > 256 || size > uint64(len(data)-at-44) {
			return 0, 0, ErrCivilConfiguration
		}
		types := at + 44 + int(c[3]*uint64(width+1))
		for i := 0; i < int(c[4]); i++ {
			if err := ctx.Err(); err != nil {
				return 0, 0, err
			}
			out[n] = int32(binary.BigEndian.Uint32(data[types+6*i : types+6*i+4]))
			n++
		}
		return at + 44 + int(size), v, nil
	}
	end, v, err := block(0, 4)
	if err != nil {
		return 0, err
	}
	if v != 0 {
		var v2 byte
		end, v2, err = block(end, 8)
		if err != nil {
			return 0, err
		}
		if v2 != v {
			return 0, ErrCivilConfiguration
		}
	}
	tail := data[end:]
	if len(tail) > 1024 {
		return 0, ErrCivilConfiguration
	}
	if len(tail) > 2 && tail[0] == '\n' && tail[len(tail)-1] == '\n' {
		a, b, two, err := civilFooterOffsets(ctx, string(tail[1:len(tail)-1]))
		if err != nil {
			return 0, err
		}
		out[n] = a
		n++
		if two {
			out[n] = b
			n++
		}
	}
	return n, nil
}

func civilFooterOffsets(ctx context.Context, s string) (int32, int32, bool, error) {
	fail := func() (int32, int32, bool, error) {
		return 0, 0, false, fmt.Errorf("%w: unsupported TZif footer prefix", ErrCivilConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, false, err
	}
	rest, ok := civilZoneName(s)
	if !ok {
		return fail()
	}
	x, rest, ok := civilZoneOffset(rest)
	if !ok {
		return fail()
	}
	std := int32(-x)
	if len(rest) == 0 || rest[0] == ',' {
		return std, 0, false, nil
	}
	rest, ok = civilZoneName(rest)
	if !ok {
		return fail()
	}
	if len(rest) == 0 || rest[0] == ',' {
		return std, std + 3600, true, nil
	}
	x, rest, ok = civilZoneOffset(rest)
	if !ok {
		return fail()
	}
	// Rules can fail in Go, falling back to ttinfo. Both possible offsets remain
	// a safe superset even when the suffix is invalid; do not interpret rules.
	_ = rest
	if err := ctx.Err(); err != nil {
		return 0, 0, false, err
	}
	return std, int32(-x), true, nil
}
func civilZoneName(s string) (string, bool) {
	if len(s) == 0 {
		return "", false
	}
	if s[0] == '<' {
		for i := 1; i < len(s); i++ {
			if s[i] == '>' {
				return s[i+1:], true
			}
		}
		return "", false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b >= '0' && b <= '9' || b == ',' || b == '-' || b == '+' {
			if i < 3 {
				return "", false
			}
			return s[i:], true
		}
	}
	return "", len(s) >= 3
}
func civilZoneOffset(s string) (int, string, bool) {
	neg := false
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		neg = s[0] == '-'
		s = s[1:]
	}
	num := func(s string, max int) (int, string, bool) {
		n, i := 0, 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			d := int(s[i] - '0')
			if n > (max-d)/10 {
				return 0, "", false
			}
			n = n*10 + d
			if n > max {
				return 0, "", false
			}
			i++
		}
		return n, s[i:], i > 0
	}
	h, r, ok := num(s, 168)
	if !ok {
		return 0, "", false
	}
	value := h * 3600
	if len(r) > 0 && r[0] == ':' {
		var m int
		m, r, ok = num(r[1:], 59)
		if !ok {
			return 0, "", false
		}
		value += m * 60
		if len(r) > 0 && r[0] == ':' {
			var sec int
			sec, r, ok = num(r[1:], 59)
			if !ok {
				return 0, "", false
			}
			value += sec
		}
	}
	if neg {
		value = -value
	}
	return value, r, true
}
