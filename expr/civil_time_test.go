package expr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Independent minimal TZif writer. Each transition names a following ttinfo;
// no resolver helpers or local time.Date choices are used to build expectations.
func civilTestBlock(version byte, width int, offsets []int32, transitions []int64, indices []byte) []byte {
	b := make([]byte, 44)
	copy(b, "TZif")
	b[4] = version
	binary.BigEndian.PutUint32(b[32:], uint32(len(transitions)))
	binary.BigEndian.PutUint32(b[36:], uint32(len(offsets)))
	binary.BigEndian.PutUint32(b[40:], 4)
	for _, s := range transitions {
		if width == 4 {
			b = binary.BigEndian.AppendUint32(b, uint32(s))
		} else {
			b = binary.BigEndian.AppendUint64(b, uint64(s))
		}
	}
	b = append(b, indices...)
	for _, o := range offsets {
		b = binary.BigEndian.AppendUint32(b, uint32(o))
		b = append(b, 0, 0)
	}
	return append(b, 'X', 'X', 'X', 0)
}
func civilTestData(version byte, a, b []int32, transitions []int64, indices []byte, footer string) []byte {
	out := civilTestBlock(version, 4, a, transitions, indices)
	if version != 0 {
		out = append(out, civilTestBlock(version, 8, b, transitions, indices)...)
	}
	if footer != "" {
		out = append(out, []byte("\n"+footer+"\n")...)
	}
	return out
}
func civilTestZone(t *testing.T, b []byte) TimeZone {
	t.Helper()
	z, e := TimeZoneFromTZData("same-label", b)
	if e != nil {
		t.Fatal(e)
	}
	return z
}
func civilTestResolver(t *testing.T, z TimeZone, p CivilFoldPolicy) CivilTimeResolver {
	t.Helper()
	r, e := NewCivilTimeResolver(context.Background(), z, CivilResolveOptions{p, GapReject})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func civilTestUTC(t *testing.T, s string) int64 {
	t.Helper()
	v, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
		t.Fatal(e)
	}
	return v.UnixNano()
}
func TestCivilTimeRealRules(t *testing.T) {
	cases := []struct {
		zone, text, early, late string
		gap                     bool
	}{
		{"new_york", "2024-11-03T01:30:00", "2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z", false},
		{"new_york", "2024-03-10T02:30:00", "", "", true},
		{"lord_howe", "2024-04-07T01:45:00", "2024-04-06T14:45:00Z", "2024-04-06T15:15:00Z", false},
		{"lord_howe", "2024-10-06T02:15:00", "", "", true},
		{"apia", "2011-12-30T12:00:00", "", "", true},
		{"apia", "2011-12-31T00:00:00", "2011-12-30T10:00:00Z", "", false},
		{"paris", "1900-01-01T00:09:21", "1900-01-01T00:00:00Z", "", false},
		{"new_york", "1969-12-31T18:59:59.999999999", "1969-12-31T23:59:59.999999999Z", "", false},
	}
	for _, c := range cases {
		t.Run(c.zone+"/"+c.text, func(t *testing.T) {
			for _, p := range []CivilFoldPolicy{FoldReject, FoldEarlier, FoldLater} {
				r := civilTestResolver(t, timezoneFixture(t, c.zone), p)
				got, e := r.Parse(context.Background(), c.text)
				if c.gap {
					if !errors.Is(e, ErrCivilGap) {
						t.Fatalf("gap: %v", e)
					}
					continue
				}
				if c.late != "" && p == FoldReject {
					if !errors.Is(e, ErrCivilFold) {
						t.Fatalf("fold: %v", e)
					}
					continue
				}
				want := c.early
				if c.late != "" && p == FoldLater {
					want = c.late
				}
				if e != nil || got != civilTestUTC(t, want) {
					t.Fatalf("policy %d got %d %v want %s", p, got, e, want)
				}
			}
		})
	}
}
func TestCivilTimeMultipleCandidatesAndExtremes(t *testing.T) {
	z := civilTestZone(t, civilTestData(0, []int32{7200, 3600, 0}, nil, []int64{-5400, -1800}, []byte{1, 2}, ""))
	for _, s := range []int64{-7200, -3600, 0} {
		v := time.Unix(s, 0).In(z.state.location)
		if v.Format("2006-01-02T15:04:05") != "1970-01-01T00:00:00" {
			t.Fatal("fixture candidate missing", s, v)
		}
	}
	for _, p := range []CivilFoldPolicy{FoldReject, FoldEarlier, FoldLater} {
		r := civilTestResolver(t, z, p)
		if !reflect.DeepEqual(r.state.offsets, []int32{0, 3600, 7200}) {
			t.Fatal(r.state.offsets)
		}
		v, e := r.Parse(context.Background(), "1970-01-01T00:00:00")
		if p == FoldReject {
			if !errors.Is(e, ErrCivilFold) {
				t.Fatal(e)
			}
		} else {
			want := int64(0)
			if p == FoldEarlier {
				want = -7200 * 1e9
			}
			if e != nil || v != want {
				t.Fatal(v, e, want)
			}
		}
	}
	for _, o := range []int32{math.MinInt32, math.MaxInt32} {
		r := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{o}, nil, nil, nil, "")), FoldReject)
		v, e := r.Parse(context.Background(), "1970-01-01T00:00:00")
		if e != nil || v != -int64(o)*1e9 {
			t.Fatal(o, v, e)
		}
	}
	// Two candidates straddle the maximum Timestamp. Fold must precede range.
	sec := int64(9223372036)
	z = civilTestZone(t, civilTestData('2', []int32{1, 0}, []int32{1, 0}, []int64{sec}, []byte{1}, ""))
	for _, p := range []CivilFoldPolicy{FoldReject, FoldEarlier, FoldLater} {
		v, e := civilTestResolver(t, z, p).Parse(context.Background(), "2262-04-11T23:47:16.999999999")
		switch p {
		case FoldReject:
			if !errors.Is(e, ErrCivilFold) {
				t.Fatal(e)
			}
		case FoldLater:
			if !errors.Is(e, ErrCivilRange) {
				t.Fatal(e)
			}
		default:
			if e != nil || v != 9223372035999999999 {
				t.Fatal(v, e)
			}
		}
	}
}
func TestCivilTimeGrammarAndRange(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, "")), FoldReject)
	good := map[string]int64{"1677-09-21T00:12:43.145224192": math.MinInt64, "2262-04-11T23:47:16.854775807": math.MaxInt64, "1969-12-31T23:59:59.999999999": -1, "1970-01-01T00:00:00": 0, "2000-02-29T00:00:00": 951782400000000000}
	for s, w := range good {
		v, e := r.Parse(context.Background(), s)
		if e != nil || v != w {
			t.Fatal(s, v, e, w)
		}
	}
	for n := 1; n <= 9; n++ {
		s := "1970-01-01T00:00:00." + strings.Repeat("1", n)
		v, e := r.Parse(context.Background(), s)
		w := int64(0)
		for i := 0; i < 9; i++ {
			w *= 10
			if i < n {
				w++
			}
		}
		if e != nil || v != w {
			t.Fatal(s, v, e)
		}
	}
	for _, s := range []string{"0000-02-29T00:00:00", "9999-12-31T23:59:59", "1677-09-21T00:12:43.145224191", "2262-04-11T23:47:16.854775808"} {
		_, e := r.Parse(context.Background(), s)
		if !errors.Is(e, ErrCivilRange) {
			t.Fatal(s, e)
		}
	}
	bad := []string{"", "2001-02-29T00:00:00", "1900-02-29T00:00:00", "2100-02-29T00:00:00", "2000-04-31T00:00:00", "2000-00-01T00:00:00", "2000-13-01T00:00:00", "2000-01-00T00:00:00", "2000-01-01T24:00:00", "2000-01-01T00:60:00", "2000-01-01T00:00:60", "1970-01-01 00:00:00", "1970-01-01T00:00:00Z", "1970-01-01T00:00:00+00:00", "1970-01-01T00:00:00.", "1970-01-01T00:00:00.1234567890", "1970-01-01T00:00:00,1", "１９７０-01-01T00:00:00", strings.Repeat("x", 65536)}
	for i, s := range bad {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, e := r.Parse(context.Background(), s)
			if !errors.Is(e, ErrCivilSyntax) {
				t.Fatal(e)
			}
			if len(e.Error()) > 256 {
				t.Fatal("unbounded error")
			}
		})
	}
}

type civilTestContext struct {
	context.Context
	calls, at int
}

func (c *civilTestContext) Err() error {
	c.calls++
	if c.at > 0 && c.calls >= c.at {
		return context.Canceled
	}
	return nil
}
func TestCivilTimeCatalogueBoundsAndCancellation(t *testing.T) {
	a, b := make([]int32, 256), make([]int32, 256)
	for i := range a {
		a[i] = int32(511 - i)
		b[i] = int32(255 - i)
	}
	z := civilTestZone(t, civilTestData('3', a, b, nil, nil, "AAA-1BBB-2,M3.2.0,M11.1.0"))
	opts := CivilResolveOptions{FoldEarlier, GapReject}
	trace := &civilTestContext{Context: context.Background()}
	r, e := NewCivilTimeResolver(trace, z, opts)
	if e != nil {
		t.Fatal(e)
	}
	if len(r.state.offsets) != 514 || r.state.offsets[0] != 0 || r.state.offsets[511] != 511 || r.state.offsets[512] != 3600 || r.state.offsets[513] != 7200 {
		t.Fatal("catalogue incomplete", r.state.offsets)
	}
	// Check each bounded phase without O(n^2) independent constructor repetitions.
	for _, at := range []int{1, 3, 259, 520, 1000, trace.calls - 1, trace.calls} {
		c := &civilTestContext{Context: context.Background(), at: at}
		v, e := NewCivilTimeResolver(c, z, opts)
		if e != context.Canceled || v.state != nil {
			t.Fatal(at, c.calls, e)
		}
	}
	pc := &civilTestContext{Context: context.Background()}
	_, _ = r.Parse(pc, "1970-01-01T00:00:00")
	for _, at := range []int{1, 2, pc.calls - 1, pc.calls} {
		c := &civilTestContext{Context: context.Background(), at: at}
		_, e := r.Parse(c, "1970-01-01T00:00:00")
		if e != context.Canceled {
			t.Fatal(at, e)
		}
	}
	t.Logf("constructor checks=%d; Parse checks=%d; 514 distinct index offsets", trace.calls, pc.calls)
	// Hundreds of transitions don't change the complete offset catalogue.
	ts := make([]int64, 4096)
	ids := make([]byte, len(ts))
	for i := range ts {
		ts[i] = int64(i) - 2048
		ids[i] = byte(i % 2)
	}
	z = civilTestZone(t, civilTestData('2', []int32{0, 1}, []int32{0, 1}, ts, ids, ""))
	r = civilTestResolver(t, z, FoldEarlier)
	if !reflect.DeepEqual(r.state.offsets, []int32{0, 1}) {
		t.Fatal(r.state.offsets)
	}
}
func TestCivilTimeFooterCatalogue(t *testing.T) {
	cases := []struct {
		s    string
		want []int32
	}{
		{"AAA0", []int32{0, 123}}, {"AAA5BBB", []int32{-18000, -14400, 123}},
		{"<+00>-0<+01>,M3.2.0,M11.1.0", []int32{0, 123, 3600}},
		{"AAA-168:59:59BBB", []int32{123, 608399, 611999}},
		{"AAA168:59:59BBB167:58:57,M3.2.0,M11.1.0", []int32{-608399, -604737, 123}},
		{"AAA0BBB-1;M3.2.0,M11.1.0", []int32{0, 123, 3600}},
		{"AAA0BBB-1,invalid", []int32{0, 123, 3600}},
		{"<>0<>-1,M3.2.0,M11.1.0", []int32{0, 123, 3600}},
	}
	for _, v := range []byte{0, '2', '3'} {
		for _, c := range cases {
			t.Run(fmt.Sprintf("%d/%s", v, c.s), func(t *testing.T) {
				z := civilTestZone(t, civilTestData(v, []int32{123}, []int32{123}, nil, nil, c.s))
				r := civilTestResolver(t, z, FoldEarlier)
				if !reflect.DeepEqual(r.state.offsets, c.want) {
					t.Fatal(r.state.offsets, c.want)
				}
			})
		}
	}
	for _, s := range []string{"AA0", "AAA169", "AAA0BBB169", "AAA0:60", "AAA0:0:60", "<bad", "AAA", "AAA+"} {
		z := civilTestZone(t, civilTestData('2', []int32{0}, []int32{0}, nil, nil, s))
		_, e := NewCivilTimeResolver(context.Background(), z, CivilResolveOptions{FoldReject, GapReject})
		if !errors.Is(e, ErrCivilConfiguration) {
			t.Fatal(s, e)
		}
	}
}
func TestCivilTimeConfigurationAndCapturedIdentity(t *testing.T) {
	data := civilTestData(0, []int32{0}, nil, nil, nil, "")
	z := civilTestZone(t, data)
	r := civilTestResolver(t, z, FoldReject)
	for i := range data {
		data[i] = 0
	}
	got, e := r.Parse(context.Background(), "1970-01-01T00:00:00")
	if e != nil || got != 0 {
		t.Fatal(got, e)
	}
	other := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{3600}, nil, nil, nil, "")), FoldReject)
	got, e = other.Parse(context.Background(), "1970-01-01T00:00:00")
	if e != nil || got != -3600e9 || r.state == other.state {
		t.Fatal(got, e)
	}
	copy := r
	if copy.state != r.state {
		t.Fatal("copy identity")
	}
	r2 := civilTestResolver(t, z, FoldReject)
	if r2.state == r.state {
		t.Fatal("independent identity")
	}
	for _, opts := range []CivilResolveOptions{{}, {Fold: FoldReject}, {Gap: GapReject}, {Fold: 4, Gap: GapReject}, {Fold: FoldReject, Gap: 2}} {
		_, e := NewCivilTimeResolver(context.Background(), z, opts)
		if !errors.Is(e, ErrCivilConfiguration) {
			t.Fatal(opts, e)
		}
	}
	opts := CivilResolveOptions{FoldReject, GapReject}
	legacy, e := LoadTimeZone("UTC")
	if e != nil {
		t.Fatal(e)
	}
	for _, zone := range []TimeZone{{}, legacy} {
		_, e := NewCivilTimeResolver(context.Background(), zone, opts)
		if !errors.Is(e, ErrCivilConfiguration) {
			t.Fatal(e)
		}
	}
	if _, e := NewCivilTimeResolver(nil, z, opts); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal(e)
	}
	if _, e := (CivilTimeResolver{}).Parse(context.Background(), "1970-01-01T00:00:00"); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal(e)
	}
	if _, e := r.Parse(nil, "1970-01-01T00:00:00"); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal(e)
	}
}

func TestCivilTimeFooterActualResolution(t *testing.T) {
	// A real transition makes Go's extension active after epoch; literal UTC
	// expectations independently distinguish the extension from ttinfo offset 123.
	for _, v := range []byte{0, '2', '3'} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			z := civilTestZone(t, civilTestData(v, []int32{123}, []int32{123}, []int64{0}, []byte{0}, "EST5EDT,M3.2.0/2,M11.1.0/2"))
			for _, c := range []struct{ text, utc string }{{"2024-01-01T12:00:00", "2024-01-01T17:00:00Z"}, {"2024-07-01T12:00:00", "2024-07-01T16:00:00Z"}} {
				got, e := civilTestResolver(t, z, FoldReject).Parse(context.Background(), c.text)
				if e != nil || got != civilTestUTC(t, c.utc) {
					t.Fatal(c, got, e)
				}
			}
			z = civilTestZone(t, civilTestData(v, []int32{123}, []int32{123}, []int64{0}, []byte{0}, "AAA0BBB-1,invalid"))
			got, e := civilTestResolver(t, z, FoldReject).Parse(context.Background(), "1970-01-02T00:02:03")
			if e != nil || got != 86400e9 {
				t.Fatal("invalid suffix must retain actual fallback", got, e)
			}
		})
	}
}
