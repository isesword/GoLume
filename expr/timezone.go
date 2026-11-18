package expr

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const maxTimeZoneDataBytes = 1 << 20

// TimeZone is a copyable, immutable handle to captured time-zone rules. Its
// zero value is invalid. Copies share rule identity; separate successful
// constructors have independent identities even for the same name and rules.
// Rule construction and plan-owned memory are outside Workspace budgets.
type TimeZone struct{ state *timeZoneState }

type timeZoneState struct {
	name     string
	location *time.Location
	source   string
	digest   [32]byte
	data     []byte
}

// Name returns the supplied zone label, or an empty string for an invalid handle.
func (zone TimeZone) Name() string {
	if zone.state == nil {
		return ""
	}
	return zone.state.name
}

func validateTimeZoneName(name string) error {
	if name == "" || len(name) > 255 || !utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 || name == "Local" {
		return fmt.Errorf("time zone name must be nonempty valid UTF-8, at most 255 bytes, without NUL, and not Local")
	}
	return nil
}

// LoadTimeZone captures rules using the standard library's available zone
// sources once. Empty names and Local are rejected; explicit UTC is allowed.
// Results do not promise identical rules across machines or tzdb/Go versions,
// or a refresh of the standard library's search configuration. Use
// TimeZoneFromTZData to supply fixed rule bytes. No loading occurs during Bind
// or row evaluation. This path follows the standard library's resource and
// leap-second handling, not the explicit-data path's bounded preflight.
func LoadTimeZone(name string) (TimeZone, error) {
	if err := validateTimeZoneName(name); err != nil {
		return TimeZone{}, err
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return TimeZone{}, fmt.Errorf("load time zone %q: %w", name, err)
	}
	return TimeZone{state: &timeZoneState{name: name, location: location, source: "standard-library"}}, nil
}

// TimeZoneFromTZData copies and parses at most 1 MiB of TZif v1, v2, or v3
// rules. Each block requires 1..256 local-time types and every referenced
// abbreviation must terminate within 255 bytes plus NUL (at most 64 KiB of
// expanded abbreviations per block). The trailing footer in every supported version is limited to
// 1024 bytes including newline delimiters, bounding repeated parser work during
// lookup. These parser resource limits are not a
// query-memory bound. Leap-second records are rejected. The name is a label, not proof of
// geographic origin or an IANA release version; even UTC labels use the supplied
// rules. Parsing is synchronous, precedes query execution, and has no query
// budget or cancellation contract. The handle exposes no mutable rule storage.
func TimeZoneFromTZData(name string, data []byte) (TimeZone, error) {
	if err := validateTimeZoneName(name); err != nil {
		return TimeZone{}, err
	}
	if len(data) > maxTimeZoneDataBytes {
		return TimeZone{}, fmt.Errorf("time zone TZif data exceeds 1 MiB limit")
	}
	copied := append([]byte(nil), data...)
	if err := validateTimeZoneData(copied); err != nil {
		return TimeZone{}, err
	}
	location, err := time.LoadLocationFromTZData(name, copied)
	if err != nil {
		return TimeZone{}, fmt.Errorf("parse time zone %q: %w", name, err)
	}
	return TimeZone{state: &timeZoneState{name: name, location: location, source: "TZif", digest: sha256.Sum256(copied), data: copied}}, nil
}

// validateTimeZoneData bounds both TZif sections before the standard parser
// allocates rule tables. It is not a second calendar engine: transition and
// footer interpretation remain the standard library's responsibility.
func validateTimeZoneData(data []byte) error {
	block := func(offset int, width uint64) (int, byte, error) {
		if offset < 0 || offset > len(data) || len(data)-offset < 44 || string(data[offset:offset+4]) != "TZif" {
			return 0, 0, fmt.Errorf("invalid or truncated TZif header")
		}
		version := data[offset+4]
		if version != 0 && version != '2' && version != '3' {
			return 0, 0, fmt.Errorf("unsupported TZif version %d", version)
		}
		var counts [6]uint64
		for i := range counts {
			counts[i] = uint64(binary.BigEndian.Uint32(data[offset+20+i*4 : offset+24+i*4]))
		}
		if counts[2] != 0 {
			return 0, 0, fmt.Errorf("TZif leap-second records are unsupported")
		}
		// uint32 counts times at most 12 cannot overflow uint64. Compare before
		// narrowing to int, including on 32-bit platforms.
		size := counts[0] + counts[1] + counts[2]*(width+4) + counts[3]*(width+1) + counts[4]*6 + counts[5]
		if size > uint64(len(data)-offset-44) {
			return 0, 0, fmt.Errorf("truncated or oversized TZif section")
		}
		if counts[4] < 1 || counts[4] > 256 {
			return 0, 0, fmt.Errorf("TZif local-time type count must be 1..256")
		}
		// The complete section span was checked above. All following offsets
		// lie inside that span, so narrowing them to int is safe. The standard
		// parser copies a referenced abbreviation for every local-time type;
		// limiting only input bytes would not bound that expansion.
		typesAt := offset + 44 + int(counts[3]*(width+1))
		abbreviationsAt := typesAt + int(counts[4]*6)
		abbreviations := data[abbreviationsAt : abbreviationsAt+int(counts[5])]
		expanded := 0
		for i := 0; i < int(counts[4]); i++ {
			start := int(data[typesAt+i*6+5])
			if start >= len(abbreviations) {
				return 0, 0, fmt.Errorf("TZif abbreviation index is out of bounds")
			}
			length := -1
			for j := 0; j < min(256, len(abbreviations)-start); j++ {
				if abbreviations[start+j] == 0 {
					length = j
					break
				}
			}
			if length < 0 {
				return 0, 0, fmt.Errorf("TZif referenced abbreviation must terminate within 255 bytes plus NUL")
			}
			expanded += length + 1
			if expanded > 64<<10 {
				return 0, 0, fmt.Errorf("TZif expanded abbreviations exceed 64 KiB")
			}
		}
		return offset + 44 + int(size), version, nil
	}
	next, version, err := block(0, 4)
	if err != nil {
		return err
	}
	end := next
	if version != 0 {
		var second byte
		end, second, err = block(next, 8)
		if err != nil {
			return err
		}
		if second != version {
			return fmt.Errorf("TZif headers have inconsistent versions")
		}
	}
	// Go accepts an extension after the final block even for v1 and may
	// reparse it on each lookup beyond the last transition. Bound that work
	// without inventing a second TZ parser.
	if len(data)-end > 1024 {
		return fmt.Errorf("TZif footer exceeds 1024-byte limit including delimiters")
	}
	return nil
}
