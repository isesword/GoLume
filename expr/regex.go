package expr

import (
	"fmt"
	"regexp"
)

// maxRegexPatternBytes bounds compilation work and retained program state.
// Go's regexp package uses RE2 syntax and guarantees linear-time matching.
const maxRegexPatternBytes = 64 << 10

func compileRegex(pattern string) (*regexp.Regexp, error) {
	if len(pattern) > maxRegexPatternBytes {
		return nil, fmt.Errorf("regular expression pattern is %d bytes; maximum is %d", len(pattern), maxRegexPatternBytes)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regular expression %q: %w", pattern, err)
	}
	return re, nil
}

func compileRegexExtract(pattern string, group int64) (*regexp.Regexp, int, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, 0, err
	}
	if group < 0 || group > int64(re.NumSubexp()) {
		return nil, 0, fmt.Errorf("regular expression capture group %d is outside [0, %d]", group, re.NumSubexp())
	}
	return re, int(group), nil
}
