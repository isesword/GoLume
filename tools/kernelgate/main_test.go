package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"no subcommand", nil, 2},
		{"unknown subcommand", []string{"frobnicate"}, 2},
		{"gate without kernels", []string{"gate"}, 1},
	}
	for _, tc := range cases {
		var stdout, stderr bytes.Buffer
		if got := run(tc.args, &stdout, &stderr); got != tc.want {
			t.Errorf("%s: exit %d, want %d", tc.name, got, tc.want)
		}
	}
	var stderr bytes.Buffer
	run(nil, &bytes.Buffer{}, &stderr)
	if !strings.Contains(stderr.String(), "usage") {
		t.Errorf("usage message missing: %q", stderr.String())
	}
}
