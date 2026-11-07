package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFixtureRowsMatchReferenceContract(t *testing.T) {
	groups := make(map[int32]aggregate)
	var selected int
	var sum int64
	var atThreshold, aboveThreshold bool
	for row := 0; row < fixtureRows; row++ {
		pred, key, value := fixtureRow(row)
		atThreshold = atThreshold || pred == 32767
		aboveThreshold = aboveThreshold || pred == 32768
		if pred > 32767 {
			selected++
			sum += value
			a := groups[key]
			a.count++
			a.sum += value
			groups[key] = a
		} else if key < groupCount || value >= 0 {
			t.Fatal("rejected rows must have distinct keys and negative values")
		}
	}
	if selected != selectedRows || len(groups) != groupCount || sum != checksum || !atThreshold || !aboveThreshold {
		t.Fatalf("fixture contract: selected=%d groups=%d sum=%d boundaries=%t/%t", selected, len(groups), sum, atThreshold, aboveThreshold)
	}
}

func TestWriteFixtureDoesNotOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.parquet")
	const marker = "original content"
	if err := os.WriteFile(path, []byte(marker), 0644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "linked.parquet")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{path, link} {
		if _, err := writeFixture(target); !errors.Is(err, os.ErrExist) {
			t.Fatalf("writeFixture(%s) = %v, want exists", target, err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != marker {
		t.Fatalf("original changed: %q, %v", data, err)
	}
}
