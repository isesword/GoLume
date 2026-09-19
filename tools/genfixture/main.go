// Command genfixture writes a deterministic columnar fixture dataset as a
// Parquet file from a versioned JSON dataset specification. The same
// (-seed, spec) pair always produces byte-identical output.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"gocolumn/internal/fixturegen"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type summary struct {
	Out    string `json:"out"`
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("genfixture", flag.ContinueOnError)
	flags.SetOutput(stderr)
	seed := flags.Uint64("seed", 42, "deterministic base seed")
	rows := flags.Int64("rows", 0, "override the row count declared by the spec (0 keeps the spec value)")
	specPath := flags.String("spec", "", "dataset spec JSON path (required)")
	out := flags.String("out", "", "Parquet output path (required)")
	compression := flags.String("compression", string(fixturegen.CompressionZstd), "Parquet codec: zstd, snappy, gzip, or none")
	rowGroupRows := flags.Int64("row-group-rows", 0, "Parquet row group size in rows (0 selects the default)")
	overwrite := flags.Bool("overwrite", false, "replace an existing output file")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *specPath == "" || *out == "" {
		fmt.Fprintln(stderr, "genfixture: -spec and -out are required")
		return 2
	}
	spec, err := fixturegen.ParseSpecFile(*specPath)
	if err != nil {
		fmt.Fprintf(stderr, "genfixture: %v\n", err)
		return 2
	}
	if *rows > 0 {
		spec.Rows = *rows
		if err := spec.Validate(); err != nil {
			fmt.Fprintf(stderr, "genfixture: %v\n", err)
			return 2
		}
	}
	table, err := fixturegen.Generate(spec, *seed)
	if err != nil {
		fmt.Fprintf(stderr, "genfixture: %v\n", err)
		return 2
	}
	options := fixturegen.ParquetOptions{
		Compression:  fixturegen.ParquetCompression(*compression),
		RowGroupRows: *rowGroupRows,
	}
	written, err := fixturegen.WriteParquetFile(*out, table, options, *overwrite)
	if err != nil {
		fmt.Fprintf(stderr, "genfixture: %v\n", err)
		return 1
	}
	digest, err := fileSHA256(*out)
	if err != nil {
		fmt.Fprintf(stderr, "genfixture: %v\n", err)
		return 1
	}
	report := summary{Out: *out, Rows: spec.Rows, Bytes: written, SHA256: digest}
	if err := json.NewEncoder(stdout).Encode(report); err != nil {
		fmt.Fprintf(stderr, "genfixture: %v\n", err)
		return 1
	}
	return 0
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
