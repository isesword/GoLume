// Command kernel-info reports exact per-operation dispatch decisions.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type capabilityReport struct {
	ID               string `json:"id"`
	SelectedBackend  string `json:"selected_backend"`
	AvailableBackend string `json:"available_backend"`
	AutoAdmitted     bool   `json:"auto_admitted"`
	VectorBits       int    `json:"vector_bits"`
	HardwareSIMD     bool   `json:"hardware_simd"`
	Emulated         bool   `json:"emulated"`
}

type report struct {
	Arch              string             `json:"arch"`
	Mode              string             `json:"mode"`
	Kernels           []capabilityReport `json:"kernels"`
	ColumnComparisons []capabilityReport `json:"column_comparisons"`
}

// Report physical numeric types once; temporal aliases use their storage type.
var columnComparisonTypes = [...]memory.DataType{
	memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
	memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
	memory.TypeFloat32, memory.TypeFloat64,
}

func reportCapability(id string, selected, automatic, available kernel.KernelCapability) capabilityReport {
	return capabilityReport{
		ID:               id,
		SelectedBackend:  selected.Backend,
		AvailableBackend: available.Backend,
		AutoAdmitted:     automatic.Backend != "scalar",
		VectorBits:       selected.VectorBits,
		HardwareSIMD:     selected.HardwareSIMD,
		Emulated:         selected.Emulated,
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("kernel-info", flag.ContinueOnError)
	flags.SetOutput(stderr)
	modeName := flags.String("mode", "auto", "kernel dispatch mode: scalar, auto, or simd")
	jsonOutput := flags.Bool("json", false, "emit kernel and column-comparison capabilities as JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	mode, err := parseMode(*modeName)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	set := kernel.New(mode)
	autoSet := kernel.New(kernel.ModeAuto)
	forcedSet := kernel.New(kernel.ModeSIMD)
	entries := make([]capabilityReport, 0, len(kernel.KernelIDs()))
	for _, id := range kernel.KernelIDs() {
		capability, ok := set.Capability(id)
		if !ok {
			fmt.Fprintf(stderr, "missing capability for %s\n", id)
			return 1
		}
		autoCapability, autoOK := autoSet.Capability(id)
		forcedCapability, forcedOK := forcedSet.Capability(id)
		if !autoOK || !forcedOK {
			fmt.Fprintf(stderr, "missing comparison capability for %s\n", id)
			return 1
		}
		entries = append(entries, reportCapability(id.String(), capability, autoCapability, forcedCapability))
	}
	columns := make([]capabilityReport, 0, len(columnComparisonTypes)*5)
	for _, typ := range columnComparisonTypes {
		for op := kernel.CompareGT; op <= kernel.CompareEQ; op++ {
			// These report IDs deliberately differ from literal ComparisonIDs.
			id := fmt.Sprintf("compare-columns-%s-%s", typ, op)
			capability, ok := set.ColumnComparisonCapability(typ, op)
			autoCapability, autoOK := autoSet.ColumnComparisonCapability(typ, op)
			forcedCapability, forcedOK := forcedSet.ColumnComparisonCapability(typ, op)
			if !ok || !autoOK || !forcedOK {
				fmt.Fprintf(stderr, "missing column-comparison capability for %s\n", id)
				return 1
			}
			columns = append(columns, reportCapability(id, capability, autoCapability, forcedCapability))
		}
	}
	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(report{Arch: set.Arch(), Mode: set.Mode().String(), Kernels: entries, ColumnComparisons: columns}); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "arch=%s mode=%s\n", set.Arch(), set.Mode())
	for _, group := range [][]capabilityReport{entries, columns} {
		for _, entry := range group {
			fmt.Fprintf(stdout, "%-30s backend=%-28s vector=%3d hardware=%-5t emulated=%-5t available=%-28s auto=%t\n",
				entry.ID, entry.SelectedBackend, entry.VectorBits, entry.HardwareSIMD, entry.Emulated,
				entry.AvailableBackend, entry.AutoAdmitted)
		}
	}
	return 0
}

func parseMode(name string) (kernel.Mode, error) {
	switch name {
	case "scalar":
		return kernel.ModeScalar, nil
	case "auto":
		return kernel.ModeAuto, nil
	case "simd", "forced", "forced-simd":
		return kernel.ModeSIMD, nil
	default:
		return 0, fmt.Errorf("unknown mode %q", name)
	}
}
