// Command kernel-info reports exact per-operation dispatch decisions.
package main

import (
	"flag"
	"fmt"
	"gocolumn/kernel"
	"os"
)

func main() {
	modeName := flag.String("mode", "auto", "kernel dispatch mode: scalar, auto, or simd")
	flag.Parse()
	mode, err := parseMode(*modeName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	set := kernel.New(mode)
	fmt.Printf("arch=%s mode=%s\n", set.Arch(), set.Mode())
	for _, id := range kernel.KernelIDs() {
		capability, ok := set.Capability(id)
		if !ok {
			fmt.Fprintf(os.Stderr, "missing capability for %s\n", id)
			os.Exit(1)
		}
		state := "scalar"
		if capability.Backend != "scalar" {
			state = "forced"
			if mode == kernel.ModeAuto {
				state = "auto-approved"
			}
		}
		fmt.Printf("%-30s backend=%-28s vector=%3d hardware=%-5t emulated=%-5t state=%s\n",
			id, capability.Backend, capability.VectorBits, capability.HardwareSIMD, capability.Emulated, state)
	}
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
