package kernel

import "runtime"

func runtimeArch() string { return runtime.GOARCH }
