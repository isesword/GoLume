package physical

import (
	"gocolumn/kernel"
	"gocolumn/source"
)

func testReadOptions() source.ReadOptions {
	return source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}
}
