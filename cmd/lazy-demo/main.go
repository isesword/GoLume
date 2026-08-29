// Command lazy-demo demonstrates lazy planning, source pushdown, and prepared
// execution over the reference Parquet workload.
package main

import (
	"context"
	"flag"
	"fmt"
	"gocolumn/engine"
	"gocolumn/lazy"
	"log"
)

func main() {
	path := flag.String("parquet", "testdata/bench-1m.parquet", "input parquet file")
	workers := flag.Int("workers", 4, "maximum execution workers")
	repeat := flag.Int("repeat", 3, "number of prepared executions")
	analyze := flag.Bool("analyze", false, "execute and print operator-level ExplainAnalyze before prepared runs")
	flag.Parse()
	if *repeat < 1 {
		log.Fatal("repeat must be at least 1")
	}

	opts := engine.DefaultOptions()
	opts.Workers = *workers
	session := lazy.NewSession(opts)
	defer session.Close()

	frame, err := session.ScanParquet(*path)
	if err != nil {
		log.Fatal(err)
	}
	frame = frame.
		Filter(lazy.Col("pred").Gt(lazy.Lit(32767))).
		Select("key", "value").
		GroupBy("key").
		Agg(lazy.Sum("value", "sum"), lazy.Count("count"))

	fmt.Println("--- logical ---")
	fmt.Println(frame.Explain())
	optimized, err := frame.ExplainOptimized()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("--- optimized ---")
	fmt.Println(optimized)

	ctx := context.Background()
	if *analyze {
		text, err := frame.ExplainAnalyze(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("--- explain analyze ---")
		fmt.Println(text)
	}
	physical, err := frame.ExplainPhysical(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("--- physical ---")
	fmt.Println(physical)

	prepared, err := frame.PrepareResult(ctx, engine.PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		log.Fatal(err)
	}
	defer prepared.Close()

	for i := 1; i <= *repeat; i++ {
		result, err := prepared.Execute(ctx, engine.ExecuteOptions{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("run=%d groups=%d selected=%d checksum=%d strategy=%s workers=%d elapsed=%s\n",
			i, result.GroupCount, result.SelectedRows, result.Checksum,
			result.Strategy.Kind, result.Strategy.Workers, result.Elapsed)
	}
}
