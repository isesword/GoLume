#!/usr/bin/env python3
"""Record numerical robustness limits in pinned Polars, separate from targets."""
import argparse
import json
import math
import sys
from pathlib import Path

import polars as pl

VERSION = "1.35.2"
if pl.__version__ != VERSION:
    raise SystemExit(f"expected Polars {VERSION}, got {pl.__version__}")


def encode(value):
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else ("+Inf" if value > 0 else "-Inf")
    return value


cases = []
for name, dtype, values in (
    ("finite_variance_overflowing_m2", pl.Float64, [-1e154, 1e154]),
    ("finite_std_overflowing_variance", pl.Float64,
     [-sys.float_info.max, sys.float_info.max]),
    ("int64_unit_differences", pl.Int64, [(1 << 63) - 3, (1 << 63) - 2, (1 << 63) - 1]),
    ("uint64_unit_differences", pl.UInt64, [(1 << 64) - 3, (1 << 64) - 2, (1 << 64) - 1]),
    ("singleton_nan", pl.Float64, [float("nan")]),
    ("singleton_inf", pl.Float64, [float("inf")]),
    ("repeated_inf", pl.Float64, [float("inf"), float("inf")]),
):
    series = pl.Series(values, dtype=dtype)
    result = {}
    for ddof in (0, 1, 2):
        result[f"var_ddof{ddof}"] = encode(series.var(ddof=ddof))
        result[f"std_ddof{ddof}"] = encode(series.std(ddof=ddof))
    cases.append({"id": name, "dtype": str(dtype),
                  "input": [encode(v) for v in values], "results": result})

content = json.dumps({"version": VERSION, "cases": cases}, indent=2, allow_nan=False) + "\n"
destination = Path(__file__).resolve().parents[1] / "testdata/fundamentals/statistics-polars-1.35.2.json"
parser = argparse.ArgumentParser()
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
if args.check:
    if destination.read_text() != content:
        raise SystemExit("statistics oracle differs; regenerate and review")
    print(f"Verified {len(cases)} statistics observations against Polars {VERSION}")
else:
    destination.write_text(content)
