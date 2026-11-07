#!/usr/bin/env python3
"""Pinned ordered fill observations; NaN is encoded separately from NULL."""
import argparse
import json
import math
from pathlib import Path
import polars as pl

VERSION = "1.35.2"
if pl.__version__ != VERSION:
    raise SystemExit(f"expected Polars {VERSION}, got {pl.__version__}")

def encode(v):
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    return v

cases = []
for dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
              pl.UInt32, pl.UInt64, pl.Float32, pl.Float64):
    for name, values in (("mixed", [None, 7, None, None, 9, None]),
                         ("empty", []), ("nulls", [None, None]),
                         ("nan", [None, float("nan"), None, 4, None]) if dtype in (pl.Float32, pl.Float64)
                         else ("boundary", [None, 127, None])):
        series = pl.Series("v", values, dtype=dtype)
        cases.append({"dtype": str(dtype), "name": name,
                      "input": list(map(encode, values)),
                      "forward": list(map(encode, series.forward_fill().to_list())),
                      "backward": list(map(encode, series.backward_fill().to_list()))})
content = json.dumps({"version": VERSION, "cases": cases}, indent=2, allow_nan=False) + "\n"
path = Path(__file__).resolve().parents[1] / "testdata/fundamentals/fills-polars-1.35.2.json"
parser = argparse.ArgumentParser()
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
if args.check:
    if path.read_text() != content:
        raise SystemExit("fill oracle differs; regenerate and review")
    print(f"Verified {len(cases)} fill cases against Polars {VERSION}")
else:
    path.write_text(content)
