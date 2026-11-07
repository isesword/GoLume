"""Regenerate the small pinned exact-distinct semantic reference."""
import json
from pathlib import Path
import polars as pl
assert pl.__version__ == "1.35.2", pl.__version__
cases = [
    ("empty", [], pl.Float64),
    ("all_null", [None,None], pl.Float64),
    ("signed_zero", [0.0,-0.0], pl.Float64),
    ("nan", [float('nan'),float('nan')], pl.Float64),
    ("mixed", [None,float('nan'),0.0,-0.0,1.0], pl.Float64),
    ("uint64_extrema", [2**64-1,2**63,2**64-1,None], pl.UInt64),
    ("binary", [b'',None,b'\x00',b'\x00',b'\xff'], pl.Binary),
]
out = {"polars_version": pl.__version__, "golume_output_dtype": "UInt64", "cases": []}
for name, values, dtype in cases:
    frame=pl.DataFrame({"v":pl.Series(values,dtype=dtype)}).select(pl.col("v").n_unique())
    out["cases"].append({"name":name,"count":frame.item(),"polars_dtype":str(frame.schema['v'])})
Path('testdata/fundamentals/distinct-polars-1.35.2.json').write_text(json.dumps(out,indent=2)+'\n')
