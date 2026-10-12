# /// script
# requires-python = ">=3.10"
# dependencies = ["pandas>=2.0"]
# ///
"""Validate data/drug_cases_<year>.csv against raw/ (independent re-parse of the raw files).

Run:  uv run etl/validate.py
"""
import csv, glob, math, os, sys
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "raw", "npa_drug_crime")
df = pd.concat([pd.read_csv(f, encoding="utf-8-sig", dtype={"roc_date_raw": str})
                for f in sorted(glob.glob(os.path.join(ROOT, "data", "drug_cases_*.csv")))], ignore_index=True)
fails = []

# 1. per-file rows & weight: raw == output + exact-duplicate rows removed (within file)
print("== 1. per source file: raw vs output + dropped duplicates ==")
by = df.groupby("source_file").agg(rows=("weight_g", "size"), w=("weight_g", lambda s: math.fsum(s)))
tot = dict(raw=0, out=0, dup=0)
for f in sorted(glob.glob(os.path.join(RAW, "*.csv"))):
    fn = os.path.basename(f)
    with open(f, encoding="utf-8-sig", newline="") as fh:
        rd = csv.reader(fh); next(rd); next(rd)
        rows = list(rd)
    raw_n, raw_w = len(rows), math.fsum(float(r[9]) for r in rows)
    uniq = {tuple(r): r for r in rows}
    dup_n = raw_n - len(uniq); dup_w = raw_w - math.fsum(float(r[9]) for r in uniq.values())
    out_n, out_w = int(by.loc[fn, "rows"]), by.loc[fn, "w"]
    ok = (raw_n == out_n + dup_n) and abs(raw_w - (out_w + dup_w)) < 1e-3 and out_n == len(uniq) and abs(out_w - (raw_w - dup_w)) < 1e-3
    tot["raw"] += raw_n; tot["out"] += out_n; tot["dup"] += dup_n
    print(f"{fn[16:27]} raw={raw_n:6d} dup_removed={dup_n:4d} out={out_n:6d} raw_w={raw_w:14.2f} out_w={out_w:14.2f} {'OK' if ok else 'MISMATCH'}")
    if not ok: fails.append(f"row/weight mismatch {fn}")
print(f"TOTAL raw={tot['raw']} - duplicates removed={tot['dup']} = {tot['raw']-tot['dup']}; output={tot['out']}")
if tot["raw"] - tot["dup"] != tot["out"]: fails.append("total mismatch")

# 2. no empty county/date/drug; level domain
print("\n== 2. completeness ==")
for c in ["county", "oc_date", "drug_name", "drug_level", "year", "quarter", "month", "source_file"]:
    n = int(df[c].isna().sum() + (df[c].astype(str).str.strip() == "").sum())
    print(f"empty {c}: {n}"); 
    if n: fails.append(f"empty {c}")
if df["drug_level"].isin(["第一級", "第二級", "第三級", "第四級", "未分級"]).all() is False: fails.append("bad level")
if not df["month"].between(1, 12).all() or not df["quarter"].between(1, 4).all(): fails.append("bad month/quarter")
print("dup rows left (same file, all raw cols):", int(df.duplicated(["source_file","case_no","roc_date_raw","address","place1","place2","place3","suspects","drug_raw","weight_g"]).sum()))
print("neg weight:", int((df.weight_g < 0).sum()))

# 3. yearly summary
print("\n== 3. per year: rows (cases-by-drug lines), distinct case ids, weight ==")
df["case_id"] = df["source_file"] + "#" + df["case_no"].astype(str)
y = df.groupby("year").agg(rows=("case_id", "size"), cases=("case_id", "nunique"), weight_kg=("weight_kg", "sum"))
print(y.round(1).to_string())
print("\ndrug_level rows:\n", df["drug_level"].value_counts().to_string())
print("\nflags: day_unknown=%d outlier(<2014)=%d weight_zero=%d garbled_names=%d" % (
    df.date_day_unknown.sum(), df.date_outlier.sum(), df.weight_zero.sum(), df.name_garbled.sum()))
print(f"\nperiod {df.oc_date.min()} .. {df.oc_date.max()}; counties={df.county.nunique()}; drugs={df.drug_name.nunique()} (raw {df.drug_raw.nunique()})")
print("\nRESULT:", "FAIL " + "; ".join(fails) if fails else "ALL CHECKS PASSED")
sys.exit(1 if fails else 0)
