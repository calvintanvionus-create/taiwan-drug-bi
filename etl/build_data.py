# /// script
# requires-python = ">=3.10"
# dependencies = ["pandas>=2.0"]
# ///
"""Aggregate data/drug_cases_<year>.csv into docs/data.js (loadable via <script src>, no server needed).

Run:  uv run etl/build_data.py

window.DRUG_DATA = {
  meta:    {...},
  periods: ["2013前","2014Q1",...],      # year-quarter of oc_date; years < 2014 (372 outlier rows) are lumped into "2013前"
  counties, levels, drugs: [...],        # dictionaries; rows reference them by index
  cols:    ["p","c","l","d","n","nw","w"],
  rows:    [[p,c,l,d,n,nw,w], ...]
}
n  = number of rows in drug_cases_<year>.csv (= one case x one drug line; NOT distinct cases)
nw = rows among them with weight_g > 0
w  = total weight in grams over rows with weight_g > 0 (zero-weight rows add nothing anyway)
Per county only the top-15 drug names (by n) are kept; the rest become "其他品項" (level is preserved).
"""
import glob, json, os, sys
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOP_N, OTHER = 15, "其他品項"
LEVELS = ["第一級", "第二級", "第三級", "第四級", "未分級"]

df = pd.concat([pd.read_csv(f, encoding="utf-8-sig", usecols=["year", "quarter", "county", "drug_level", "drug_name", "weight_g"])
                for f in sorted(glob.glob(os.path.join(ROOT, "data", "drug_cases_*.csv")))], ignore_index=True)
total_rows, total_w = len(df), float(df.weight_g.sum())

df["period"] = [f"{y}Q{q}" if y >= 2014 else "2013前" for y, q in zip(df.year, df.quarter)]
top = (df.groupby(["county", "drug_name"]).size().rename("n").reset_index()
         .sort_values(["county", "n"], ascending=[True, False]).groupby("county").head(TOP_N))
keep = set(zip(top.county, top.drug_name))
df["drug"] = [d if (c, d) in keep else OTHER for c, d in zip(df.county, df.drug_name)]
df["pos"] = df.weight_g > 0
df["wpos"] = df.weight_g.where(df.pos, 0.0)

g = (df.groupby(["period", "county", "drug_level", "drug"])
       .agg(n=("pos", "size"), nw=("pos", "sum"), w=("wpos", "sum")).reset_index())

periods = sorted(g.period.unique(), key=lambda p: (-1, 0) if p == "2013前" else (int(p[:4]), int(p[-1])))
counties = sorted(g.county.unique())
drugs = sorted(d for d in g.drug.unique() if d != OTHER) + [OTHER]
ix = lambda lst: {v: i for i, v in enumerate(lst)}
P, C, L, D = ix(periods), ix(counties), ix(LEVELS), ix(drugs)
rows = [[P[r.period], C[r.county], L[r.drug_level], D[r.drug], int(r.n), int(r.nw), round(float(r.w), 2)]
        for r in g.itertuples()]

data = dict(
    meta=dict(source="內政部警政署 毒品犯罪資料 (data.gov.tw 57268)", built_from="data/drug_cases_<year>.csv",
              n_unit="列數(案件×毒品品項,非去重案件數)", weight_unit="公克(只計重量>0的列)",
              top_n_per_county=TOP_N, other_label=OTHER,
              note="毒品級別為現行(2026)分級;2013前=發生日期早於2014年的異常/舊案列"),
    periods=periods, counties=counties, levels=LEVELS, drugs=drugs,
    cols=["p", "c", "l", "d", "n", "nw", "w"], rows=rows)
out = os.path.join(ROOT, "docs", "data.js")
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, "w", encoding="utf-8") as fh:
    fh.write("window.DRUG_DATA = " + json.dumps(data, ensure_ascii=False, separators=(",", ":")) + ";\n")

# ---- check: re-read the written file, compare with the CSV ----
txt = open(out, encoding="utf-8").read()
back = json.loads(txt[len("window.DRUG_DATA = "):].rstrip().rstrip(";"))
n_sum = sum(r[4] for r in back["rows"]); w_sum = sum(r[6] for r in back["rows"])
ok_n = n_sum == total_rows
ok_w = abs(w_sum - total_w) < 1.0
print(f"data.js size: {os.path.getsize(out)/1024/1024:.2f} MB ({os.path.getsize(out):,} bytes), {len(back['rows']):,} aggregate rows")
print(f"periods={len(periods)} counties={len(counties)} drugs(incl. {OTHER})={len(drugs)}")
print(f"sum(n) in data.js = {n_sum:,}   rows in drug_cases_*.csv = {total_rows:,}   {'MATCH' if ok_n else 'MISMATCH'}")
print(f"sum(w) in data.js = {w_sum:,.2f} g   sum weight_g in csv = {total_w:,.2f} g   {'MATCH' if ok_w else 'MISMATCH'}")
print(f"rows folded into {OTHER}: {int(g[g.drug == OTHER].n.sum()):,} ({g[g.drug == OTHER].n.sum()/total_rows:.2%})")
sys.exit(0 if ok_n and ok_w else 1)
