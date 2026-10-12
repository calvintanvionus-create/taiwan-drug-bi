# /// script
# requires-python = ">=3.10"
# dependencies = ["pandas>=2.0"]
# ///
"""ETL: merge the 46 quarterly NPA drug-case CSVs (data.gov.tw 57268) into data/drug_cases_<year>.csv.

Run:  uv run etl/etl.py        (raw/ is read-only input; nothing there is modified)

Units: weight is 公克 (grams) in every source file (header 數量淨重_克 / 數量（淨重）_克),
so no conversion is needed; output has weight_g (grams) and weight_kg (= g / 1000).
Dedup: exact duplicates of all 10 raw columns *within the same source file* are dropped
(cross-file identical rows are kept: `no` restarts in each file, so they are different cases).
"""
import csv, datetime, glob, json, os, re, unicodedata
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "raw", "npa_drug_crime")
OUT = os.path.join(ROOT, "data")

# ---------- county ----------
COUNTIES = ["新北市", "臺北市", "桃園市", "臺中市", "臺南市", "高雄市", "基隆市", "新竹市", "嘉義市",
            "新竹縣", "苗栗縣", "彰化縣", "南投縣", "雲林縣", "嘉義縣", "屏東縣", "宜蘭縣", "花蓮縣",
            "臺東縣", "澎湖縣", "金門縣", "連江縣"]
OVERSEAS, SEA, UNKNOWN = "境外(外國)", "海域", "無法辨識"

def clean_addr(a):
    a = unicodedata.normalize("NFKC", a).strip().replace("台", "臺")
    return a

def split_addr(a):
    a = clean_addr(a)
    if a[:3] in COUNTIES:
        return a[:3], a[3:]
    if a.startswith("外國"):
        return OVERSEAS, a[2:]
    if "海峽" in a or "沿海" in a or a.startswith("臺灣海"):
        return SEA, a                       # 臺灣海峽 / 新竹鹿港沿海 (no county)
    return UNKNOWN, a                       # '2', '999', '99999000'

# ---------- drug names ----------
# alias -> canonical (after NFKC + whitespace strip). Same substance, different spelling.
ALIAS = {
    "依托咪酯(Etomidate)": "依托咪酯",
    "大麻(大麻花及大麻葉)": "大麻",
    "MDMA(快樂丸、搖頭丸、3,4-亞甲基雙氧甲基安非他命)": "MDMA(快樂丸)",
    "甲基-安非他命(Methylamphetamine)": "甲基安非他命",
    "甲基-卡西酮(Methylcathinone)": "甲基卡西酮",
    "4-甲氧基安非他命(PMA)": "副甲氧基安非他命(PMA)",
    "FM2": "氟硝西泮(FM2)",
    "氟硝西泮": "氟硝西泮(FM2)",
}
STATUTE = "條例第2條明列"
GAZETTE = "行政院公告品項(依現行分級,未逐項核對公告)"
LEVELS = {  # canonical name -> (level, basis).  Current (2026) level, NOT the level at time of offence.
    "海洛因": (1, STATUTE), "嗎啡": (1, STATUTE), "鴉片(阿片)": (1, STATUTE), "古柯鹼": (1, STATUTE),
    "美沙冬": (1, GAZETTE), "吩坦尼": (1, GAZETTE),
    "安非他命": (2, STATUTE), "大麻": (2, STATUTE), "罌粟": (2, STATUTE), "古柯": (2, STATUTE),
    "配西汀": (2, STATUTE), "潘他唑新(速賜康)": (2, STATUTE),
    "甲基安非他命": (2, GAZETTE), "MDMA(快樂丸)": (2, GAZETTE), "3,4-亞甲基雙氧安非他命(MDA)": (2, GAZETTE),
    "大麻脂": (2, GAZETTE), "大麻浸膏": (2, GAZETTE), "大麻酊": (2, GAZETTE), "四氫大麻酚": (2, GAZETTE),
    "GHB(伽瑪-羥基丁酸)": (2, GAZETTE),
    "依托咪酯": (2, "2025-11 起列二級(2024-08~2025-11 為三級);此處採現行分級"),
    "左旋甲基安非他命": (2, "安非他命相類製品(推定)"), "右旋安非他命": (2, "安非他命相類製品(推定)"),
    "左旋安非他命": (2, "安非他命相類製品(推定)"), "外消旋甲基安非他命": (2, "安非他命相類製品(推定)"),
    "西可巴比妥(紅中)": (3, STATUTE), "異戊巴比妥(青發)": (3, STATUTE),
    "K(愷)他命(含液態)": (3, GAZETTE), "氟硝西泮(FM2)": (3, GAZETTE), "硝甲西泮(硝甲氮平)": (3, GAZETTE),
    "4-甲基甲基卡西酮(喵喵)": (3, GAZETTE), "麥角二乙胺(LSD)": (3, GAZETTE),
    "阿普唑他(蝴蝶片)": (4, STATUTE), "二丙烯基巴比妥": (4, STATUTE),
    "安定(煩寧)": (4, GAZETTE), "氯硝西泮": (4, GAZETTE), "硝西泮": (4, GAZETTE), "勞拉西泮": (4, GAZETTE),
    "苯巴比妥": (4, GAZETTE), "舒樂安定": (4, GAZETTE), "唑匹可隆": (4, GAZETTE),
    "氯二氮平(Chlordiazepoxide)": (4, GAZETTE),
}
LEVEL_LABEL = {1: "第一級", 2: "第二級", 3: "第三級", 4: "第四級"}

def norm_drug(raw):
    s = unicodedata.normalize("NFKC", raw)
    s = re.sub(r"\s+", "", s)
    s = s.replace("?咯", "吡咯")            # 吡 was lost to '?' in the source encoding (pyrrolidine names)
    return ALIAS.get(s, s)

# ---------- dates ----------
def parse_date(d):
    """7/6-digit ROC (yyymmdd) or 8-digit Gregorian (yyyymmdd). day/month '00' = unknown -> 1."""
    d = d.strip()
    if len(d) == 8:
        y, m, dd = int(d[:4]), int(d[4:6]), int(d[6:])
    else:
        y, m, dd = int(d[:-4]) + 1911, int(d[-4:-2]), int(d[-2:])
    unknown = False
    if dd == 0:
        dd, unknown = 1, True
    try:
        return datetime.date(y, m, dd), unknown
    except ValueError:
        raise ValueError(f"unparseable date {d!r}")

def main():
    os.makedirs(OUT, exist_ok=True)
    files = sorted(glob.glob(os.path.join(RAW, "*.csv")))
    out, dropped, per_file = [], [], {}
    for f in files:
        fn = os.path.basename(f)
        with open(f, encoding="utf-8-sig", newline="") as fh:
            rd = csv.reader(fh)
            header, _zh = next(rd), next(rd)          # row 2 is the Chinese header line
            assert header == ["no","type","oc_dt","oc_addr","oc_p1","oc_p2","oc_p3","proc_no","kind","weight_g"], (fn, header)
            seen, n_raw, w_raw = set(), 0, 0.0
            for line_no, row in enumerate(rd, start=3):
                assert len(row) == 10, (fn, line_no)
                n_raw += 1; w = float(row[9]); w_raw += w
                key = tuple(row)
                if key in seen:
                    dropped.append((fn, line_no, w)); continue
                seen.add(key)
                date, unk = parse_date(row[2])
                county, district = split_addr(row[3])
                name = norm_drug(row[8])
                lvl, basis = LEVELS.get(name, (None, ""))
                out.append(dict(
                    source_file=fn, source_line=line_no, case_no=int(row[0]), case_type=row[1].strip(),
                    oc_date=date.isoformat(), year=date.year, quarter=(date.month - 1) // 3 + 1, month=date.month,
                    roc_date_raw=row[2].strip(), county=county, district=district, address=clean_addr(row[3]),
                    place1=row[4].strip(), place2=row[5].strip(), place3=row[6].strip(),
                    suspects=int(row[7]), drug_raw=row[8], drug_name=name,
                    drug_level=LEVEL_LABEL.get(lvl, "未分級"), weight_g=w, weight_kg=round(w / 1000, 6),
                    date_day_unknown=unk, date_outlier=date.year < 2014,
                    weight_zero=(w == 0), name_garbled=("?" in name)))
            per_file[fn] = dict(raw_rows=n_raw, raw_weight_g=round(w_raw, 4))
    df = pd.DataFrame(out)
    # GitHub limits single files to 100 MB -> one file per occurrence year (data/drug_cases_<year>.csv)
    for old in glob.glob(os.path.join(OUT, "drug_cases*.csv")):
        os.remove(old)
    for year, part in df.groupby("year"):
        part.to_csv(os.path.join(OUT, f"drug_cases_{year}.csv"), index=False, encoding="utf-8-sig")

    # mapping table
    m = (df.groupby(["drug_raw", "drug_name", "drug_level"]).size().rename("rows").reset_index())
    m["raw_name_nfkc"] = m["drug_raw"].map(lambda s: unicodedata.normalize("NFKC", s))
    m["level_basis"] = m["drug_name"].map(lambda n: LEVELS.get(n, (None, ""))[1])
    m["name_garbled"] = m["drug_name"].str.contains(r"\?", regex=True)
    m = m[["drug_raw", "drug_name", "drug_level", "level_basis", "name_garbled", "rows"]].sort_values(["drug_name", "drug_raw"])
    m.to_csv(os.path.join(OUT, "drug_name_mapping.csv"), index=False, encoding="utf-8-sig")

    for fn, _l, w in dropped:
        per_file[fn].setdefault("dropped_rows", 0); per_file[fn].setdefault("dropped_weight_g", 0.0)
        per_file[fn]["dropped_rows"] += 1; per_file[fn]["dropped_weight_g"] = round(per_file[fn]["dropped_weight_g"] + w, 4)
    log = dict(files=len(files), raw_rows=sum(v["raw_rows"] for v in per_file.values()),
               dropped_duplicates=len(dropped), dropped_with_zero_weight=sum(1 for *_, w in dropped if w == 0),
               output_rows=len(df), per_file=per_file)
    with open(os.path.join(OUT, "etl_log.json"), "w", encoding="utf-8") as fh:
        json.dump(log, fh, ensure_ascii=False, indent=1)
    print(f"files={log['files']} raw_rows={log['raw_rows']} dropped_duplicates={log['dropped_duplicates']} "
          f"(zero-weight: {log['dropped_with_zero_weight']}) output_rows={log['output_rows']}")

if __name__ == "__main__":
    main()
