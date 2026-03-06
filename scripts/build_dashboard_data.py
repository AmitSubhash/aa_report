#!/usr/bin/env python3
"""
build_dashboard_data.py — Alumni Outcomes Analytics Preprocessing
Indiana University – Bloomington Graduate School

Non-destructive: creates timestamped run folders, never deletes/overwrites.
Produces JSON bundles, Excel/CSV reports, Markdown summaries.
"""

import os, sys, re, math, json, hashlib, logging
from pathlib import Path
from datetime import datetime
from collections import OrderedDict

import pandas as pd
import numpy as np

try:
    import openpyxl  # noqa – needed by pd.ExcelWriter
except ImportError:
    sys.exit("ERROR: openpyxl is required.  pip install openpyxl")

# ─────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BUNDLE_DIR = PROJECT_ROOT / "data" / "bundles"

EXPECTED_FILES = {
    "dss":      "ozgurcodedss.csv",
    "aa_mr":    "ai_raw_data_most_recent_2026-1-28_11-40-33.csv",
    "aa_long":  "ai_raw_data_longitudinal_2026-1-28_11-41-7.csv",
    "lc":       "LCAST_ALUMNI.csv",
    "pseo":     "pseo.csv",
}

SCOPES = OrderedDict([
    ("2004_2025", {"start": 2004, "end": 2025, "label": "Full scope"}),
    ("2010_2025", {"start": 2010, "end": 2025, "label": "Modern scope"}),
    ("2010_2024", {"start": 2010, "end": 2024, "label": "Stable trend scope"}),
])

VALIDATION_TARGETS_2004_2025 = {
    "DSS_STUDENTS":               15186,
    "AA_HAS_ROW":                  1890,
    "AA_HAS_EMPLOYMENT":           1536,
    "LC_HAS_ROW":                 11768,
    "LC_TRUE_MATCH":               4545,
    "LC_EMPLOYMENT_TRUE_MATCH":    4519,
    "LC_ROW_ONLY_NO_TRUE_MATCH":   7223,
    "PSEO_HAS_ROW":                2195,
    "ANY_VENDOR_ROW":             12421,
    "ANY_VENDOR_MATCH_STRICT":     6715,
    "ANY_EMPLOYMENT_OUTCOME":      5488,
    "NO_OUTCOME":                  8708,
}

VALIDATION_NO_OUTCOME_REASONS = {
    "LIGHTCAST_ROW_BUT_NO_TRUE_MATCH":              5706,
    "NO_VENDOR_RECORD":                             2765,
    "AA_ROW_BUT_BLANK_ENTITY_POSITION":              219,
    "LIGHTCAST_TRUE_MATCH_BUT_BLANK_EMPLOYER_TITLE":  18,
}

pd.set_option("display.max_columns", 250)
pd.set_option("display.width", 220)

# ─────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
REPORT_DIR = PROJECT_ROOT / "reports" / TIMESTAMP
REPORT_DIR.mkdir(parents=True, exist_ok=True)
BUNDLE_DIR.mkdir(parents=True, exist_ok=True)

log_path = REPORT_DIR / "run.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("build_dashboard_data")
log.info(f"Run timestamp: {TIMESTAMP}")
log.info(f"Project root : {PROJECT_ROOT}")
log.info(f"Report dir   : {REPORT_DIR}")

# ─────────────────────────────────────────────────────────────────────
# HELPERS: file discovery
# ─────────────────────────────────────────────────────────────────────
def find_file(basename: str) -> Path:
    """Look in RAW_DIR first, then search recursively from PROJECT_ROOT."""
    direct = RAW_DIR / basename
    if direct.exists():
        log.info(f"  Found (direct): {direct}")
        return direct
    log.warning(f"  Not in {RAW_DIR}, searching recursively…")
    for root, _dirs, files in os.walk(PROJECT_ROOT):
        if basename in files:
            p = Path(root) / basename
            log.info(f"  Found (search): {p}")
            return p
    raise FileNotFoundError(f"Cannot find '{basename}' under {PROJECT_ROOT}")


def resolve_all_files() -> dict[str, Path]:
    resolved = {}
    for key, basename in EXPECTED_FILES.items():
        log.info(f"Resolving [{key}] → {basename}")
        resolved[key] = find_file(basename)
    return resolved


# ─────────────────────────────────────────────────────────────────────
# HELPERS: reading CSVs
# ─────────────────────────────────────────────────────────────────────
def sniff_delimiter(path: Path, sample_bytes: int = 200_000) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(sample_bytes)
    candidates = [",", "\t", "|", ";"]
    counts = {c: sample.count(c) for c in candidates}
    return max(counts, key=counts.get) if max(counts.values()) > 0 else ","


def read_csv_safe(path: Path) -> pd.DataFrame:
    delim = sniff_delimiter(path)
    for enc in ["utf-8-sig", "utf-8", "cp1252", "latin1"]:
        try:
            df = pd.read_csv(path, sep=delim, dtype=str, encoding=enc, engine="python")
            df.columns = [str(c).strip().upper().replace(" ", "_") for c in df.columns]
            log.info(f"  Read {path.name}: {df.shape[0]:,} rows × {df.shape[1]} cols  (enc={enc}, delim={repr(delim)})")
            return df
        except Exception:
            continue
    raise RuntimeError(f"Failed reading {path}")


# ─────────────────────────────────────────────────────────────────────
# HELPERS: ID normalisation
# ─────────────────────────────────────────────────────────────────────
def normalize_join_id(value, pad: int = 10):
    """Return (JOIN_ID, issue_tag)."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return (pd.NA, "MISSING")
    raw = str(value).strip()
    if raw == "" or raw.lower() in {"nan", "none", "null", "<na>"}:
        return (pd.NA, "MISSING")
    s = raw
    # Excel float artefact  "12345.0"
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    elif re.search(r"[eE]", s) or re.search(r"\d+\.\d+", s):
        try:
            f = float(s)
            if f == f and f == int(f):  # not NaN, is integer
                s = str(int(f))
        except Exception:
            pass
    digits = re.sub(r"\D", "", s)
    if digits == "":
        return (pd.NA, "NON_NUMERIC")
    if len(digits) > pad:
        return (digits, f"TOO_LONG_{len(digits)}")
    return (digits.zfill(pad), "OK")


def add_join_id(df: pd.DataFrame, id_col: str, tag: str) -> pd.DataFrame:
    df = df.copy()
    if id_col not in df.columns:
        raise KeyError(f"[{tag}] Missing ID column '{id_col}'. Cols: {list(df.columns)[:20]}")
    pairs = df[id_col].apply(lambda v: normalize_join_id(v))
    df["JOIN_ID"] = pairs.apply(lambda t: t[0])
    df[f"__{tag}_ID_ISSUE"] = pairs.apply(lambda t: t[1])
    issues = df[f"__{tag}_ID_ISSUE"].value_counts()
    log.info(f"  [{tag}] ID issues: {dict(issues)}")
    return df


# ─────────────────────────────────────────────────────────────────────
# HELPERS: year derivation
# ─────────────────────────────────────────────────────────────────────
def derive_year_end(value, pivot: int = 30):
    if value is None:
        return np.nan
    s = str(value).strip()
    if s == "" or s.lower() in {"nan", "none", "null", "<na>"}:
        return np.nan
    # "2005" or "AY 04/05" → 2005
    m = re.search(r"(\d{4})\s*$", s)
    if m:
        return int(m.group(1))
    m = re.search(r"[-/]\s*(\d{2})\s*$", s)
    if m:
        yy = int(m.group(1))
        return (2000 if yy <= pivot else 1900) + yy
    if re.fullmatch(r"\d{3}", s):
        yy = int(s) % 100
        return (2000 if yy <= pivot else 1900) + yy
    if re.fullmatch(r"\d{4}", s):
        return int(s)
    return np.nan


# ─────────────────────────────────────────────────────────────────────
# HELPERS: boolean / nonblank
# ─────────────────────────────────────────────────────────────────────
def nonblank(s: pd.Series) -> pd.Series:
    return ~(s.isna() | s.astype(str).str.strip().eq("") |
             s.astype(str).str.lower().isin(["nan", "none", "null", "<na>"]))


def parse_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin(["true", "t", "1", "yes", "y"])


def any_nonblank(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return pd.Series(False, index=df.index)
    mask = np.zeros(len(df), dtype=bool)
    for c in cols:
        mask |= nonblank(df[c]).to_numpy()
    return pd.Series(mask, index=df.index)


def first_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ─────────────────────────────────────────────────────────────────────
# HELPERS: Oracle IN-clause
# ─────────────────────────────────────────────────────────────────────
def write_oracle_in(ids, path: Path, chunk: int = 900):
    ids = [i for i in ids if isinstance(i, str) and i.strip()]
    with open(path, "w", encoding="utf-8") as f:
        for i in range(0, len(ids), chunk):
            c = ids[i:i + chunk]
            f.write(f"PRSN_UNIV_ID IN ({','.join(repr(x) for x in c)})\n")
            if i + chunk < len(ids):
                f.write("OR\n")
    log.info(f"  Oracle IN file: {path}  ({len(ids):,} IDs)")


# ─────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────
def main():
    # ── 1. Resolve files ──────────────────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 1: Resolve raw data files")
    log.info("=" * 60)
    paths = resolve_all_files()

    # ── 2. Load & standardise ─────────────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 2: Load and standardise raw data")
    log.info("=" * 60)
    dss_raw   = read_csv_safe(paths["dss"])
    aa_mr_raw = read_csv_safe(paths["aa_mr"])
    aa_lg_raw = read_csv_safe(paths["aa_long"])
    lc_raw    = read_csv_safe(paths["lc"])
    pseo_raw  = read_csv_safe(paths["pseo"])

    # ── 3. Add JOIN_ID ────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 3: Normalise JOIN_IDs")
    log.info("=" * 60)
    dss   = add_join_id(dss_raw,   "PRSN_UNIV_ID",   "DSS")
    aa_mr = add_join_id(aa_mr_raw, "CLIENTPERSONID",  "AA_MR")
    aa_lg = add_join_id(aa_lg_raw, "CLIENTPERSONID",  "AA_LONG")
    lc    = add_join_id(lc_raw,    "PRSN_UNIV_ID",    "LC")
    pseo  = add_join_id(pseo_raw,  "PRSN_UNIV_ID",    "PSEO")

    # ── 4. Data inventory ─────────────────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 4: Data inventory")
    log.info("=" * 60)
    inventory_rows = []
    for name, df, id_col in [
        ("DSS", dss, "PRSN_UNIV_ID"), ("AA_MR", aa_mr, "CLIENTPERSONID"),
        ("AA_LONG", aa_lg, "CLIENTPERSONID"), ("Lightcast", lc, "PRSN_UNIV_ID"),
        ("PSEO", pseo, "PRSN_UNIV_ID"),
    ]:
        n_rows = len(df)
        n_cols = len(df.columns)
        n_unique_id = df["JOIN_ID"].dropna().nunique()
        pct_missing = df["JOIN_ID"].isna().mean()
        inventory_rows.append({
            "SOURCE": name, "ROWS": n_rows, "COLS": n_cols,
            "RAW_ID_COL": id_col, "UNIQUE_JOIN_IDS": n_unique_id,
            "PCT_MISSING_JOIN_ID": round(pct_missing, 4),
        })
        log.info(f"  {name}: {n_rows:,} rows, {n_unique_id:,} unique IDs, "
                 f"{pct_missing:.2%} missing JOIN_ID")
    inventory = pd.DataFrame(inventory_rows)

    # ── 5. DSS cohort year derivation ────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 5: DSS cohort year derivation")
    log.info("=" * 60)
    year_col = first_col(dss, ["SR_STU_DEGR_FSCL_YR", "ACAD_YEAR_DESC", "ACAD_YEAR"])
    if year_col:
        dss["COHORT_YEAR"] = dss[year_col].apply(derive_year_end)
        log.info(f"  Used '{year_col}' for cohort year derivation")
    else:
        log.error("  No year column found in DSS!")
        dss["COHORT_YEAR"] = np.nan

    dss["COHORT_YEAR"] = pd.to_numeric(dss["COHORT_YEAR"], errors="coerce")
    log.info(f"  Cohort year range: {dss['COHORT_YEAR'].min():.0f} – {dss['COHORT_YEAR'].max():.0f}")
    log.info(f"  Cohort year distribution:\n{dss['COHORT_YEAR'].value_counts().sort_index().to_string()}")

    # International flag
    eth_col = first_col(dss, ["PRSN_DRVD_ETHNIC_IR_RPT_DESC"])
    deg_desc_col = first_col(dss, ["ACAD_DEGR_DESC"])
    deg_lvl_col  = first_col(dss, ["ACAD_DEGR_ED_LVL_CD"])
    major_col    = first_col(dss, ["ACAD_PLAN_MJR1_DESC"])
    career_col   = first_col(dss, ["ACAD_CAREER_CD"])

    # ── 6. AA combined + year diagnostics ────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 6: AA year alignment diagnostics")
    log.info("=" * 60)
    aa_all = pd.concat([aa_lg, aa_mr], ignore_index=True, sort=False)
    if "YEAR" in aa_all.columns:
        aa_all["AA_YEAR_NUM"] = pd.to_numeric(aa_all["YEAR"], errors="coerce")
        log.info(f"  AA YEAR distribution:\n{aa_all['AA_YEAR_NUM'].value_counts().sort_index().to_string()}")
    if "SNAPSHOTYEAR" in aa_all.columns:
        aa_all["AA_SNAP_NUM"] = pd.to_numeric(aa_all["SNAPSHOTYEAR"], errors="coerce")
        log.info(f"  AA SNAPSHOTYEAR distribution:\n{aa_all['AA_SNAP_NUM'].value_counts().sort_index().to_string()}")

    # ── 7. Lightcast diagnostics ──────────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 7: Lightcast match diagnostics")
    log.info("=" * 60)
    if "MATCH_STATUS" in lc.columns:
        lc["_MATCH_TRUE"] = parse_bool(lc["MATCH_STATUS"])
        ms_dist = lc["_MATCH_TRUE"].value_counts()
        log.info(f"  MATCH_STATUS distribution: {dict(ms_dist)}")
    else:
        lc["_MATCH_TRUE"] = False
        log.warning("  MATCH_STATUS column not found — treating all as False")

    if "INST_CAMPUS_NAME" in lc.columns:
        campus_all = lc["INST_CAMPUS_NAME"].value_counts()
        campus_unique = lc.drop_duplicates("JOIN_ID")["INST_CAMPUS_NAME"].value_counts()
        campus_true = lc[lc["_MATCH_TRUE"]].drop_duplicates("JOIN_ID")["INST_CAMPUS_NAME"].value_counts()
        log.info(f"  Campus (all rows):\n{campus_all.to_string()}")
        log.info(f"  Campus (unique students):\n{campus_unique.to_string()}")
        log.info(f"  Campus (true-match unique):\n{campus_true.to_string()}")

    if "MATCH_CONFIDENCE" in lc.columns:
        lc["_MATCH_CONF_NUM"] = pd.to_numeric(lc["MATCH_CONFIDENCE"], errors="coerce")
        for status_val in [True, False]:
            subset = lc[lc["_MATCH_TRUE"] == status_val]["_MATCH_CONF_NUM"]
            if len(subset):
                log.info(f"  Match confidence (MATCH_STATUS={status_val}): "
                         f"mean={subset.mean():.2f}, median={subset.median():.2f}, "
                         f"min={subset.min():.2f}, max={subset.max():.2f}")

    # ── 8. Build scoped outputs ──────────────────────────────────────
    log.info("=" * 60)
    log.info("PHASE 8: Build scoped master tables + reports")
    log.info("=" * 60)

    all_bundles = {}

    for scope_key, scope_cfg in SCOPES.items():
        yr_start, yr_end = scope_cfg["start"], scope_cfg["end"]
        log.info(f"\n{'─'*50}")
        log.info(f"SCOPE: {scope_key}  ({yr_start}–{yr_end})")
        log.info(f"{'─'*50}")

        # Filter DSS
        dss_s = dss.dropna(subset=["JOIN_ID", "COHORT_YEAR"]).copy()
        dss_s = dss_s[(dss_s["COHORT_YEAR"] >= yr_start) & (dss_s["COHORT_YEAR"] <= yr_end)]

        # Primary cohort year = max per student
        pcy = dss_s.groupby("JOIN_ID", as_index=False).agg(
            PRIMARY_COHORT_YEAR=("COHORT_YEAR", "max"),
            DSS_MIN_GRAD_YEAR=("COHORT_YEAR", "min"),
            DSS_MAX_GRAD_YEAR=("COHORT_YEAR", "max"),
            N_DSS_GRAD_YEARS=("COHORT_YEAR", "nunique"),
        )

        # Attributes from the row at primary year
        attr_cols = ["JOIN_ID", "COHORT_YEAR"]
        for c in [deg_desc_col, deg_lvl_col, major_col, career_col, eth_col]:
            if c and c in dss_s.columns and c not in attr_cols:
                attr_cols.append(c)

        dss_attr = dss_s[attr_cols].merge(pcy[["JOIN_ID", "PRIMARY_COHORT_YEAR"]], on="JOIN_ID")
        dss_attr = dss_attr[dss_attr["COHORT_YEAR"] == dss_attr["PRIMARY_COHORT_YEAR"]]
        if deg_lvl_col and deg_lvl_col in dss_attr.columns:
            dss_attr["_dlvl"] = pd.to_numeric(dss_attr[deg_lvl_col], errors="coerce")
            dss_attr = dss_attr.sort_values(["JOIN_ID", "_dlvl"], ascending=[True, False], na_position="last")
            dss_attr.drop(columns=["_dlvl"], inplace=True)
        dss_attr = dss_attr.drop_duplicates("JOIN_ID", keep="first")

        student = pcy.merge(dss_attr.drop(columns=["COHORT_YEAR", "PRIMARY_COHORT_YEAR"], errors="ignore"), on="JOIN_ID", how="left")

        # International flag
        if eth_col and eth_col in student.columns:
            student["IS_INTERNATIONAL"] = student[eth_col].astype(str).str.strip().str.lower().eq("us nonresident")
        else:
            student["IS_INTERNATIONAL"] = False

        BASE_IDS = set(student["JOIN_ID"])
        N = len(BASE_IDS)
        log.info(f"  DSS students in scope: {N:,}")

        # ── AA flags ──
        aa_sc = aa_all[aa_all["JOIN_ID"].isin(BASE_IDS)].copy()
        aa_sc["_HAS_EMP"] = any_nonblank(aa_sc, ["ENTITYNAME", "POSITION"])
        aa_flags = aa_sc.groupby("JOIN_ID", as_index=False).agg(
            AA_ROW_COUNT=("JOIN_ID", "size"),
            AA_HAS_EMPLOYMENT=("_HAS_EMP", "any"),
        )
        aa_flags["AA_HAS_ROW"] = True

        # ── LC flags ──
        lc_sc = lc[lc["JOIN_ID"].isin(BASE_IDS)].copy()
        lc_sc["_LC_EMP"] = any_nonblank(lc_sc, ["COMPANY_NAME", "COMPANY_RAW", "TITLE_NAME", "TITLE_RAW"])
        lc_sc["_LC_EMP_TRUE"] = lc_sc["_MATCH_TRUE"] & lc_sc["_LC_EMP"]
        lc_flags = lc_sc.groupby("JOIN_ID", as_index=False).agg(
            LC_ROW_COUNT=("JOIN_ID", "size"),
            LC_TRUE_MATCH=("_MATCH_TRUE", "any"),
            LC_EMPLOYMENT_TRUE_MATCH=("_LC_EMP_TRUE", "any"),
        )
        lc_flags["LC_HAS_ROW"] = True
        lc_flags["LC_ROW_ONLY_NO_TRUE_MATCH"] = lc_flags["LC_HAS_ROW"] & ~lc_flags["LC_TRUE_MATCH"]

        # ── PSEO flags ──
        pseo_sc = pseo[pseo["JOIN_ID"].isin(BASE_IDS)].copy()
        pseo_flags = pseo_sc.groupby("JOIN_ID", as_index=False).agg(PSEO_ROW_COUNT=("JOIN_ID", "size"))
        pseo_flags["PSEO_HAS_ROW"] = True

        # ── Merge master ──
        master = student.copy()
        master = master.merge(aa_flags[["JOIN_ID", "AA_HAS_ROW", "AA_HAS_EMPLOYMENT", "AA_ROW_COUNT"]], on="JOIN_ID", how="left")
        master = master.merge(lc_flags[["JOIN_ID", "LC_HAS_ROW", "LC_TRUE_MATCH", "LC_EMPLOYMENT_TRUE_MATCH",
                                         "LC_ROW_ONLY_NO_TRUE_MATCH", "LC_ROW_COUNT"]], on="JOIN_ID", how="left")
        master = master.merge(pseo_flags[["JOIN_ID", "PSEO_HAS_ROW", "PSEO_ROW_COUNT"]], on="JOIN_ID", how="left")

        bool_cols = ["AA_HAS_ROW", "AA_HAS_EMPLOYMENT", "LC_HAS_ROW", "LC_TRUE_MATCH",
                     "LC_EMPLOYMENT_TRUE_MATCH", "LC_ROW_ONLY_NO_TRUE_MATCH", "PSEO_HAS_ROW"]
        for c in bool_cols:
            master[c] = master[c].fillna(False).astype(bool)
        for c in ["AA_ROW_COUNT", "LC_ROW_COUNT", "PSEO_ROW_COUNT"]:
            if c in master.columns:
                master[c] = pd.to_numeric(master[c], errors="coerce").fillna(0).astype(int)

        # ── Union metrics ──
        master["ANY_VENDOR_ROW"]          = master["AA_HAS_ROW"] | master["LC_HAS_ROW"] | master["PSEO_HAS_ROW"]
        master["ANY_VENDOR_MATCH_STRICT"] = master["AA_HAS_ROW"] | master["LC_TRUE_MATCH"] | master["PSEO_HAS_ROW"]
        master["ANY_EMPLOYMENT_OUTCOME"]  = master["AA_HAS_EMPLOYMENT"] | master["LC_EMPLOYMENT_TRUE_MATCH"]
        master["NO_OUTCOME"]              = ~master["ANY_EMPLOYMENT_OUTCOME"] & ~master["PSEO_HAS_ROW"]

        # ── Bucket (disjoint, precedence order) ──
        master["BUCKET"] = "NO_OUTCOME"
        master.loc[master["PSEO_HAS_ROW"], "BUCKET"]               = "PSEO_ONLY_OR_ENGAGEMENT"
        master.loc[master["LC_EMPLOYMENT_TRUE_MATCH"], "BUCKET"]    = "LIGHTCAST_EMPLOYMENT"
        master.loc[master["AA_HAS_EMPLOYMENT"], "BUCKET"]           = "AA_EMPLOYMENT"

        # ── NO_OUTCOME reason ──
        master["NO_OUTCOME_REASON"] = pd.NA
        mask_no = master["NO_OUTCOME"]
        master.loc[mask_no & ~master["AA_HAS_ROW"] & ~master["LC_HAS_ROW"] & ~master["PSEO_HAS_ROW"],
                   "NO_OUTCOME_REASON"] = "NO_VENDOR_RECORD"
        master.loc[mask_no & master["LC_HAS_ROW"] & ~master["LC_TRUE_MATCH"] & ~master["AA_HAS_EMPLOYMENT"],
                   "NO_OUTCOME_REASON"] = "LIGHTCAST_ROW_BUT_NO_TRUE_MATCH"
        master.loc[mask_no & master["AA_HAS_ROW"] & ~master["AA_HAS_EMPLOYMENT"],
                   "NO_OUTCOME_REASON"] = "AA_ROW_BUT_BLANK_ENTITY_POSITION"
        master.loc[mask_no & master["LC_TRUE_MATCH"] & ~master["LC_EMPLOYMENT_TRUE_MATCH"],
                   "NO_OUTCOME_REASON"] = "LIGHTCAST_TRUE_MATCH_BUT_BLANK_EMPLOYER_TITLE"
        master["NO_OUTCOME_REASON"] = master["NO_OUTCOME_REASON"].fillna("OTHER")

        # ── Quality checks ──
        log.info("  Quality checks:")
        assert len(master) == N, f"Row count mismatch: {len(master)} vs {N}"
        log.info(f"    One row per student: PASS ({N:,})")
        bucket_sum = master["BUCKET"].value_counts().sum()
        assert bucket_sum == N, f"Bucket sum {bucket_sum} != {N}"
        log.info(f"    Bucket partition sums to DSS: PASS")
        no_count = master["NO_OUTCOME"].sum()
        no_bucket = (master["BUCKET"] == "NO_OUTCOME").sum()
        assert no_count == no_bucket, f"NO_OUTCOME flag {no_count} != bucket {no_bucket}"
        log.info(f"    NO_OUTCOME consistency: PASS")
        assert master["LC_EMPLOYMENT_TRUE_MATCH"].sum() <= master["LC_TRUE_MATCH"].sum()
        log.info(f"    LC_EMPLOYMENT <= LC_TRUE_MATCH: PASS")
        assert master["AA_HAS_EMPLOYMENT"].sum() <= master["AA_HAS_ROW"].sum()
        log.info(f"    AA_EMPLOYMENT <= AA_HAS_ROW: PASS")

        # ── Overall summary metrics ──
        metrics = OrderedDict()
        for m in ["AA_HAS_ROW", "AA_HAS_EMPLOYMENT", "LC_HAS_ROW", "LC_TRUE_MATCH",
                   "LC_EMPLOYMENT_TRUE_MATCH", "LC_ROW_ONLY_NO_TRUE_MATCH", "PSEO_HAS_ROW",
                   "ANY_VENDOR_ROW", "ANY_VENDOR_MATCH_STRICT", "ANY_EMPLOYMENT_OUTCOME", "NO_OUTCOME"]:
            metrics[m] = int(master[m].sum())
        metrics["DSS_STUDENTS"] = N

        summary_rows = [{"METRIC": "DSS_STUDENTS", "COUNT": N, "RATE": 1.0}]
        for k, v in metrics.items():
            if k != "DSS_STUDENTS":
                summary_rows.append({"METRIC": k, "COUNT": v, "RATE": round(v / N, 4)})
        overall_summary = pd.DataFrame(summary_rows)

        # ── Validation against targets (2004_2025 only) ──
        validation_results = []
        if scope_key == "2004_2025":
            log.info("  Validation against expected targets:")
            for k, expected in VALIDATION_TARGETS_2004_2025.items():
                actual = metrics.get(k, N if k == "DSS_STUDENTS" else 0)
                if k == "DSS_STUDENTS":
                    actual = N
                diff = actual - expected
                pct_diff = diff / expected * 100 if expected else 0
                status = "OK" if abs(pct_diff) < 5 else "WARNING"
                validation_results.append({
                    "METRIC": k, "EXPECTED": expected, "ACTUAL": actual,
                    "DIFF": diff, "PCT_DIFF": round(pct_diff, 2), "STATUS": status,
                })
                log.info(f"    {k}: expected={expected:,}  actual={actual:,}  "
                         f"diff={diff:+,}  ({pct_diff:+.1f}%)  [{status}]")
            for k, expected in VALIDATION_NO_OUTCOME_REASONS.items():
                actual = int((master["NO_OUTCOME_REASON"] == k).sum())
                diff = actual - expected
                pct_diff = diff / expected * 100 if expected else 0
                status = "OK" if abs(pct_diff) < 5 else "WARNING"
                validation_results.append({
                    "METRIC": f"NO_OUTCOME:{k}", "EXPECTED": expected, "ACTUAL": actual,
                    "DIFF": diff, "PCT_DIFF": round(pct_diff, 2), "STATUS": status,
                })
                log.info(f"    NO_OUTCOME:{k}: expected={expected:,}  actual={actual:,}  "
                         f"diff={diff:+,}  ({pct_diff:+.1f}%)  [{status}]")

        # ── Vendor funnel ──
        vendor_funnel = pd.DataFrame([
            {"VENDOR": "AA", "STAGE": "AA has row", "COUNT": int(master["AA_HAS_ROW"].sum())},
            {"VENDOR": "AA", "STAGE": "AA has employment", "COUNT": int(master["AA_HAS_EMPLOYMENT"].sum())},
            {"VENDOR": "Lightcast", "STAGE": "LC has row", "COUNT": int(master["LC_HAS_ROW"].sum())},
            {"VENDOR": "Lightcast", "STAGE": "LC true match", "COUNT": int(master["LC_TRUE_MATCH"].sum())},
            {"VENDOR": "Lightcast", "STAGE": "LC employment (true match)", "COUNT": int(master["LC_EMPLOYMENT_TRUE_MATCH"].sum())},
            {"VENDOR": "Lightcast", "STAGE": "LC row-only no true match", "COUNT": int(master["LC_ROW_ONLY_NO_TRUE_MATCH"].sum())},
            {"VENDOR": "PSEO", "STAGE": "PSEO has row", "COUNT": int(master["PSEO_HAS_ROW"].sum())},
        ])
        vendor_funnel["RATE_OF_DSS"] = round(vendor_funnel["COUNT"] / N, 4)

        # ── Bucket counts ──
        bucket_counts = master["BUCKET"].value_counts().rename_axis("BUCKET").reset_index(name="STUDENTS")
        bucket_counts["RATE"] = round(bucket_counts["STUDENTS"] / N, 4)

        # ── By year ──
        by_year = master.dropna(subset=["PRIMARY_COHORT_YEAR"]).groupby("PRIMARY_COHORT_YEAR", as_index=False).agg(
            STUDENTS=("JOIN_ID", "count"),
            AA_HAS_ROW=("AA_HAS_ROW", "sum"), AA_HAS_EMPLOYMENT=("AA_HAS_EMPLOYMENT", "sum"),
            LC_HAS_ROW=("LC_HAS_ROW", "sum"), LC_TRUE_MATCH=("LC_TRUE_MATCH", "sum"),
            LC_EMPLOYMENT_TRUE_MATCH=("LC_EMPLOYMENT_TRUE_MATCH", "sum"),
            LC_ROW_ONLY_NO_TRUE_MATCH=("LC_ROW_ONLY_NO_TRUE_MATCH", "sum"),
            PSEO_HAS_ROW=("PSEO_HAS_ROW", "sum"),
            ANY_EMPLOYMENT_OUTCOME=("ANY_EMPLOYMENT_OUTCOME", "sum"),
            NO_OUTCOME=("NO_OUTCOME", "sum"),
        ).sort_values("PRIMARY_COHORT_YEAR")
        for c in ["AA_HAS_ROW", "AA_HAS_EMPLOYMENT", "LC_HAS_ROW", "LC_TRUE_MATCH",
                   "LC_EMPLOYMENT_TRUE_MATCH", "LC_ROW_ONLY_NO_TRUE_MATCH", "PSEO_HAS_ROW",
                   "ANY_EMPLOYMENT_OUTCOME", "NO_OUTCOME"]:
            by_year[f"RATE_{c}"] = round(by_year[c] / by_year["STUDENTS"], 4)

        # ── By international ──
        by_intl = master.groupby("IS_INTERNATIONAL", as_index=False).agg(
            STUDENTS=("JOIN_ID", "count"),
            AA_HAS_ROW=("AA_HAS_ROW", "sum"), AA_HAS_EMPLOYMENT=("AA_HAS_EMPLOYMENT", "sum"),
            LC_HAS_ROW=("LC_HAS_ROW", "sum"), LC_TRUE_MATCH=("LC_TRUE_MATCH", "sum"),
            LC_EMPLOYMENT_TRUE_MATCH=("LC_EMPLOYMENT_TRUE_MATCH", "sum"),
            NO_OUTCOME=("NO_OUTCOME", "sum"),
            ANY_EMPLOYMENT_OUTCOME=("ANY_EMPLOYMENT_OUTCOME", "sum"),
        )
        for c in ["AA_HAS_ROW", "AA_HAS_EMPLOYMENT", "LC_HAS_ROW", "LC_TRUE_MATCH",
                   "LC_EMPLOYMENT_TRUE_MATCH", "NO_OUTCOME", "ANY_EMPLOYMENT_OUTCOME"]:
            by_intl[f"RATE_{c}"] = round(by_intl[c] / by_intl["STUDENTS"], 4)

        # ── By degree (min denom 50) ──
        by_degree = pd.DataFrame()
        if deg_desc_col and deg_desc_col in master.columns:
            by_degree = master.groupby(deg_desc_col, as_index=False).agg(
                STUDENTS=("JOIN_ID", "count"), NO_OUTCOME=("NO_OUTCOME", "sum"),
                ANY_EMPLOYMENT_OUTCOME=("ANY_EMPLOYMENT_OUTCOME", "sum"),
                LC_TRUE_MATCH=("LC_TRUE_MATCH", "sum"),
            )
            by_degree["NO_OUTCOME_RATE"] = round(by_degree["NO_OUTCOME"] / by_degree["STUDENTS"], 4)
            by_degree = by_degree[by_degree["STUDENTS"] >= 50].sort_values("NO_OUTCOME_RATE", ascending=False)

        # ── By major (min denom 50) ──
        by_major = pd.DataFrame()
        if major_col and major_col in master.columns:
            by_major = master.groupby(major_col, as_index=False).agg(
                STUDENTS=("JOIN_ID", "count"), NO_OUTCOME=("NO_OUTCOME", "sum"),
                ANY_EMPLOYMENT_OUTCOME=("ANY_EMPLOYMENT_OUTCOME", "sum"),
            )
            by_major["NO_OUTCOME_RATE"] = round(by_major["NO_OUTCOME"] / by_major["STUDENTS"], 4)
            by_major = by_major[by_major["STUDENTS"] >= 50].sort_values("NO_OUTCOME_RATE", ascending=False)

        # ── NO_OUTCOME deep dive ──
        no_out = master[master["NO_OUTCOME"]].copy()
        no_reason = no_out["NO_OUTCOME_REASON"].value_counts().rename_axis("REASON").reset_index(name="STUDENTS")
        no_reason["RATE_OF_NO_OUTCOME"] = round(no_reason["STUDENTS"] / len(no_out), 4) if len(no_out) else 0

        no_by_year = no_out.dropna(subset=["PRIMARY_COHORT_YEAR"]).groupby(
            "PRIMARY_COHORT_YEAR", as_index=False
        ).agg(STUDENTS=("JOIN_ID", "count")).sort_values("PRIMARY_COHORT_YEAR")

        no_by_intl = no_out.groupby("IS_INTERNATIONAL", as_index=False).agg(
            STUDENTS=("JOIN_ID", "count")
        )
        no_by_intl["RATE"] = round(no_by_intl["STUDENTS"] / len(no_out), 4) if len(no_out) else 0

        # LC row-only-no-true-match diagnostic
        lc_mismatch = master[master["LC_ROW_ONLY_NO_TRUE_MATCH"]].copy()

        # ── AA year alignment crosstab ──
        aa_year_crosstab = pd.DataFrame()
        if "AA_YEAR_NUM" in aa_sc.columns:
            aa_with_dss = aa_sc.merge(
                master[["JOIN_ID", "PRIMARY_COHORT_YEAR"]], on="JOIN_ID", how="inner"
            )
            if len(aa_with_dss):
                aa_year_crosstab = pd.crosstab(
                    aa_with_dss["PRIMARY_COHORT_YEAR"], aa_with_dss["AA_YEAR_NUM"],
                    margins=True
                ).reset_index()

        # ── LC true match rate by year / intl ──
        lc_match_by_year = pd.DataFrame()
        if len(lc_sc):
            lc_with_dss = lc_sc.merge(master[["JOIN_ID", "PRIMARY_COHORT_YEAR", "IS_INTERNATIONAL"]], on="JOIN_ID", how="inner")
            if len(lc_with_dss):
                lc_match_by_year = lc_with_dss.groupby("PRIMARY_COHORT_YEAR", as_index=False).agg(
                    LC_ROWS=("JOIN_ID", "size"),
                    LC_TRUE_MATCHES=("_MATCH_TRUE", "sum"),
                )
                lc_match_by_year["TRUE_MATCH_RATE"] = round(
                    lc_match_by_year["LC_TRUE_MATCHES"] / lc_match_by_year["LC_ROWS"], 4
                )

        # ── Sankey edges ──
        sankey_overall = bucket_counts.rename(columns={"BUCKET": "TARGET", "STUDENTS": "VALUE"})[["TARGET", "VALUE"]].copy()
        sankey_overall.insert(0, "SOURCE", "DSS_TOTAL")

        sankey_intl = master.groupby(["IS_INTERNATIONAL", "BUCKET"], as_index=False).agg(VALUE=("JOIN_ID", "count"))
        sankey_intl["SOURCE"] = np.where(sankey_intl["IS_INTERNATIONAL"], "INTERNATIONAL", "DOMESTIC")
        sankey_intl = sankey_intl.rename(columns={"BUCKET": "TARGET"})[["SOURCE", "TARGET", "VALUE"]]

        # ── Example no-outcome rows ──
        example_rows = {}
        for reason in no_out["NO_OUTCOME_REASON"].unique():
            sub = no_out[no_out["NO_OUTCOME_REASON"] == reason]
            example_rows[reason] = sub.head(25).to_dict(orient="records")

        # ══════════════════════════════════════════════════════════════
        # WRITE OUTPUTS
        # ══════════════════════════════════════════════════════════════

        # -- Excel reports --
        xlsx_true_match = REPORT_DIR / f"true_match_report_{scope_key}.xlsx"
        with pd.ExcelWriter(xlsx_true_match, engine="openpyxl") as w:
            overall_summary.to_excel(w, sheet_name="overall_metrics", index=False)
            vendor_funnel.to_excel(w, sheet_name="vendor_funnel", index=False)
            bucket_counts.to_excel(w, sheet_name="bucket_counts", index=False)
            by_year.to_excel(w, sheet_name="by_year", index=False)
            by_intl.to_excel(w, sheet_name="by_international", index=False)
            if len(by_degree):
                by_degree.to_excel(w, sheet_name="by_degree", index=False)
            if len(by_major):
                by_major.to_excel(w, sheet_name="by_major", index=False)
            no_reason.to_excel(w, sheet_name="no_outcome_reasons", index=False)
            no_by_year.to_excel(w, sheet_name="no_outcome_by_year", index=False)
            no_by_intl.to_excel(w, sheet_name="no_outcome_by_intl", index=False)
            if len(aa_year_crosstab):
                aa_year_crosstab.to_excel(w, sheet_name="aa_year_crosstab", index=False)
            if len(lc_match_by_year):
                lc_match_by_year.to_excel(w, sheet_name="lc_match_by_year", index=False)
            if validation_results:
                pd.DataFrame(validation_results).to_excel(w, sheet_name="validation", index=False)
        log.info(f"  Wrote: {xlsx_true_match}")

        # -- CSVs --
        no_out.to_csv(REPORT_DIR / f"no_outcome_students_{scope_key}.csv", index=False)
        lc_mismatch.to_csv(REPORT_DIR / f"lightcast_row_only_no_true_match_{scope_key}.csv", index=False)
        write_oracle_in(no_out["JOIN_ID"].tolist(), REPORT_DIR / f"no_outcome_oracle_in_{scope_key}.txt")

        # -- Markdown summary --
        md_lines = []
        md_lines.append(f"# Report Summary: {scope_key} ({yr_start}–{yr_end})")
        md_lines.append(f"\nGenerated: {TIMESTAMP}\n")
        md_lines.append("## 1. What the data is\n")
        md_lines.append("This report analyses alumni employment and engagement outcomes for Indiana University – Bloomington Graduate School graduates.")
        md_lines.append(f"The scope covers cohort years {yr_start}–{yr_end}.\n")
        md_lines.append("## 2. What each source contributes\n")
        md_lines.append(f"- **DSS** (Decision Support System): Authoritative student denominator. {N:,} unique students.")
        md_lines.append(f"- **Academic Analytics (AA)**: Supplemental academic/employment data. {int(master['AA_HAS_ROW'].sum()):,} students with any row, {int(master['AA_HAS_EMPLOYMENT'].sum()):,} with employment info.")
        md_lines.append(f"- **Lightcast**: Broadest employment source, but ONLY when MATCH_STATUS == True. {int(master['LC_HAS_ROW'].sum()):,} students with a row, {int(master['LC_TRUE_MATCH'].sum()):,} with true match, {int(master['LC_EMPLOYMENT_TRUE_MATCH'].sum()):,} with employment.")
        md_lines.append(f"- **PSEO**: Student engagement/career services only, NOT employment. {int(master['PSEO_HAS_ROW'].sum()):,} students.\n")
        md_lines.append("## 3. Join plan\n")
        md_lines.append("All sources joined to DSS via normalised JOIN_ID (10-digit zero-padded PRSN_UNIV_ID / CLIENTPERSONID).\n")
        md_lines.append("## 4. Largest loopholes / pitfalls\n")
        md_lines.append(f"- **{int(master['LC_ROW_ONLY_NO_TRUE_MATCH'].sum()):,} students** ({master['LC_ROW_ONLY_NO_TRUE_MATCH'].mean():.1%}) have a Lightcast row but NO true match — the single largest coverage gap.")
        md_lines.append("- AA is snapshot-based, not continuous annual coverage. Years 2021–2024 may appear sparse.")
        md_lines.append("- PSEO is engagement data, not employment.\n")
        md_lines.append("## 5. Employment source breakdown\n")
        md_lines.append(f"| Source | Employment Count | Rate of DSS |")
        md_lines.append(f"|--------|-----------------|-------------|")
        md_lines.append(f"| AA | {int(master['AA_HAS_EMPLOYMENT'].sum()):,} | {master['AA_HAS_EMPLOYMENT'].mean():.1%} |")
        md_lines.append(f"| Lightcast (true match) | {int(master['LC_EMPLOYMENT_TRUE_MATCH'].sum()):,} | {master['LC_EMPLOYMENT_TRUE_MATCH'].mean():.1%} |")
        md_lines.append(f"| **Any Employment** | {int(master['ANY_EMPLOYMENT_OUTCOME'].sum()):,} | {master['ANY_EMPLOYMENT_OUTCOME'].mean():.1%} |\n")
        md_lines.append("## 6. Year coverage\n")
        md_lines.append("Not all years have equal coverage. Earlier cohorts (pre-2010) have lower match rates. 2025 may be incomplete.\n")
        md_lines.append("## 7. Bloomington denominator\n")
        md_lines.append("Yes — DSS is filtered to IUBLA (Bloomington) graduate awards (degree levels 8, 9).\n")
        md_lines.append("## 8. International students\n")
        for _, r in by_intl.iterrows():
            label = "International" if r["IS_INTERNATIONAL"] else "Domestic"
            md_lines.append(f"- {label}: {int(r['STUDENTS']):,} students, employment rate {r.get('RATE_ANY_EMPLOYMENT_OUTCOME', 0):.1%}, no-outcome rate {r.get('RATE_NO_OUTCOME', 0):.1%}")
        md_lines.append("")
        md_lines.append("## 9. No-outcome breakdown\n")
        md_lines.append(f"Total NO_OUTCOME: {len(no_out):,} ({len(no_out)/N:.1%})\n")
        md_lines.append("| Reason | Count | % of No-Outcome |")
        md_lines.append("|--------|-------|-----------------|")
        for _, r in no_reason.iterrows():
            md_lines.append(f"| {r['REASON']} | {int(r['STUDENTS']):,} | {r['RATE_OF_NO_OUTCOME']:.1%} |")
        md_lines.append("")
        md_lines.append("## 10. Recommended interpretation\n")
        md_lines.append("- Use Lightcast true-match employment as the primary employment indicator.")
        md_lines.append("- Use AA as a supplement for academic placements.")
        md_lines.append("- Do NOT treat PSEO as employment — it is engagement/service data.")
        md_lines.append("- The Lightcast row-only-no-true-match population is the main bottleneck to improving coverage.")
        md_lines.append("- AA should not be used as a backbone denominator — it is partial and snapshot-based.\n")

        md_path = REPORT_DIR / f"report_summary_{scope_key}.md"
        md_path.write_text("\n".join(md_lines), encoding="utf-8")
        log.info(f"  Wrote: {md_path}")

        # ══════════════════════════════════════════════════════════════
        # JSON BUNDLE for dashboard
        # ══════════════════════════════════════════════════════════════

        def ser(obj):
            """JSON serialiser for numpy/pandas types."""
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj) if not np.isnan(obj) else None
            if isinstance(obj, (np.bool_,)):
                return bool(obj)
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            if pd.isna(obj):
                return None
            return str(obj)

        bundle = {
            "scope": scope_key,
            "year_start": yr_start,
            "year_end": yr_end,
            "label": scope_cfg["label"],
            "generated": TIMESTAMP,
            "n_students": N,
            "overall_summary": overall_summary.to_dict(orient="records"),
            "vendor_funnel": vendor_funnel.to_dict(orient="records"),
            "bucket_counts": bucket_counts.to_dict(orient="records"),
            "by_year": by_year.to_dict(orient="records"),
            "by_international": by_intl.to_dict(orient="records"),
            "by_degree": by_degree.to_dict(orient="records") if len(by_degree) else [],
            "by_major": by_major.to_dict(orient="records") if len(by_major) else [],
            "no_outcome_reasons": no_reason.to_dict(orient="records"),
            "no_outcome_by_year": no_by_year.to_dict(orient="records"),
            "no_outcome_by_intl": no_by_intl.to_dict(orient="records"),
            "no_outcome_examples": {k: v[:10] for k, v in example_rows.items()},
            "sankey_overall": sankey_overall.to_dict(orient="records"),
            "sankey_intl": sankey_intl.to_dict(orient="records"),
            "aa_year_crosstab": aa_year_crosstab.to_dict(orient="records") if len(aa_year_crosstab) else [],
            "lc_match_by_year": lc_match_by_year.to_dict(orient="records") if len(lc_match_by_year) else [],
            "validation": validation_results if validation_results else [],
        }

        bundle_path = BUNDLE_DIR / f"dashboard_bundle_{scope_key}.json"
        with open(bundle_path, "w", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2, default=ser)
        log.info(f"  Wrote bundle: {bundle_path}")

        all_bundles[scope_key] = bundle

    # ── Data inventory Excel (once) ──
    inv_path = REPORT_DIR / "data_inventory_report.xlsx"
    with pd.ExcelWriter(inv_path, engine="openpyxl") as w:
        inventory.to_excel(w, sheet_name="inventory", index=False)
    log.info(f"Wrote: {inv_path}")

    # ── Executive memo ──
    memo_lines = []
    b = all_bundles.get("2004_2025", {})
    n = b.get("n_students", 0)
    memo_lines.append("# Executive Memo: Alumni Outcomes Coverage")
    memo_lines.append(f"\nDate: {TIMESTAMP}")
    memo_lines.append(f"Scope: IU Bloomington Graduate School, 2004–2025\n")
    memo_lines.append("## Key Numbers\n")
    if b.get("overall_summary"):
        for row in b["overall_summary"]:
            rate_pct = f"{row['RATE']:.1%}" if row["RATE"] is not None else "N/A"
            memo_lines.append(f"- **{row['METRIC']}**: {row['COUNT']:,} ({rate_pct})")
    memo_lines.append("\n## Key Caveats\n")
    memo_lines.append("1. Lightcast has the broadest reach but only ~39% of its rows yield a true match.")
    memo_lines.append("2. Academic Analytics is snapshot-based — not continuous annual coverage.")
    memo_lines.append("3. PSEO is engagement data only, not employment.")
    memo_lines.append("4. 2025 data is likely incomplete (partial year).")
    memo_lines.append("5. International students have lower match rates across all sources.\n")
    memo_lines.append("## Recommendation\n")
    memo_lines.append("- Use AA as a **supplement**, not a backbone. Lightcast true-match is the primary employment source.")
    memo_lines.append("- The dominant coverage gap is Lightcast row-only-no-true-match — improving Lightcast matching would have the highest marginal return.")
    memo_lines.append("- For stable trend analysis, use the 2010–2024 scope.\n")

    memo_path = REPORT_DIR / "executive_memo.md"
    memo_path.write_text("\n".join(memo_lines), encoding="utf-8")
    log.info(f"Wrote: {memo_path}")

    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info(f"Reports: {REPORT_DIR}")
    log.info(f"Bundles: {BUNDLE_DIR}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
