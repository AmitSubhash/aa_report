# IU Bloomington Graduate School — Alumni Outcomes Analytics

Production-quality, non-destructive alumni outcomes system using DSS as the authoritative denominator, Lightcast as the broadest employment source (true-match only), Academic Analytics as supplemental, and PSEO as engagement data.

## Quick Start

### 1. Install dependencies

```bash
pip install pandas numpy openpyxl
```

### 2. Place raw data files


If files are elsewhere under the project root, the script will find them recursively and log the resolved paths.

### 3. Run preprocessing

```bash
python scripts/build_dashboard_data.py
```

This creates:
- `reports/<timestamp>/` — Excel reports, Markdown summaries, CSV exports, Oracle IN-clause files
- `data/bundles/` — JSON bundles for the dashboard (one per scope)

Each run creates a **new timestamped folder** — nothing is overwritten or deleted.

### 4. Open the dashboard

Open `dashboard/index.html` in any modern browser. Click **Load JSON Bundle** and select one or more `dashboard_bundle_*.json` files from `data/bundles/`.

No server, no npm, no build step required. Just a browser.

## Project Structure

```
├── scripts/
│   └── build_dashboard_data.py    # Main preprocessing pipeline
├── dashboard/
│   └── index.html                 # Self-contained dashboard (inline CSS+JS)
├── data/
│   ├── raw/                       # Raw source CSVs (not committed)
│   └── bundles/                   # Precomputed JSON bundles for dashboard
├── reports/
│   └── <timestamp>/               # Timestamped run outputs
│       ├── data_inventory_report.xlsx
│       ├── true_match_report_2004_2025.xlsx
│       ├── true_match_report_2010_2025.xlsx
│       ├── true_match_report_2010_2024.xlsx
│       ├── report_summary_2004_2025.md
│       ├── report_summary_2010_2025.md
│       ├── report_summary_2010_2024.md
│       ├── executive_memo.md
│       ├── no_outcome_students_*.csv
│       ├── lightcast_row_only_no_true_match_*.csv
│       ├── no_outcome_oracle_in_*.txt
│       └── run.log
└── README.md
```

## Scopes

| Scope | Years | Purpose |
|-------|-------|---------|
| `2004_2025` | 2004–2025 | Full historical scope |
| `2010_2025` | 2010–2025 | Modern scope |
| `2010_2024` | 2010–2024 | Stable trend scope (excludes incomplete 2025) |

## Data Source Summary

| Source | Role | ID Column | Key Rule |
|--------|------|-----------|----------|
| DSS | Authoritative denominator | PRSN_UNIV_ID | Degree levels 8,9; checkout AW |
| Academic Analytics | Supplemental employment | CLIENTPERSONID | Snapshot-based, NOT annual |
| Lightcast | Primary employment | PRSN_UNIV_ID | **MATCH_STATUS must be True** |
| PSEO | Engagement only | PRSN_UNIV_ID | NOT employment data |

## Refresh Workflow

1. Replace/update CSVs in `data/raw/`
2. Run `python scripts/build_dashboard_data.py`
3. Load new bundles from `data/bundles/` in the dashboard

Previous reports remain intact in their timestamped folders.

## Known Caveats

- **Lightcast row ≠ true match**: ~60% of Lightcast rows have `MATCH_STATUS = False`. Only true matches count.
- **AA is snapshot-based**: Not continuous annual coverage. Years 2021–2024 may appear sparse.
- **PSEO is engagement only**: Career services interaction, not employment.
- **2025 is likely incomplete**: Partial-year data.
- **International students**: Lower match rates across all vendor sources.
- **Multiple DSS records per student**: Resolved by taking the maximum cohort year as PRIMARY_COHORT_YEAR.
