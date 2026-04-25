# cu_pipeline

AI-ready ETL pipeline for credit union member data. Pulls raw records from SQL Server, engineers a member feature store, validates quality, and writes versioned parquet files for downstream ML models.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SQL Server (dbo.*)                                                     │
│                                                                         │
│  tblMember ──────┐                                                      │
│  tblAccount ─────┼──► sql/*.sql  (date-windowed queries)               │
│  tblTransaction ─┘                                                      │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ pyodbc + SQLAlchemy
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  pipeline/extract.py                                                    │
│                                                                         │
│  pull_members()          → data/raw/members_YYYYMMDD.csv               │
│  pull_accounts()         → data/raw/accounts_YYYYMMDD.csv              │
│  pull_transactions(90d)  → data/raw/transactions_YYYYMMDD.csv          │
│  pull_transactions(180d) → data/raw/transactions_YYYYMMDD.csv          │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ pandas DataFrames
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  pipeline/transform.py                                                  │
│                                                                         │
│  build_member_features()                                                │
│                                                                         │
│  tenure_years        days since membership open / 365.25               │
│  product_count       distinct account types per member                  │
│  has_direct_deposit  any ACH credit or payroll pattern in 90d          │
│  login_freq_90d      online banking logins in 90d (optional input)     │
│  avg_balance_trend   Σ current_balance − Σ balance_90d_ago             │
│  nsf_count_6m        NSF/overdraft fee transactions in 180d            │
│  debit_swipe_delta   POS debits last 30d − prior 30d                   │
│                                                                         │
│  Nulls: numeric → median imputation  |  flags → False                  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ member_features DataFrame (1 row/member)
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  pipeline/load.py                                                       │
│                                                                         │
│  write_features()                                                       │
│    data/features/member_features_YYYYMMDD.parquet  ← versioned         │
│    data/features/member_features_latest.parquet    ← always current    │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  pipeline/validate.py  (Great Expectations)                             │
│                                                                         │
│  MemberID — no nulls                                                    │
│  MemberID — unique                                                      │
│  tenure_years >= 0                                                      │
│  product_count in [1, 15]                                               │
│  churn_score in [0.0, 1.0]  (when column is present)                   │
│                                                                         │
│  PASS/FAIL per assertion → pipeline.log                                 │
└─────────────────────────────────────────────────────────────────────────┘

Orchestration: pipeline/scheduler.py (APScheduler, 02:00 UTC nightly)
All stage output and errors → pipeline.log
```

---

## Project structure

```
cu_pipeline/
├── pipeline/
│   ├── extract.py       SQL Server connections + raw CSV pulls
│   ├── transform.py     Feature engineering (one row per member)
│   ├── load.py          Parquet feature store writer
│   ├── validate.py      Great Expectations quality assertions
│   └── scheduler.py     APScheduler entry point
├── sql/
│   ├── members.sql      Active members with demographics + credit profile
│   ├── accounts.sql     Open share/loan accounts with balances
│   └── transactions.sql 90-day (or N-day) transaction history
├── data/
│   ├── raw/             Datestamped CSVs from each extract run
│   └── features/        Versioned parquet outputs
├── pipeline.log         Runtime log (all stages)
├── .env                 DB_CONNECTION_STRING (never commit)
├── requirements.txt
└── README.md
```

---

## Setup

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

Dependencies include: `sqlalchemy`, `pyodbc`, `pandas`, `pyarrow`, `great-expectations`, `apscheduler`, `python-dotenv`.

**2. Configure the database connection**

Copy `.env.example` to `.env` and fill in your SQL Server ODBC string:

```
DB_CONNECTION_STRING=Driver={ODBC Driver 17 for SQL Server};Server=<host>;Database=<db>;Trusted_Connection=yes;
```

The connection string is passed as a raw ODBC string and URL-encoded internally by `extract.py`. Username/password auth is also supported:

```
DB_CONNECTION_STRING=Driver={ODBC Driver 17 for SQL Server};Server=<host>;Database=<db>;UID=<user>;PWD=<pass>;
```

---

## Usage

**Run the full pipeline once and exit:**

```bash
python -m pipeline.scheduler --run-now
```

**Start the nightly scheduler (blocks, runs at 02:00 UTC):**

```bash
python -m pipeline.scheduler
```

**Run individual stages in a Python session:**

```python
from pipeline.extract import pull_members, pull_accounts, pull_transactions
from pipeline.transform import build_member_features
from pipeline.load import write_features
from pipeline.validate import validate_features

members      = pull_members()
accounts     = pull_accounts()
transactions = pull_transactions(days=90)
txn_6m       = pull_transactions(days=180)

features = build_member_features(members, accounts, transactions,
                                  transactions_6m=txn_6m)
write_features(features)
validate_features(features)
```

**Pass online banking logins for `login_freq_90d`:**

```python
# logins must have columns: MemberID, LoginDate
features = build_member_features(members, accounts, transactions,
                                  logins=logins_df, transactions_6m=txn_6m)
```

If `logins` is omitted, `login_freq_90d` is left as `NA` in the feature store and a warning is written to `pipeline.log`. If `transactions_6m` is omitted, `nsf_count_6m` is computed from the 90-day window with a warning.

---

## Feature store schema

| Column | Type | Description |
|---|---|---|
| `MemberID` | int | Primary key — matches source system |
| `tenure_years` | float | Years since membership open date |
| `product_count` | int | Count of distinct account types held |
| `has_direct_deposit` | bool | Any ACH payroll credit in last 90 days |
| `login_freq_90d` | int / NA | Online banking logins in last 90 days |
| `avg_balance_trend` | float | Total balance change (current − 90d ago), dollars |
| `nsf_count_6m` | int | NSF / overdraft fee events in last 180 days |
| `debit_swipe_delta` | int | POS debit count: last 30d minus prior 30d |
| `feature_date` | str | ISO date the feature run executed (`YYYY-MM-DD`) |
| `pulled_at` | str | ISO-8601 UTC timestamp of the run |

**Null handling:** numeric features with missing values are imputed with the column median; boolean flags default to `False`. When a full column is null (e.g., `login_freq_90d` without a logins source), it is left as `NA` rather than silently imputed with an uninformative median.

**Versioning:** each run writes `member_features_YYYYMMDD.parquet` and overwrites `member_features_latest.parquet` (byte-identical copy). Historical files are never overwritten.

---

## Data conventions

- All monetary amounts stored as `DECIMAL(15,2)` — never cast to `float` in SQL or Python
- Queries always use date-windowed `WHERE` clauses — full table scans are not permitted
- NSF detection uses description regex: `NSF | NON-SUFFICIENT | INSUFFICIENT FUNDS | OVERDRAFT FEE`
- Direct deposit detection: `ChannelCode = ACH` with positive amount, or description matching `PAYROLL | DIRECT DEP | DIR DEP | ACH CREDIT`
- POS debit swipes: `ChannelCode = POS` with negative amount

---

## Monitoring

All pipeline output goes to `pipeline.log`. Each run produces entries like:

```
2026-04-25 02:00:01 INFO  scheduler: === pipeline run start ===
2026-04-25 02:00:03 INFO  extract: pull_members: 18432 rows → data/raw/members_20260425.csv
2026-04-25 02:00:11 INFO  transform: build_member_features: 18432 members, 7 feature columns
2026-04-25 02:00:11 INFO  transform: impute login_freq_90d: 3201 nulls → median 4
2026-04-25 02:00:12 INFO  load: 18432 members written → data/features/member_features_20260425.parquet (2.1 MB)
2026-04-25 02:00:14 INFO  validate: PASS  MemberID — no nulls
2026-04-25 02:00:14 INFO  validate: PASS  tenure_years >= 0
2026-04-25 02:00:14 INFO  validate: overall PASS  (4/4 checks passed)
2026-04-25 02:00:14 INFO  scheduler: === pipeline run complete in 13.2s ===
```

A `FAIL` on any validation assertion is logged at `WARNING` level with an unexpected row count and percentage. Pipeline errors are logged at `ERROR` with a full traceback.
