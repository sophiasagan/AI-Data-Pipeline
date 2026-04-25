import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger("transform")

_DIRECT_DEPOSIT_RE = r"PAYROLL|DIRECT\s+DEP|DIR\s*DEP|ACH\s+CREDIT"
_NSF_RE = r"NSF|NON.SUFFICIENT|INSUFFICIENT\s+FUNDS|OVERDRAFT\s+FEE"

_NUMERIC_FEATURES = [
    "tenure_years",
    "product_count",
    "login_freq_90d",
    "avg_balance_trend",
    "nsf_count_6m",
    "debit_swipe_delta",
]
_FLAG_FEATURES = ["has_direct_deposit"]

_OUTPUT_COLS = [
    "MemberID",
    "tenure_years",
    "product_count",
    "has_direct_deposit",
    "login_freq_90d",
    "avg_balance_trend",
    "nsf_count_6m",
    "debit_swipe_delta",
    "feature_date",
    "pulled_at",
]


def _to_utc(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    return df


def _to_num(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def build_member_features(
    members: pd.DataFrame,
    accounts: pd.DataFrame,
    transactions: pd.DataFrame,
    logins: pd.DataFrame | None = None,
    transactions_6m: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return one row per member with engineered features.

    members:         pull_members()
    accounts:        pull_accounts()
    transactions:    pull_transactions(days=90)  — covers features 3, 5, 7
    logins:          optional [MemberID, LoginDate]; login_freq_90d is NA if omitted
    transactions_6m: pull_transactions(days=180) for nsf_count_6m;
                     falls back to transactions with a warning if omitted
    """
    ref_date = pd.Timestamp.now(tz="UTC")
    logger.info(
        "build_member_features: %d members, %d accounts, %d transactions",
        len(members), len(accounts), len(transactions),
    )

    members = _to_utc(members.copy(), "MembershipOpenDate")
    accounts = _to_num(accounts.copy(), "CurrentBalance")
    txn = _to_utc(
        _to_num(transactions.copy(), "Amount", "RunningBalance"),
        "TransactionDate",
    )

    # Index on MemberID so every intermediate Series aligns on the same axis.
    base = members.set_index("MemberID")
    parts: dict[str, pd.Series] = {}

    # 1. tenure_years ---------------------------------------------------------
    parts["tenure_years"] = (
        (ref_date - base["MembershipOpenDate"]).dt.days / 365.25
    ).round(4)

    # 2. product_count --------------------------------------------------------
    parts["product_count"] = (
        accounts.groupby("MemberID")["AccountTypeCode"]
        .nunique()
        .rename("product_count")
    )

    # 3. has_direct_deposit ---------------------------------------------------
    # ACH credits or descriptions that match common payroll/DD patterns.
    is_credit = txn["Amount"] > 0
    is_ach_channel = txn["ChannelCode"].eq("ACH")
    is_payroll_desc = txn["Description"].str.contains(
        _DIRECT_DEPOSIT_RE, case=False, na=False, regex=True
    )
    parts["has_direct_deposit"] = (
        txn[is_credit & (is_ach_channel | is_payroll_desc)]
        .groupby("MemberID")
        .size()
        .gt(0)
        .rename("has_direct_deposit")
    )

    # 4. login_freq_90d -------------------------------------------------------
    if logins is not None:
        lgn = _to_utc(logins.copy(), "LoginDate")
        cutoff_90 = ref_date - pd.Timedelta(days=90)
        parts["login_freq_90d"] = (
            lgn[lgn["LoginDate"] >= cutoff_90]
            .groupby("MemberID")
            .size()
            .rename("login_freq_90d")
        )
    else:
        logger.warning(
            "login_freq_90d: logins DataFrame not supplied — column will be NA; "
            "pass a DataFrame with [MemberID, LoginDate] to populate"
        )
        parts["login_freq_90d"] = pd.Series(dtype="Int64", name="login_freq_90d")

    # 5. avg_balance_trend ----------------------------------------------------
    # Estimate each account's balance at the start of the 90-day window as
    # the RunningBalance of its earliest transaction minus that transaction's
    # Amount.  Accounts with zero transactions in the window are assumed
    # unchanged (bal_start = CurrentBalance), so they contribute 0 to the trend.
    earliest_per_acct = (
        txn.sort_values("TransactionDate")
        .groupby("AccountID", sort=False)
        .first()
        .reset_index()[["AccountID", "MemberID", "RunningBalance", "Amount"]]
    )
    earliest_per_acct["bal_start"] = (
        earliest_per_acct["RunningBalance"] - earliest_per_acct["Amount"]
    )

    accts_bal = accounts[["AccountID", "MemberID", "CurrentBalance"]].copy()
    accts_bal = accts_bal.merge(
        earliest_per_acct[["AccountID", "bal_start"]], on="AccountID", how="left"
    )
    accts_bal["bal_start"] = accts_bal["bal_start"].fillna(accts_bal["CurrentBalance"])

    current_total = accts_bal.groupby("MemberID")["CurrentBalance"].sum()
    start_total = accts_bal.groupby("MemberID")["bal_start"].sum()
    parts["avg_balance_trend"] = (
        (current_total - start_total).round(2).rename("avg_balance_trend")
    )

    # 6. nsf_count_6m ---------------------------------------------------------
    if transactions_6m is not None:
        txn_nsf = _to_utc(
            _to_num(transactions_6m.copy(), "Amount"), "TransactionDate"
        )
        logger.info("nsf_count_6m: using dedicated 6-month transaction window")
    else:
        txn_nsf = txn
        logger.warning(
            "nsf_count_6m: transactions_6m not supplied — using 90-day window; "
            "pass pull_transactions(days=180) for accurate 6-month NSF counts"
        )
    nsf_mask = txn_nsf["Description"].str.contains(
        _NSF_RE, case=False, na=False, regex=True
    )
    parts["nsf_count_6m"] = (
        txn_nsf[nsf_mask]
        .groupby("MemberID")
        .size()
        .rename("nsf_count_6m")
    )

    # 7. debit_swipe_delta ----------------------------------------------------
    # Positive delta → member is swiping more; negative → pulling back.
    cutoff_30 = ref_date - pd.Timedelta(days=30)
    cutoff_60 = ref_date - pd.Timedelta(days=60)
    pos_debits = txn[(txn["Amount"] < 0) & (txn["ChannelCode"] == "POS")]

    last_30 = (
        pos_debits[pos_debits["TransactionDate"] >= cutoff_30]
        .groupby("MemberID")
        .size()
        .rename("debit_last_30")
    )
    prior_30 = (
        pos_debits[
            (pos_debits["TransactionDate"] >= cutoff_60)
            & (pos_debits["TransactionDate"] < cutoff_30)
        ]
        .groupby("MemberID")
        .size()
        .rename("debit_prior_30")
    )
    swipe_df = last_30.to_frame().join(prior_30, how="outer").fillna(0)
    parts["debit_swipe_delta"] = (
        (swipe_df["debit_last_30"] - swipe_df["debit_prior_30"])
        .astype(int)
        .rename("debit_swipe_delta")
    )

    # -- assemble -------------------------------------------------------------
    feature_df = pd.concat(parts.values(), axis=1)
    result = base.join(feature_df, how="left").reset_index()

    # -- imputation -----------------------------------------------------------
    for col in _NUMERIC_FEATURES:
        n_null = int(result[col].isna().sum())
        if n_null == 0:
            continue
        median_val = result[col].median()
        if pd.isna(median_val):
            # Entire column is null (e.g., logins DataFrame not supplied).
            logger.warning(
                "impute %s: all %d values null, no median available — column left as NA",
                col, n_null,
            )
            continue
        result[col] = result[col].fillna(median_val)
        logger.info("impute %s: %d nulls → median %g", col, n_null, median_val)

    for col in _FLAG_FEATURES:
        n_null = int(result[col].isna().sum())
        if n_null == 0:
            continue
        result[col] = result[col].fillna(False).astype(bool)
        logger.info("impute %s: %d nulls → False", col, n_null)

    now = datetime.now(timezone.utc)
    result["feature_date"] = now.date().isoformat()
    result["pulled_at"] = now.isoformat()

    result = result[_OUTPUT_COLS]
    logger.info(
        "build_member_features: output %d rows × %d feature columns",
        len(result),
        len(_NUMERIC_FEATURES) + len(_FLAG_FEATURES),
    )
    return result
