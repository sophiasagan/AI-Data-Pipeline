import logging

import great_expectations as ge
import pandas as pd

logger = logging.getLogger("validate")


def _check(label: str, result) -> bool:
    """Log PASS/FAIL for one GE expectation result and return success bool."""
    passed = result.success
    if passed:
        logger.info("PASS  %s", label)
    else:
        rc = result.result or {}
        n_bad = rc.get("unexpected_count", "?")
        pct = rc.get("unexpected_percent") or 0.0
        logger.warning("FAIL  %s  (%s unexpected rows, %.1f%%)", label, n_bad, pct)
    return passed


def _safe_check(label: str, gdf, method: str, *args, **kwargs) -> bool:
    """Run a GE expectation, converting any error into a logged FAIL."""
    try:
        result = getattr(gdf, method)(*args, **kwargs)
        return _check(label, result)
    except Exception as exc:
        logger.error("ERROR  %s — %s", label, exc)
        return False


def validate_features(df: pd.DataFrame) -> bool:
    """Run GE assertions against the feature DataFrame.

    Returns True only if every applicable assertion passes.
    """
    logger.info("validate_features: %d rows, columns: %s", len(df), list(df.columns))
    gdf = ge.from_pandas(df)
    results: list[bool] = []

    # MemberID: no nulls and unique
    results.append(_safe_check(
        "MemberID — no nulls",
        gdf, "expect_column_values_to_not_be_null", "MemberID",
    ))
    results.append(_safe_check(
        "MemberID — unique",
        gdf, "expect_column_values_to_be_unique", "MemberID",
    ))

    # tenure_years >= 0 (negative tenure is impossible)
    results.append(_safe_check(
        "tenure_years >= 0",
        gdf, "expect_column_values_to_be_between", "tenure_years",
        min_value=0,
    ))

    # product_count: realistic credit union range
    results.append(_safe_check(
        "product_count in [1, 15]",
        gdf, "expect_column_values_to_be_between", "product_count",
        min_value=1, max_value=15,
    ))

    # churn_score: only asserted when the column is present (optional downstream feature)
    if "churn_score" in df.columns:
        results.append(_safe_check(
            "churn_score in [0.0, 1.0]",
            gdf, "expect_column_values_to_be_between", "churn_score",
            min_value=0.0, max_value=1.0,
        ))
    else:
        logger.info("SKIP  churn_score in [0.0, 1.0]  (column not present)")

    n_pass = sum(results)
    n_total = len(results)
    overall = "PASS" if all(results) else "FAIL"
    logger.info("validate_features: overall %s  (%d/%d checks passed)", overall, n_pass, n_total)
    return all(results)
