import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger("load")

FEATURES_DIR = Path("data/features")
FEATURES_DIR.mkdir(parents=True, exist_ok=True)

_METADATA_COLS = {"MemberID", "feature_date", "pulled_at"}


def _human_size(n_bytes: int) -> str:
    if n_bytes >= 1_048_576:
        return f"{n_bytes / 1_048_576:.1f} MB"
    return f"{n_bytes / 1_024:.1f} KB"


def write_features(df: pd.DataFrame) -> Path:
    """Write feature DataFrame to dated and latest parquet files.

    Returns the path of the dated file.
    """
    if df.empty:
        raise ValueError("write_features: DataFrame is empty — aborting write")

    date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
    dated_path = FEATURES_DIR / f"member_features_{date_tag}.parquet"
    latest_path = FEATURES_DIR / "member_features_latest.parquet"

    df.to_parquet(dated_path, index=False, engine="pyarrow")
    shutil.copy2(dated_path, latest_path)

    dated_size = _human_size(dated_path.stat().st_size)
    feature_cols = [c for c in df.columns if c not in _METADATA_COLS]

    logger.info("load: %d members written → %s (%s)", len(df), dated_path, dated_size)
    logger.info("load: also written → %s", latest_path)
    logger.info("load: feature columns (%d): %s", len(feature_cols), ", ".join(feature_cols))

    return dated_path
