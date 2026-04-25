import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

load_dotenv()

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("extract")

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

SQL_DIR = Path("sql")


def _get_engine():
    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("DB_CONNECTION_STRING is not set in environment")
    # SQLAlchemy expects the raw pyodbc connection string wrapped as:
    # mssql+pyodbc:///?odbc_connect=<url-encoded-string>
    from urllib.parse import quote_plus
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
        fast_executemany=True,
    )
    return engine


def _read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


def _save_raw(df: pd.DataFrame, name: str) -> Path:
    date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = RAW_DIR / f"{name}_{date_tag}.csv"
    df.to_csv(path, index=False)
    return path


def pull_members() -> pd.DataFrame:
    logger.info("pull_members: starting")
    try:
        engine = _get_engine()
        query = _read_sql("members.sql")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        df["pulled_at"] = datetime.now(timezone.utc).isoformat()
        path = _save_raw(df, "members")
        logger.info("pull_members: %d rows → %s", len(df), path)
        return df
    except (OperationalError, SQLAlchemyError, pyodbc.Error) as exc:
        logger.error("pull_members: connection error — %s", exc)
        raise
    except Exception as exc:
        logger.error("pull_members: unexpected error — %s", exc)
        raise


def pull_accounts() -> pd.DataFrame:
    logger.info("pull_accounts: starting")
    try:
        engine = _get_engine()
        query = _read_sql("accounts.sql")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        df["pulled_at"] = datetime.now(timezone.utc).isoformat()
        path = _save_raw(df, "accounts")
        logger.info("pull_accounts: %d rows → %s", len(df), path)
        return df
    except (OperationalError, SQLAlchemyError, pyodbc.Error) as exc:
        logger.error("pull_accounts: connection error — %s", exc)
        raise
    except Exception as exc:
        logger.error("pull_accounts: unexpected error — %s", exc)
        raise


def pull_transactions(days: int = 90) -> pd.DataFrame:
    logger.info("pull_transactions: starting (days=%d)", days)
    try:
        engine = _get_engine()
        query = _read_sql("transactions.sql")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"days": days})
        df["pulled_at"] = datetime.now(timezone.utc).isoformat()
        path = _save_raw(df, "transactions")
        logger.info("pull_transactions: %d rows → %s", len(df), path)
        return df
    except (OperationalError, SQLAlchemyError, pyodbc.Error) as exc:
        logger.error("pull_transactions: connection error — %s", exc)
        raise
    except Exception as exc:
        logger.error("pull_transactions: unexpected error — %s", exc)
        raise
