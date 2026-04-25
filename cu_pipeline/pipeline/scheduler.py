"""
Entry point for the nightly ETL pipeline.

  Run once now:  python -m pipeline.scheduler --run-now
  Run on schedule (2am nightly):  python -m pipeline.scheduler
"""
import argparse
import logging
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# Configure root logger before any pipeline imports so all child loggers
# inherit this handler (extract, transform, load, validate).
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("scheduler")

from cu_pipeline.pipeline.extract import pull_members, pull_accounts, pull_transactions  # noqa: E402
from cu_pipeline.pipeline.transform import build_member_features  # noqa: E402
from cu_pipeline.pipeline.load import write_features  # noqa: E402
from cu_pipeline.pipeline.validate import validate_features  # noqa: E402


def run_pipeline() -> None:
    start = datetime.now(timezone.utc)
    logger.info("=== pipeline run start ===")

    try:
        # -- Extract ----------------------------------------------------------
        logger.info("stage: extract")
        members = pull_members()
        accounts = pull_accounts()
        transactions = pull_transactions(days=90)
        transactions_6m = pull_transactions(days=180)

        # -- Transform --------------------------------------------------------
        logger.info("stage: transform")
        features = build_member_features(
            members,
            accounts,
            transactions,
            transactions_6m=transactions_6m,
        )

        # -- Load -------------------------------------------------------------
        logger.info("stage: load")
        write_features(features)

        # -- Validate ---------------------------------------------------------
        logger.info("stage: validate")
        passed = validate_features(features)
        if not passed:
            logger.warning("pipeline: validation failed — check assertions above")

    except Exception as exc:
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.error(
            "=== pipeline run FAILED after %.1fs: %s ===",
            elapsed, exc,
            exc_info=True,
        )
        raise

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info("=== pipeline run complete in %.1fs ===", elapsed)


def main() -> None:
    parser = argparse.ArgumentParser(description="cu_pipeline scheduler")
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Execute the pipeline immediately and exit",
    )
    args = parser.parse_args()

    if args.run_now:
        run_pipeline()
        return

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_pipeline,
        CronTrigger(hour=2, minute=0, timezone="UTC"),
        id="nightly_pipeline",
        name="cu_pipeline nightly ETL",
        misfire_grace_time=3600,   # run up to 1h late if the process was down
        coalesce=True,             # skip stacked missed runs, not all of them
    )
    logger.info("scheduler: registered nightly job at 02:00 UTC")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("scheduler: shutting down")
        sys.exit(0)


if __name__ == "__main__":
    main()
