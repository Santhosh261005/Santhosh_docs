"""Run warning-only referential-quality checks across Silver datasets."""
from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from pipelines.bronze_to_silver_common import configure_logging
from pipelines.cross_dataset_quality import run_cross_dataset_quality_report
from pipelines.landing_to_bronze_common import start_landing_spark


def main():
    logger = configure_logging()
    spark = start_landing_spark("silver_cross_dataset_quality")
    try:
        return run_cross_dataset_quality_report(spark, logger=logger)
    except Exception:
        logger.exception("Cross-dataset quality reporting failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
