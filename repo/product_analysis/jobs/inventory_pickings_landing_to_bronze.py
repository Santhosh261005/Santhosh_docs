"""Command-line Landing-to-Bronze job for inventory_pickings."""
from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(PROJECT_ROOT))
DATA_ROOT = PROJECT_ROOT.parent.parent / "data"
from pipelines.landing_to_bronze_common import run_landing_job
DEFAULT_SOURCE = DATA_ROOT / r"inventory/pickings/2026-10-07_23_17.csv"
DEFAULT_SCHEMA = PROJECT_ROOT / "schemas" / "inventory_pickings_schema.json"
DATASET = "inventory_pickings"
DATASET_KEY = "inventory/pickings"
def main(argv=None): return run_landing_job(DATASET, DATASET_KEY, DEFAULT_SOURCE, DEFAULT_SCHEMA, argv)
if __name__ == "__main__": main()
