"""Command-line Bronze-to-Silver job for inventory_pickings."""
from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(PROJECT_ROOT))
from pipelines.inventory_pickings_silver import SPEC
from pipelines.multi_dataset_silver import silver_job_main
def main(argv=None): return silver_job_main(SPEC, argv)
if __name__ == "__main__": main()
