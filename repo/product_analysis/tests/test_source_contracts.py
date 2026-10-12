"""Static checks for declared schemas, manifests, notebooks and executable modules."""
import ast
import csv
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT.parent.parent / "data"
DATASETS = {
    "products": ("products_schema.json", "products/products", "id,default_code,name,categ_id,uom_id,list_price,create_date,write_date,active,qty_available,virtual_available,product_tmpl_id"),
    "sales_orders": ("sales_orders_schema.json", "sales/orders", "id,name,partner_id,date_order,state,amount_untaxed,amount_total,currency_id,create_date,write_date,company_id,user_id,team_id,client_order_ref,ops_order_at,ops_confirmed_at,ops_unmet_demand,ops_closed"),
    "inventory_quants": ("inventory_quants_schema.json", "inventory/quants", "id,product_id,location_id,quantity,reserved_quantity,write_date,company_id"),
    "inventory_pickings": ("inventory_pickings_schema.json", "inventory/pickings", "id,name,partner_id,state,origin,location_id,location_dest_id,company_id,scheduled_date,date_done,create_date,write_date,picking_type_id,sale_id,purchase_id,ops_warehouse_stage,ops_handling_seconds"),
}


class SourceContractTests(unittest.TestCase):
    def test_schema_keys_match_handoff_headers(self):
        for name, (schema_file, _, header) in DATASETS.items():
            with self.subTest(dataset=name):
                schema = json.loads((ROOT / "schemas" / schema_file).read_text(encoding="utf-8"))
                self.assertEqual(list(schema), header.split(","))

    def test_local_csv_headers_match_declared_schema(self):
        for name, rel in (("products", "products/2026-10-07_23_39.csv"), ("sales_orders", "orders/2026-10-07_23_39.csv")):
            schema_file = DATASETS[name][0]
            with self.subTest(dataset=name):
                with (DATA / rel).open(encoding="utf-8-sig", newline="") as handle:
                    header = next(csv.reader(handle))
                self.assertEqual(header, list(json.loads((ROOT / "schemas" / schema_file).read_text(encoding="utf-8"))))

    def test_manifest_declares_all_four_datasets_and_expected_headers(self):
        manifest = json.loads((DATA / "source-manifest.json").read_text(encoding="utf-8"))
        for name, (schema_file, dataset_key, _) in DATASETS.items():
            with self.subTest(dataset=name):
                self.assertIn(dataset_key, manifest["datasets"])
                schema = json.loads((ROOT / "schemas" / schema_file).read_text(encoding="utf-8"))
                file_entry = next(x for x in manifest["files"] if x.get("dataset") == dataset_key and x.get("path", "").endswith(".csv"))
                self.assertEqual(file_entry["columns"], list(schema))

    def test_all_python_files_parse(self):
        for path in ROOT.rglob("*.py"):
            with self.subTest(path=path.relative_to(ROOT)):
                ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def test_eight_dataset_notebooks_are_valid_json(self):
        for name in DATASETS:
            for stage in ("bronze", "silver"):
                filename = f"{name}_landing_to_bronze.ipynb" if stage == "bronze" else f"{name}_bronze_to_silver.ipynb"
                path = ROOT / "notebooks" / stage / filename
                with self.subTest(path=path.relative_to(ROOT)):
                    notebook = json.loads(path.read_text(encoding="utf-8"))
                    self.assertEqual(notebook["nbformat"], 4)
                    for index, cell in enumerate(notebook["cells"]):
                        if cell.get("cell_type") == "code":
                            ast.parse("".join(cell.get("source", [])), filename=f"{path}:{index}")


if __name__ == "__main__":
    unittest.main()
