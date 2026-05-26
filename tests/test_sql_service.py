from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from pipeline.repositories.file_repository import write_semicolon_csv
from pipeline.services.sql_service import (
    export_sql_to_csv,
    import_csv_to_sql,
    import_directory_to_sql,
    sql_load_history,
    sql_query,
    sql_tables,
)


class SqlServiceTest(unittest.TestCase):
    def test_import_export_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            db_path = root / "mpstats.duckdb"
            input_file = root / "input.csv"
            output_file = root / "export.csv"

            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Дата": "01.01.2025", "Маркетплейс": "Ozon", "SKU": "a", "Продажи, шт": 10},
                        {"Дата": "01.02.2025", "Маркетплейс": "WB", "SKU": "b", "Продажи, шт": 20},
                    ]
                ),
                input_file,
            )

            imported = import_csv_to_sql(
                input_file,
                db_path=db_path,
                table_name="mpstats_products",
                mode="replace",
                load_name="unit-test",
                project_name="tests",
            )
            self.assertEqual(imported.rows, 2)

            tables = sql_tables(db_path)
            self.assertIn("mpstats_products", tables["table_name"].tolist())

            history = sql_load_history(db_path)
            self.assertEqual(int(history.iloc[0]["rows_loaded"]), 2)

            selected = sql_query(
                db_path,
                'SELECT "SKU", "Продажи, шт" FROM mpstats_products WHERE "Маркетплейс" = \'WB\'',
            )
            self.assertEqual(selected.iloc[0]["SKU"], "b")

            exported = export_sql_to_csv(
                db_path=db_path,
                output_file=output_file,
                query='SELECT * FROM mpstats_products ORDER BY "SKU"',
            )
            self.assertEqual(exported.rows, 2)
            self.assertTrue(output_file.exists())

    def test_import_directory_replace_then_append(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "csv"
            source_dir.mkdir()
            db_path = root / "mpstats.duckdb"

            write_semicolon_csv(pd.DataFrame([{"SKU": "a"}]), source_dir / "a.csv")
            write_semicolon_csv(pd.DataFrame([{"SKU": "b"}]), source_dir / "b.csv")

            result = import_directory_to_sql(
                source_dir,
                db_path=db_path,
                table_name="mpstats_products",
                mode="replace",
            )
            self.assertEqual(result.ok, 2)
            self.assertEqual(result.rows, 2)
            loaded = sql_query(db_path, "SELECT COUNT(*) AS cnt FROM mpstats_products")
            self.assertEqual(int(loaded.iloc[0]["cnt"]), 2)


if __name__ == "__main__":
    unittest.main()
