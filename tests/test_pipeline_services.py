from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from pipeline.repositories.file_repository import write_semicolon_csv
from pipeline.services.classification_service import classify_file
from pipeline.services.enrich_service import (
    extract_category_from_filename,
    extract_first_date_from_filename,
    extract_marketplace_from_filename,
)
from pipeline.services.merge_service import merge_dataframes
from pipeline.services.run_service import parse_steps


class PipelineServicesTest(unittest.TestCase):
    def test_parse_steps_supports_ranges_and_lists(self) -> None:
        self.assertEqual(parse_steps("2-4,6"), [2, 3, 4, 6])
        self.assertEqual(parse_steps([6, 2, 2]), [2, 6])

    def test_filename_metadata_extractors(self) -> None:
        filename = "Ozon_-_Категории_-_Продукты_2025-06-01-2025-06-30__Мясо.csv"
        dt = extract_first_date_from_filename(filename)
        self.assertIsNotNone(dt)
        self.assertEqual(dt.strftime("%d.%m.%Y"), "01.06.2025")
        self.assertEqual(extract_marketplace_from_filename(filename), "Ozon")
        self.assertEqual(extract_category_from_filename(filename), "Мясо")

    def test_merge_dataframes_filters_sales_and_deduplicates(self) -> None:
        frame = pd.DataFrame(
            [
                {"SKU": "a", "Продажи": "10", "Название": "one"},
                {"SKU": "a", "Продажи": "10", "Название": "one"},
                {"SKU": "b", "Продажи": "0", "Название": "two"},
                {"SKU": "c", "Продажи": "50000", "Название": "three"},
            ]
        )
        merged = merge_dataframes([frame], min_sales=0, max_sales=40_000)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged.iloc[0]["SKU"], "a")
        self.assertIn("Продажи, шт", merged.columns)

    def test_classify_file_applies_rules_and_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_file = root / "input.csv"
            output_file = root / "out.csv"
            rules_file = root / "rules.csv"
            fill_file = root / "fill.json"

            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {
                            "Дата": "01.01.2025",
                            "Маркетплейс": "Ozon",
                            "Категория": "Мясо",
                            "SKU": "12345",
                            "Бренд": "brand",
                            "Название": "Тестовый продукт мясной",
                            "Продажи, шт": 1,
                        },
                        {
                            "Дата": "01.01.2025",
                            "Маркетплейс": "Ozon",
                            "Категория": "Мясо",
                            "SKU": "67890",
                            "Бренд": "brand",
                            "Название": "Тестовый продукт без ключа",
                            "Продажи, шт": 1,
                        }
                    ]
                ),
                input_file,
            )
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        "1;1;*;Подкатегория;Название;contains;мясной;Мясо;fill_empty;;",
                        "1;999;*;Подкатегория;;otherwise;;Прочее;fill_empty;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            fill_file.write_text(json.dumps({"Тип": "Прочие"}, ensure_ascii=False), encoding="utf-8")

            result, report, step = classify_file(
                input_file,
                output_file,
                rules_path=rules_file,
                fill_unclassified={"Тип": "Прочие"},
                write_xlsx=False,
            )

            self.assertEqual(step.ok, 1)
            self.assertTrue(output_file.exists())
            self.assertEqual(report["applied_rows"].sum(), 2)
            self.assertEqual(result.iloc[0]["Подкатегория"], "Мясо")
            self.assertEqual(result.iloc[1]["Подкатегория"], "Прочее")
            self.assertEqual(result.iloc[0]["Тип"], "Прочие")


if __name__ == "__main__":
    unittest.main()
