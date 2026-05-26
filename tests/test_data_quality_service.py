from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from pipeline.repositories.data_quality_repository import DataQualityRepository
from pipeline.repositories.file_repository import write_semicolon_csv
from pipeline.services.data_quality_service import DataQualityService


def make_service(root: Path) -> DataQualityService:
    return DataQualityService(DataQualityRepository(project_root=root, workdir=root / "pipeline"))


def write_quality_csv(root: Path, project_name: str, rows: list[dict[str, object]], *, classified: bool = True) -> Path:
    pipeline_dir = root / "pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    suffix = "_merged_classified.csv" if classified else "_merged.csv"
    path = pipeline_dir / f"03_{project_name}{suffix}"
    write_semicolon_csv(pd.DataFrame(rows), path)
    return path


class DataQualityServiceTest(unittest.TestCase):
    def test_normal_classified_csv_is_ok(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(
                root,
                "unit",
                [
                    {"Название": "Лимон 1 кг", "Маркетплейс": "Ozon", "Категория": "Кислота", "SKU": "sku-1", "Вес, кг": 1.0, "Объем, кг": 5.0, "Подкатегория": "Лимонная"},
                    {"Название": "Лимон 2 кг", "Маркетплейс": "WB", "Категория": "Кислота", "SKU": "sku-2", "Вес, кг": 2.0, "Объем, кг": 6.0, "Подкатегория": "Лимонная"},
                ],
            )

            report = make_service(root).build_report("unit")

            self.assertEqual(report["status"], "OK")
            self.assertEqual(report["total_rows"], 2)
            self.assertEqual(report["source"]["kind"], "classified")
            self.assertEqual(report["metrics"]["weight_volume"]["parsed_count"], 2)
            self.assertEqual(report["metrics"]["classification"]["classified_count"], 2)
            self.assertEqual(report["metrics"]["duplicates"]["duplicate_rows"], 0)

    def test_empty_classification_warns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(
                root,
                "unit",
                [
                    {"Название": "Лимон 1 кг", "Маркетплейс": "Ozon", "Категория": "Кислота", "SKU": "sku-1", "Вес, кг": 1.0, "Подкатегория": ""},
                    {"Название": "Лимон 2 кг", "Маркетплейс": "WB", "Категория": "Кислота", "SKU": "sku-2", "Вес, кг": 2.0, "Подкатегория": "Лимонная"},
                ],
            )

            report = make_service(root).build_report("unit")

            self.assertEqual(report["status"], "WARNING")
            self.assertEqual(report["metrics"]["classification"]["unclassified_count"], 1)
            self.assertTrue(any(problem["type"] == "Не классифицировано" for problem in report["problems"]))
            self.assertEqual(len(report["examples"]["unclassified"]), 1)

    def test_weight_anomalies_warn(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(
                root,
                "unit",
                [
                    {"Название": "Вес отрицательный", "Маркетплейс": "Ozon", "Категория": "Тест", "SKU": "sku-1", "Вес, кг": -1.0, "Подкатегория": "Тест"},
                    {"Название": "Очень большой", "Маркетплейс": "Ozon", "Категория": "Тест", "SKU": "sku-2", "Вес, кг": 45.0, "Подкатегория": "Тест"},
                    {"Название": "Очень маленький", "Маркетплейс": "Ozon", "Категория": "Тест", "SKU": "sku-3", "Вес, кг": 0.0005, "Подкатегория": "Тест"},
                ],
            )

            report = make_service(root).build_report("unit")

            self.assertEqual(report["status"], "WARNING")
            self.assertEqual(report["metrics"]["anomalies"]["zero_or_negative"], 1)
            self.assertEqual(report["metrics"]["anomalies"]["too_large"], 1)
            self.assertEqual(report["metrics"]["anomalies"]["suspicious"], 1)
            self.assertEqual(len(report["examples"]["anomalies"]), 3)

    def test_csv_without_optional_columns_does_not_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(
                root,
                "unit",
                [
                    {"Название": "Товар без расширенных колонок", "SKU": "sku-1"},
                    {"Название": "Ещё товар", "SKU": "sku-2"},
                ],
            )

            report = make_service(root).build_report("unit")

            self.assertEqual(report["status"], "WARNING")
            self.assertEqual(report["total_rows"], 2)
            self.assertTrue(any(item["check"] == "Вес/объём" for item in report["skipped_checks"]))
            self.assertTrue(any(item["check"] == "Классификация" for item in report["skipped_checks"]))
            self.assertEqual(report["metrics"]["duplicates"]["duplicate_rows"], 0)

    def test_empty_file_fails_and_missing_project_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = write_quality_csv(root, "empty", [], classified=True)
            pd.DataFrame(columns=["Название", "SKU", "Вес, кг"]).to_csv(path, sep=";", index=False, encoding="utf-8-sig")
            service = make_service(root)

            report = service.build_report("empty")
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["total_rows"], 0)

            with self.assertRaises(FileNotFoundError):
                service.build_report("missing")


if __name__ == "__main__":
    unittest.main()
