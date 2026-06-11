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
    folder = "processed" if classified else "merged"
    suffix = "_classified.csv" if classified else ".csv"
    project_dir = root / "data" / "projects" / project_name / folder
    project_dir.mkdir(parents=True, exist_ok=True)
    path = project_dir / f"{project_name}{suffix}"
    write_semicolon_csv(pd.DataFrame(rows), path)
    return path


def row(
    month: str,
    sku: str,
    sales: float,
    *,
    category: str = "Кислота",
    brand: str = "Brand A",
    price: float = 10.0,
    revenue: float | None = None,
) -> dict[str, object]:
    return {
        "Дата": f"01.{month}.2025",
        "Маркетплейс": "Ozon",
        "Категория": category,
        "SKU": sku,
        "Бренд": brand,
        "Название": f"Товар {sku}",
        "Продажи, шт": sales,
        "Средняя цена, руб": price,
        "Выручка, руб": sales * price if revenue is None else revenue,
        "Вес, кг (ед.)": 1.0,
        "Цена за кг": price,
    }


def issue_ids(report: dict[str, object]) -> set[str]:
    return {str(issue["check_id"]) for issue in report.get("issues", [])}


class DataQualityServiceTest(unittest.TestCase):
    def test_normal_sku_history_is_ok(self) -> None:
        """Стабильная история без скачков не должна шуметь.

        Медиана нужна как устойчивая база: она не дёргается от небольших
        колебаний продаж и лучше простого среднего на грязных маркетплейсных
        данных.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows = [
                row("01", "sku-1", 20),
                row("02", "sku-1", 22),
                row("03", "sku-1", 21),
                row("04", "sku-1", 23),
                row("01", "sku-2", 18, brand="Brand B"),
                row("02", "sku-2", 19, brand="Brand B"),
                row("03", "sku-2", 20, brand="Brand B"),
                row("04", "sku-2", 18, brand="Brand B"),
            ]
            write_quality_csv(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertEqual(report["status"], "OK")
            self.assertEqual(report["metrics"]["summary_by_severity"]["total"], 0)

    def test_sales_spike_warns(self) -> None:
        """Резкий рост SKU ловится только при большом абсолютном эффекте."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(
                root,
                "unit",
                [row("01", "sku-1", 20), row("02", "sku-1", 22), row("03", "sku-1", 21), row("04", "sku-1", 500)],
            )

            report = make_service(root).build_report("unit")

            self.assertEqual(report["status"], "WARNING")
            self.assertIn("sku_sales_spike", issue_ids(report))

    def test_sales_drop_warns(self) -> None:
        """Провал стабильного SKU почти до нуля считается бизнес-предупреждением."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(
                root,
                "unit",
                [row("01", "sku-1", 100), row("02", "sku-1", 110), row("03", "sku-1", 120), row("04", "sku-1", 0, revenue=0)],
            )

            report = make_service(root).build_report("unit")

            self.assertIn("sku_sales_drop", issue_ids(report))

    def test_new_sku_small_sales_is_not_warning(self) -> None:
        """Новый SKU с маленькими продажами полезен аналитически, но не должен быть warning."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows = [row("01", "base", 20), row("02", "base", 20), row("03", "base", 20), row("04", "base", 20), row("04", "new", 5)]
            write_quality_csv(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertNotIn("new_sku_high_sales", issue_ids(report))
            self.assertEqual(report["status"], "OK")

    def test_new_sku_high_sales_warns(self) -> None:
        """Новый SKU с большим объёмом выделяется отдельным lifecycle-событием."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows = [row("01", "base", 20), row("02", "base", 20), row("03", "base", 20), row("04", "base", 20), row("04", "new", 800)]
            write_quality_csv(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertIn("new_sku_high_sales", issue_ids(report))
            self.assertTrue(report["business_changes"])

    def test_price_tenfold_change_and_negative_price(self) -> None:
        """Цена проверяется по истории SKU, а отрицательная цена сразу CRITICAL."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows = [
                row("01", "price-jump", 10, price=10),
                row("02", "price-jump", 10, price=10),
                row("03", "price-jump", 10, price=10),
                row("04", "price-jump", 10, price=100),
                row("04", "negative", 10, price=-5, revenue=50),
            ]
            write_quality_csv(root, "unit", rows)

            report = make_service(root).build_report("unit")

            ids = issue_ids(report)
            self.assertIn("sku_price_change", ids)
            self.assertIn("zero_or_negative_price", ids)
            self.assertGreaterEqual(report["metrics"]["summary_by_severity"]["CRITICAL"], 1)

    def test_duplicate_sku_period_warns(self) -> None:
        """Один SKU в одной категории и месяце в нескольких строках — бизнес-дубль."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(root, "unit", [row("01", "sku-1", 10), row("01", "sku-1", 10), row("02", "sku-1", 11)])

            report = make_service(root).build_report("unit")

            self.assertIn("duplicate_sku_period_count", issue_ids(report))
            self.assertIn("duplicate_metric_rows", issue_ids(report))

    def test_missing_period_warns(self) -> None:
        """Пропущенный месяц между загруженными периодами виден отдельным issue."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(root, "unit", [row("01", "sku-1", 10), row("02", "sku-1", 10), row("04", "sku-1", 10)])

            report = make_service(root).build_report("unit")

            self.assertIn("missing_period", issue_ids(report))

    def test_latest_period_row_drop_is_info(self) -> None:
        """Последний неполный период помечается мягко, потому что месяц мог быть выгружен не до конца."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows: list[dict[str, object]] = []
            for month in ("01", "02", "03"):
                rows.extend(row(month, f"sku-{index}", 1, brand=f"Brand {index}") for index in range(20))
            rows.extend(row("04", f"sku-{index}", 1, brand=f"Brand {index}") for index in range(4))
            write_quality_csv(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertIn("category_period_row_drop", issue_ids(report))
            matching = [issue for issue in report["issues"] if issue["check_id"] == "category_period_row_drop"]
            self.assertTrue(all(issue["severity"] == "INFO" for issue in matching))

    def test_brand_share_spike_warns(self) -> None:
        """Доля бренда сравнивается с собственной историей внутри категории."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows = []
            for month in ("01", "02", "03"):
                rows.append(row(month, "brand-a", 5, brand="Brand A"))
                rows.append(row(month, "brand-b", 95, brand="Brand B"))
            rows.append(row("04", "brand-a", 80, brand="Brand A"))
            rows.append(row("04", "brand-b", 20, brand="Brand B"))
            write_quality_csv(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertIn("brand_revenue_share_spike", issue_ids(report))

    def test_revenue_price_sales_mismatch_warns(self) -> None:
        """ТО сравнивается с продажи × цена с допуском, а не как жёсткое равенство."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_csv(root, "unit", [row("01", "sku-1", 10, price=10, revenue=500)])

            report = make_service(root).build_report("unit")

            self.assertIn("revenue_price_sales_mismatch", issue_ids(report))

    def test_empty_file_fails_and_missing_project_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = write_quality_csv(root, "empty", [], classified=True)
            pd.DataFrame(columns=["Дата", "SKU", "Продажи, шт"]).to_csv(path, sep=";", index=False, encoding="utf-8-sig")
            service = make_service(root)

            report = service.build_report("empty")
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["total_rows"], 0)

            with self.assertRaises(FileNotFoundError):
                service.build_report("missing")


if __name__ == "__main__":
    unittest.main()
