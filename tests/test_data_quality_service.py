from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from pipeline.repositories.data_quality_repository import DataQualityRepository
from pipeline.repositories.sql_repository import apply_migrations, connect
from pipeline.services.data_quality_service import DataQualityService


def make_service(root: Path) -> DataQualityService:
    return DataQualityService(DataQualityRepository(project_root=root, workdir=root / "pipeline"))


def write_quality_cube(root: Path, project_name: str, rows: list[dict[str, object]]) -> Path:
    db_path = root / "mpstats.duckdb"
    root.mkdir(parents=True, exist_ok=True)
    table_name = "mpstats_products"
    metadata = ["__project_name", "__year", "__month", "__marketplace_code", "__category_key", "__source_file"]
    row_columns: list[str] = []
    for item in rows:
        for column in item:
            if column not in row_columns:
                row_columns.append(column)
    if not row_columns:
        row_columns = ["Дата", "Категория", "SKU", "Продажи, шт", "Средняя цена, руб", "Выручка, руб"]
    columns = [*metadata, *row_columns]
    with connect(db_path) as con:
        apply_migrations(con)
        con.execute(f"CREATE TABLE IF NOT EXISTS {quote_name(table_name)} ({', '.join(f'{quote_name(column)} VARCHAR' for column in columns)})")
        for column in columns:
            con.execute(f"ALTER TABLE {quote_name(table_name)} ADD COLUMN IF NOT EXISTS {quote_name(column)} VARCHAR")
        con.execute(f"DELETE FROM {quote_name(table_name)} WHERE {quote_name('__project_name')} = ?", [project_name])
        for item in rows:
            month = _month_from_row(item)
            category = str(item.get("Категория") or "category")
            payload = {
                "__project_name": project_name,
                "__year": "2025",
                "__month": str(month),
                "__marketplace_code": "oz",
                "__category_key": category,
                "__source_file": f"cube/{project_name}/{month:02d}/{category}.csv",
                **{key: str(value) for key, value in item.items()},
            }
            placeholders = ", ".join("?" for _ in columns)
            con.execute(
                f"INSERT INTO {quote_name(table_name)} ({', '.join(quote_name(column) for column in columns)}) VALUES ({placeholders})",
                [payload.get(column) for column in columns],
            )
        con.execute("DELETE FROM cube_registry WHERE project_name = ?", [project_name])
        registry: dict[tuple[int, str], int] = {}
        for item in rows or [{"Дата": "01.01.2025", "Категория": "empty"}]:
            key = (_month_from_row(item), str(item.get("Категория") or "empty"))
            registry[key] = registry.get(key, 0) + (1 if rows else 0)
        for (month, category), count in registry.items():
            con.execute(
                """
                INSERT INTO cube_registry (
                    id, project_name, year, month, marketplace, marketplace_code,
                    category_key, category_name, rows_count, source_processed_file_path
                )
                VALUES (?, ?, 2025, ?, 'Ozon', 'oz', ?, ?, ?, ?)
                """,
                [f"{project_name}-{month}-{category}", project_name, month, category, category, count, f"cube/{project_name}/{month:02d}/{category}.csv"],
            )
    return db_path


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


def quote_name(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _month_from_row(item: dict[str, object]) -> int:
    date = str(item.get("Дата") or "01.01.2025")
    try:
        return int(date.split(".")[1])
    except (IndexError, ValueError):
        return 1


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
            write_quality_cube(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertEqual(report["status"], "OK")
            self.assertEqual(report["source"]["kind"], "cube")
            self.assertEqual(report["metrics"]["summary_by_severity"]["total"], 0)

    def test_sales_spike_warns(self) -> None:
        """Резкий рост SKU ловится только при большом абсолютном эффекте."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_cube(
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
            write_quality_cube(
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
            write_quality_cube(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertNotIn("new_sku_high_sales", issue_ids(report))
            self.assertEqual(report["status"], "OK")

    def test_new_sku_high_sales_warns(self) -> None:
        """Новый SKU с большим объёмом выделяется отдельным lifecycle-событием."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rows = [row("01", "base", 20), row("02", "base", 20), row("03", "base", 20), row("04", "base", 20), row("04", "new", 800)]
            write_quality_cube(root, "unit", rows)

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
            write_quality_cube(root, "unit", rows)

            report = make_service(root).build_report("unit")

            ids = issue_ids(report)
            self.assertIn("sku_price_change", ids)
            self.assertIn("zero_or_negative_price", ids)
            self.assertGreaterEqual(report["metrics"]["summary_by_severity"]["CRITICAL"], 1)

    def test_duplicate_sku_period_warns(self) -> None:
        """Один SKU в одной категории и месяце в нескольких строках — бизнес-дубль."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_cube(root, "unit", [row("01", "sku-1", 10), row("01", "sku-1", 10), row("02", "sku-1", 11)])

            report = make_service(root).build_report("unit")

            self.assertIn("duplicate_sku_period_count", issue_ids(report))
            self.assertIn("duplicate_metric_rows", issue_ids(report))

    def test_missing_period_warns(self) -> None:
        """Пропущенный месяц между загруженными периодами виден отдельным issue."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_cube(root, "unit", [row("01", "sku-1", 10), row("02", "sku-1", 10), row("04", "sku-1", 10)])

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
            write_quality_cube(root, "unit", rows)

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
            write_quality_cube(root, "unit", rows)

            report = make_service(root).build_report("unit")

            self.assertIn("brand_revenue_share_spike", issue_ids(report))

    def test_revenue_price_sales_mismatch_warns(self) -> None:
        """ТО сравнивается с продажи × цена с допуском, а не как жёсткое равенство."""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_cube(root, "unit", [row("01", "sku-1", 10, price=10, revenue=500)])

            report = make_service(root).build_report("unit")

            self.assertIn("revenue_price_sales_mismatch", issue_ids(report))

    def test_empty_file_fails_and_missing_project_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write_quality_cube(root, "empty", [])
            service = make_service(root)

            report = service.build_report("empty")
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["total_rows"], 0)

            with self.assertRaises(FileNotFoundError):
                service.build_report("missing")


if __name__ == "__main__":
    unittest.main()
