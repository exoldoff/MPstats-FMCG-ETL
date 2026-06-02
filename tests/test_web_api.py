from __future__ import annotations

from datetime import date
from io import BytesIO
import json
import os
from pathlib import Path
import tempfile
from threading import Event
import time
import unittest
from unittest.mock import patch

import pandas as pd
from fastapi.testclient import TestClient

from mpstats_app.config import AppSettings
from mpstats_app.main import create_app
from mpstats_app.repositories.duckdb_repository import DuckDbAppRepository
from mpstats_app.services.smart_pipeline_service import month_day_coverage
from pipeline.repositories.file_repository import write_semicolon_csv
from pipeline.repositories.sql_repository import connect


def make_settings(root: Path) -> AppSettings:
    return AppSettings.create(
        project_root=root,
        workdir=root / "pipeline",
        db_path=root / "mpstats.duckdb",
        config_path=root / "pipeline" / "step1_export_config.json",
        rules_path=root / "classifiers" / "rules.csv",
        scheduler_poll_seconds=0.1,
        static_dir=root / "web" / "dist",
    )


def seed_project(root: Path) -> None:
    (root / "pipeline").mkdir(parents=True)
    (root / "classifiers").mkdir(parents=True)
    (root / "pipeline" / "step1_export_config.json").write_text(
        """
        {
          "export_months_by_year": {"2025": [1]},
          "save_dir": "",
          "skip_if_exists": true,
          "extract_zip": true,
          "cookie": "",
          "tasks": [
            {"active": true, "mp": "oz", "path": "Продукты/Тест", "cat": "Тест", "fbs": 1}
          ]
        }
        """,
        encoding="utf-8",
    )
    (root / "classifiers" / "rules.csv").write_text(
        "\n".join(
            [
                "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                "1;1;*;Тип;Название;contains;лимон;Кислота;fill_empty;;",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "Справочник категорий MP STATS.csv").write_text(
        "\n".join(
            [
                "Чек;Категория;МП;FBS;От;До;Комментарий;Путь;Фильтр;Путь2;Фильтр2;Актуализация",
                ";Лимонная кислота;Озон;1;2025;2025;;Продукты/Тест/Лимонная кислота;;;;",
                ";Лимонная кислота;WB;;2025;2025;;Продукты/Тест/WB;;;;",
                ";Масло;WB;;2025;2025;;Продукты/Тест/Масло;\"\"\"Подсолнечное\"\"\";;;",
                ";Смена пути;WB;;янв.25;янв.25;;Продукты/Старый путь;;;;",
                ";Смена пути;WB;;фев.25;фев.25;;Продукты/Новый путь;;;;",
                ";Пустой путь;WB;;2025;2025;;НД;;;;",
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )


def _has_openpyxl() -> bool:
    try:
        __import__("openpyxl")
        return True
    except ModuleNotFoundError:
        return False


class WebApiTest(unittest.TestCase):
    def test_month_day_coverage_marks_current_month_by_saved_day(self) -> None:
        current = month_day_coverage(2026, 5, today=date(2026, 5, 28))
        self.assertEqual(current["days_loaded"], 28)
        self.assertEqual(current["days_in_month"], 31)
        self.assertEqual(current["data_actual_until"], "2026-05-28")

        past = month_day_coverage(2026, 4, today=date(2026, 5, 28))
        self.assertEqual(past["days_loaded"], 30)
        self.assertEqual(past["days_in_month"], 30)
        self.assertEqual(past["data_actual_until"], "2026-04-30")

    def test_db_import_widens_empty_classifier_column_to_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            repository = DuckDbAppRepository(settings)

            first_file = root / "first.csv"
            write_semicolon_csv(
                pd.DataFrame([{"SKU": "sku-1", "Подкатегория": None, "Цена за кг": 10.5}]),
                first_file,
            )
            repository.import_products_file_idempotent(
                run_id="run-1",
                csv_path=first_file,
                table_name=settings.products_table,
                project_name="unit",
                year=2025,
                month=1,
                marketplace_code="oz",
                category_key="sugar",
            )

            second_file = root / "second.csv"
            write_semicolon_csv(
                pd.DataFrame([{"SKU": "sku-2", "Подкатегория": "Тростниковый", "Цена за кг": 12.5}]),
                second_file,
            )
            inserted = repository.import_products_file_idempotent(
                run_id="run-1",
                csv_path=second_file,
                table_name=settings.products_table,
                project_name="unit",
                year=2025,
                month=1,
                marketplace_code="oz",
                category_key="sugar",
            )

            self.assertEqual(inserted, 1)
            with connect(settings.db_path) as con:
                data_type = con.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_name = ? AND column_name = 'Подкатегория'
                    """,
                    [settings.products_table],
                ).fetchone()[0]
                values = con.execute(
                    f'SELECT "Подкатегория" FROM "{settings.products_table}" ORDER BY "SKU"'
                ).fetchall()

            self.assertEqual(data_type, "VARCHAR")
            self.assertEqual(values, [(None,), ("Тростниковый",)])

    def test_health_settings_schedules_and_run_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                health = client.get("/api/health")
                self.assertEqual(health.status_code, 200)
                self.assertTrue(health.json()["ok"])

                config_response = client.get("/api/settings/export-config")
                self.assertEqual(config_response.status_code, 200)
                self.assertEqual(config_response.json()["config"]["export_months_by_year"], {"2025": [1]})

                rules_response = client.put("/api/rules", json={"content": "active;priority\n"})
                self.assertEqual(rules_response.status_code, 200)
                self.assertEqual((root / "classifiers" / "rules.csv").read_text(encoding="utf-8"), "active;priority\n")

                structured_rules = client.put(
                    "/api/classifier/rules",
                    json={
                        "rules": [
                            {
                                "active": True,
                                "priority": 10,
                                "category": "*",
                                "target_column": "Тип",
                                "set_value": "Кислота",
                                "mode": "fill_empty",
                                "comment": "unit",
                                "conditions": [
                                    {"join_with_prev": "and", "match_field": "Название", "match_type": "contains", "pattern": "лимон"},
                                    {"join_with_prev": "or", "match_field": "SKU", "match_type": "contains", "pattern": "лимон"},
                                ],
                            },
                            {
                                "active": True,
                                "priority": 999,
                                "category": "*",
                                "target_column": "Тип",
                                "set_value": "Прочее",
                                "mode": "fill_empty",
                                "comment": "fallback",
                                "conditions": [
                                    {"join_with_prev": "and", "match_field": "", "match_type": "otherwise", "pattern": ""},
                                ],
                            }
                        ]
                    },
                )
                self.assertEqual(structured_rules.status_code, 200)
                rules_payload = structured_rules.json()["rules"]
                self.assertEqual(rules_payload[0]["conditions"][1]["join_with_prev"], "or")
                self.assertEqual(rules_payload[1]["conditions"][0]["match_type"], "otherwise")
                self.assertIn("conditions_json", (root / "classifiers" / "rules.csv").read_text(encoding="utf-8-sig"))

                schedule_response = client.post(
                    "/api/schedules",
                    json={
                        "name": "daily",
                        "project_name": "unit",
                        "steps": "2-6",
                        "enabled": True,
                        "interval_minutes": 60,
                    },
                )
                self.assertEqual(schedule_response.status_code, 200)
                schedule_id = schedule_response.json()["schedule_id"]
                self.assertEqual(client.get("/api/schedules").json()["schedules"][0]["schedule_id"], schedule_id)

                run_response = client.post("/api/runs", json={"project_name": "unit", "steps": "2-3"})
                self.assertEqual(run_response.status_code, 200)
                self.assertEqual(run_response.json()["status"], "queued")

    def test_project_management_lists_and_deletes_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                created = client.post("/api/projects", json={"project_name": "Новый проект"})
                self.assertEqual(created.status_code, 200)
                self.assertEqual(created.json()["project_name"], "Новый проект")
                self.assertTrue((root / "data" / "projects" / "Новый_проект").exists())
                self.assertFalse(created.json()["has_files"])

                after_create = client.get("/api/projects")
                self.assertIn("Новый проект", {project["project_name"] for project in after_create.json()["projects"]})
                self.assertTrue(
                    next(project for project in after_create.json()["projects"] if project["project_name"] == "Новый проект")["is_current"]
                )

                repository: DuckDbAppRepository = app.state.repository
                repository.create_pipeline_run(
                    run_id="plan-1",
                    project_name="unit",
                    run_type="historical_backfill",
                    period_from="2025-01",
                    period_to="2025-01",
                    selected_category_ids=["category-1"],
                    settings={},
                )
                repository.upsert_cube_entry(
                    {
                        "project_name": "unit",
                        "year": 2025,
                        "month": 1,
                        "marketplace": "Ozon",
                        "marketplace_code": "oz",
                        "category_key": "test",
                        "category_name": "Тест",
                        "rows_count": 1,
                        "days_loaded": 17,
                        "days_in_month": 31,
                        "data_actual_until": "2025-01-17",
                        "source_processed_file_path": "unit.csv",
                        "file_hash": "hash",
                    }
                )
                csv_path = root / "classified.csv"
                write_semicolon_csv(
                    pd.DataFrame([{"Маркетплейс": "Ozon", "Категория": "Тест", "Название": "Товар"}]),
                    csv_path,
                )
                repository.import_products_file_idempotent(
                    run_id="plan-1",
                    csv_path=csv_path,
                    table_name=settings.products_table,
                    project_name="unit",
                    year=2025,
                    month=1,
                    marketplace_code="oz",
                    category_key="test",
                )
                project_dir = root / "data" / "projects" / "unit"
                (project_dir / "raw").mkdir(parents=True)
                (project_dir / "raw" / "unit.csv").write_text("x", encoding="utf-8")

                projects = client.get("/api/projects")
                self.assertEqual(projects.status_code, 200)
                unit = next(project for project in projects.json()["projects"] if project["project_name"] == "unit")
                self.assertEqual(unit["cube_slices_count"], 1)
                self.assertEqual(unit["product_rows_count"], 1)
                self.assertTrue(unit["has_files"])
                cube = client.get("/api/workflow/pipeline/cube", params={"project_name": "unit"})
                self.assertEqual(cube.status_code, 200)
                cube_item = cube.json()["items"][0]
                self.assertEqual(cube_item["days_loaded"], 17)
                self.assertEqual(cube_item["days_in_month"], 31)
                self.assertEqual(cube_item["data_actual_until"], "2025-01-17")

                deleted = client.delete("/api/projects", params={"project_name": "unit", "delete_files": True})
                self.assertEqual(deleted.status_code, 200)
                self.assertEqual(deleted.json()["deleted"]["cube_registry"], 1)
                self.assertEqual(deleted.json()["deleted"]["product_rows"], 1)
                self.assertFalse(project_dir.exists())

                after_delete = client.get("/api/projects")
                self.assertNotIn("unit", {project["project_name"] for project in after_delete.json()["projects"]})

    def test_spa_fallback_does_not_shadow_unknown_api_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            static_dir = root / "web" / "dist"
            static_dir.mkdir(parents=True)
            (static_dir / "index.html").write_text("<div id=\"root\"></div>", encoding="utf-8")
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                api_response = client.get("/api/workflow/pipeline/missing")
                self.assertEqual(api_response.status_code, 404)
                self.assertEqual(api_response.headers["content-type"].split(";")[0], "application/json")
                self.assertEqual(api_response.json()["detail"], "API endpoint not found")

                page_response = client.get("/workflow/monthly")
                self.assertEqual(page_response.status_code, 200)
                self.assertIn("root", page_response.text)

    def test_product_search_uses_latest_successful_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            repository = DuckDbAppRepository(settings)
            repository.ensure_ready()
            run = repository.create_run(
                run_id="run-1",
                project_name="unit",
                steps="2,3,4,5,6",
                source="manual",
                schedule_id=None,
                workdir=settings.workdir,
                config_path=settings.config_path,
                rules_path=settings.rules_path,
                db_path=settings.db_path,
                products_table=settings.products_table,
                write_xlsx=False,
                max_weight_kg=40.0,
                fill_unclassified=None,
            )
            repository.finish_run(str(run["run_id"]), "succeeded")
            csv_path = root / "classified.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {
                            "Маркетплейс": "Ozon",
                            "Категория": "Тест",
                            "Артикул": "123",
                            "SKU": "Лимонная кислота 1 кг",
                            "Бренд": "Brand",
                        }
                    ]
                ),
                csv_path,
            )
            repository.import_products_file(
                run_id="run-1",
                csv_path=csv_path,
                table_name=settings.products_table,
                project_name="unit",
            )
            other_run = repository.create_run(
                run_id="run-2",
                project_name="other",
                steps="2,3,4,5,6",
                source="manual",
                schedule_id=None,
                workdir=settings.workdir,
                config_path=settings.config_path,
                rules_path=settings.rules_path,
                db_path=settings.db_path,
                products_table=settings.products_table,
                write_xlsx=False,
                max_weight_kg=40.0,
                fill_unclassified=None,
            )
            repository.finish_run(str(other_run["run_id"]), "succeeded")
            other_csv_path = root / "other_classified.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {
                            "Маркетплейс": "Ozon",
                            "Категория": "Тест",
                            "Артикул": "999",
                            "SKU": "Соль 1 кг",
                            "Бренд": "Other",
                        }
                    ]
                ),
                other_csv_path,
            )
            repository.import_products_file(
                run_id="run-2",
                csv_path=other_csv_path,
                table_name=settings.products_table,
                project_name="other",
            )

            app = create_app(settings, start_workers=False)
            with TestClient(app) as client:
                preview = client.get("/api/products", params={"project_name": "unit", "limit": 100})
                self.assertEqual(preview.status_code, 200)
                preview_payload = preview.json()
                self.assertEqual(preview_payload["total"], 1)
                self.assertEqual(preview_payload["run_id"], "run-1")
                self.assertEqual(str(preview_payload["rows"][0]["Артикул"]), "123")

                response = client.get("/api/products", params={"project_name": "unit", "query": "лимон"})
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(payload["total"], 1)
                self.assertEqual(payload["run_id"], "run-1")
                self.assertEqual(str(payload["rows"][0]["Артикул"]), "123")

    def test_export_options_preview_build_split_and_by_category(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            repository = DuckDbAppRepository(settings)
            repository.ensure_ready()

            lemon_file = root / "lemon.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Маркетплейс": "Ozon", "Категория": "Лимонная кислота", "SKU": "sku-1", "Название": "лимон 1 кг", "Бренд": "Brand A", "Продажи, шт": 3},
                        {"Маркетплейс": "Ozon", "Категория": "Лимонная кислота", "SKU": "sku-2", "Название": "лимон 2 кг", "Бренд": "Brand B", "Продажи, шт": 5},
                        {"Маркетплейс": "Ozon", "Категория": "Лимонная кислота", "SKU": "sku-0", "Название": "лимон без продаж", "Бренд": "Brand Z", "Продажи, шт": 0},
                    ]
                ),
                lemon_file,
            )
            repository.import_products_file_idempotent(
                run_id="run-export-1",
                csv_path=lemon_file,
                table_name=settings.products_table,
                project_name="unit",
                year=2025,
                month=1,
                marketplace_code="oz",
                category_key="lemon-oz",
            )
            soap_file = root / "soap.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Маркетплейс": "WB", "Категория": "Мыло", "SKU": "sku-3", "Название": "мыло тест", "Бренд": "Brand C"},
                    ]
                ),
                soap_file,
            )
            repository.import_products_file_idempotent(
                run_id="run-export-2",
                csv_path=soap_file,
                table_name=settings.products_table,
                project_name="unit",
                year=2025,
                month=2,
                marketplace_code="wb",
                category_key="soap-wb",
            )

            app = create_app(settings, start_workers=False)
            with (
                patch.object(app.state.repository, "count_export_products", side_effect=AssertionError("Export flow should reuse breakdown totals.")),
                TestClient(app) as client,
            ):
                options = client.get("/api/exports/options", params={"project_name": "unit"})
                self.assertEqual(options.status_code, 200)
                options_payload = options.json()
                self.assertEqual(options_payload["period_from"], "2025-01")
                self.assertEqual(options_payload["period_to"], "2025-02")
                self.assertIn("Название", options_payload["columns"])
                self.assertNotIn("__row_hash", options_payload["columns"])
                self.assertEqual({row["category_key"] for row in options_payload["categories"]}, {"lemon-oz", "soap-wb"})
                self.assertEqual(Path(options_payload["default_output_dir"]), (root / "data" / "projects" / "unit" / "exports").resolve())

                preview_payload = {
                    "project_name": "unit",
                    "category_keys": ["lemon-oz"],
                    "period_from": "2025-01",
                    "period_to": "2025-01",
                    "selected_columns": ["SKU", "Название", "Бренд", "Продажи, шт"],
                    "filters": [
                        {"column": "Название", "match_type": "contains", "value": "лимон"},
                        {"column": "Продажи, шт", "match_type": "gt", "value": "0"},
                    ],
                    "excluded_row_hashes": [],
                    "sort_column": "SKU",
                    "sort_direction": "asc",
                    "split_by_category": False,
                    "limit": 100,
                    "offset": 0,
                }
                preview = client.post("/api/exports/preview", json=preview_payload)
                self.assertEqual(preview.status_code, 200)
                preview_data = preview.json()
                self.assertEqual(preview_data["total"], 2)
                self.assertEqual(preview_data["columns"], ["SKU", "Название", "Бренд", "Продажи, шт"])
                self.assertEqual(preview_data["breakdown"][0]["period"], "2025-01")
                self.assertEqual(preview_data["breakdown"][0]["rows_count"], 2)
                excluded_hash = preview_data["rows"][0]["__row_hash"]

                excluded_preview = client.post(
                    "/api/exports/preview",
                    json={**preview_payload, "excluded_row_hashes": [excluded_hash]},
                )
                self.assertEqual(excluded_preview.status_code, 200)
                self.assertEqual(excluded_preview.json()["total"], 1)

                template = client.post(
                    "/api/exports/templates",
                    json={
                        **{key: value for key, value in preview_payload.items() if key not in {"limit", "offset", "excluded_row_hashes"}},
                        "name": "Ежемесячный лимон",
                        "export_format": "xlsx",
                        "output_dir": str(root / "template-exports"),
                    },
                )
                self.assertEqual(template.status_code, 200)
                template_payload = template.json()
                self.assertEqual(template_payload["name"], "Ежемесячный лимон")
                self.assertEqual(template_payload["project_name"], "unit")
                self.assertEqual(template_payload["category_keys"], ["lemon-oz"])
                self.assertEqual(template_payload["selected_columns"], ["SKU", "Название", "Бренд", "Продажи, шт"])

                templates = client.get("/api/exports/templates", params={"project_name": "unit"})
                self.assertEqual(templates.status_code, 200)
                self.assertEqual([row["name"] for row in templates.json()["templates"]], ["Ежемесячный лимон"])

                deleted_template = client.delete(
                    f"/api/exports/templates/{template_payload['id']}",
                    params={"project_name": "unit"},
                )
                self.assertEqual(deleted_template.status_code, 200)
                self.assertEqual(client.get("/api/exports/templates", params={"project_name": "unit"}).json()["templates"], [])

                output_dir = root / "exports"
                built = client.post(
                    "/api/exports/build",
                    json={
                        **preview_payload,
                        "excluded_row_hashes": [excluded_hash],
                        "output_dir": str(output_dir),
                        "confirm_large_export": False,
                    },
                )
                self.assertEqual(built.status_code, 200)
                built_payload = built.json()
                self.assertEqual(built_payload["total"], 1)
                self.assertEqual(len(built_payload["artifacts"]), 1)
                self.assertEqual(Path(built_payload["output_dir"]), (output_dir / "unit").resolve())
                xlsx_path = Path(built_payload["artifacts"][0]["path"])
                self.assertEqual(xlsx_path.parent, (output_dir / "unit").resolve())
                self.assertTrue(xlsx_path.exists())

                from openpyxl import load_workbook

                workbook = load_workbook(xlsx_path, read_only=False)
                worksheet = workbook.active
                headers = [cell.value for cell in worksheet[1]]
                self.assertEqual(headers, ["SKU", "Название", "Бренд", "Продажи, шт"])
                self.assertNotIn("__row_hash", headers)
                self.assertEqual(worksheet.auto_filter.ref, "A1:D2")

                downloaded = client.get("/api/exports/download-file", params={"path": str(xlsx_path)})
                self.assertEqual(downloaded.status_code, 200)

                csv_job = client.post(
                    "/api/exports/build-jobs",
                    json={
                        **preview_payload,
                        "excluded_row_hashes": [excluded_hash],
                        "output_dir": str(output_dir),
                        "confirm_large_export": False,
                        "export_format": "csv",
                    },
                )
                self.assertEqual(csv_job.status_code, 200)
                csv_job_payload = csv_job.json()
                self.assertIn(csv_job_payload["status"], {"queued", "running", "succeeded"})
                csv_job_id = csv_job_payload["id"]
                for _ in range(40):
                    csv_job_payload = client.get(f"/api/exports/build-jobs/{csv_job_id}").json()
                    if csv_job_payload["status"] == "succeeded":
                        break
                    time.sleep(0.05)
                self.assertEqual(csv_job_payload["status"], "succeeded")
                self.assertEqual(csv_job_payload["progress"], 100.0)
                csv_result = csv_job_payload["result"]
                self.assertEqual(csv_result["export_format"], "csv")
                self.assertEqual(csv_result["total"], 1)
                csv_path = Path(csv_result["artifacts"][0]["path"])
                self.assertEqual(csv_path.suffix, ".csv")
                self.assertEqual(csv_result["artifacts"][0]["format"], "csv")
                self.assertTrue(csv_path.exists())
                csv_text = csv_path.read_text(encoding="utf-8-sig")
                self.assertIn("SKU;Название;Бренд;Продажи, шт", csv_text.splitlines()[0])

                with patch("mpstats_app.services.export_service.EXCEL_MAX_DATA_ROWS", 1), patch(
                    "mpstats_app.services.export_service.EXPORT_BATCH_SIZE", 1
                ):
                    split = client.post(
                        "/api/exports/build",
                        json={
                            "project_name": "unit",
                            "category_keys": ["lemon-oz", "soap-wb"],
                            "period_from": "2025-01",
                            "period_to": "2025-02",
                            "selected_columns": ["SKU", "Название"],
                            "filters": [],
                            "excluded_row_hashes": [],
                            "sort_column": "SKU",
                            "sort_direction": "asc",
                            "split_by_category": True,
                            "output_dir": str(output_dir),
                            "confirm_large_export": False,
                        },
                    )
                self.assertEqual(split.status_code, 200)
                split_payload = split.json()
                self.assertEqual(split_payload["total"], 4)
                self.assertEqual(len(split_payload["artifacts"]), 4)
                self.assertEqual(
                    sorted(item["category_key"] for item in split_payload["artifacts"]),
                    ["lemon-oz", "lemon-oz", "lemon-oz", "soap-wb"],
                )

    def test_workflow_categories_classify_preview_and_save(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            input_file = root / "ready.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {
                            "Дата": "01.01.2025",
                            "Маркетплейс": "Ozon",
                            "Категория": "Лимонная кислота",
                            "SKU": "123",
                            "Бренд": "Brand",
                            "Название": "лимон тест",
                            "Продажи, шт": 5,
                        }
                    ]
                ),
                input_file,
            )

            with TestClient(app) as client:
                settings_response = client.put(
                    "/api/workflow/settings",
                    json={
                        "cookie": "session=test",
                        "project_name": "unit",
                        "workflow_mode": "historical_backfill",
                        "start_year": 2024,
                        "start_month": 1,
                        "end_year": 2026,
                        "end_month": 5,
                    },
                )
                self.assertEqual(settings_response.status_code, 200)
                self.assertEqual(settings_response.json()["project_name"], "unit")
                self.assertEqual(settings_response.json()["start_year"], 2024)
                self.assertEqual(settings_response.json()["end_month"], 5)

                loaded_settings = client.get("/api/workflow/settings")
                self.assertEqual(loaded_settings.status_code, 200)
                self.assertEqual(loaded_settings.json()["workflow_mode"], "historical_backfill")
                self.assertEqual(loaded_settings.json()["start_month"], 1)

                categories = client.get("/api/workflow/categories")
                self.assertEqual(categories.status_code, 200)
                category_rows = categories.json()["categories"]
                self.assertGreaterEqual(len(category_rows), 3)
                self.assertFalse(any(row["category_name"] == "Пустой путь" for row in category_rows))
                lemon_oz = next(row for row in category_rows if row["category_name"] == "Лимонная кислота" and row["mp_code"] == "oz")
                lemon_wb = next(row for row in category_rows if row["category_name"] == "Лимонная кислота" and row["mp_code"] == "wb")
                self.assertTrue(lemon_oz["fbs"])
                self.assertFalse(lemon_wb["fbs"])
                oil = next(row for row in category_rows if row["category_name"] == "Масло")
                self.assertIn("Подсолнечное", str(oil["filter_json"]))
                switched = [row for row in category_rows if row["category_name"] == "Смена пути"]
                self.assertEqual([row["period_from"] for row in switched], ["2025-01", "2025-02"])
                self.assertEqual([row["period_to"] for row in switched], ["2025-01", "2025-02"])

                source = client.get("/api/workflow/categories/source")
                self.assertEqual(source.status_code, 200)
                source_rows = source.json()["rows"]
                self.assertGreaterEqual(len(source_rows), 4)
                source_rows.append(
                    {
                        "active": True,
                        "category_name": "Новая категория",
                        "marketplace": "WB",
                        "fbs": True,
                        "period_from": "2025",
                        "period_to": "2025",
                        "comment": "unit",
                        "path": "Продукты/Новая",
                        "filter_text": "\"мыло\"&NOT\"хозяйственное\"",
                        "path2": "",
                        "filter2_text": "",
                        "actualization": "",
                    }
                )
                saved_source = client.put("/api/workflow/categories/source", json={"rows": source_rows})
                self.assertEqual(saved_source.status_code, 200)
                refreshed_categories = client.get("/api/workflow/categories").json()["categories"]
                self.assertTrue(any(row["category_name"] == "Новая категория" for row in refreshed_categories))
                new_category = next(row for row in refreshed_categories if row["category_name"] == "Новая категория")
                self.assertTrue(new_category["fbs"])
                new_filter = json.loads(new_category["filter_json"])
                self.assertEqual(new_filter["name"]["operator"], "AND")
                self.assertEqual(new_filter["name"]["condition1"]["type"], "contains")
                self.assertEqual(new_filter["name"]["condition2"]["type"], "notContains")

                classified = client.post(
                    "/api/workflow/classify",
                    json={"project_name": "unit", "input_file": str(input_file), "overwrite_input": False},
                )
                self.assertEqual(classified.status_code, 200)
                preview = classified.json()["preview"]
                self.assertEqual(preview["total"], 1)
                self.assertEqual(preview["rows"][0]["Тип"], "Кислота")

                uploaded = client.post(
                    "/api/workflow/classify-upload",
                    params={"project_name": "unit", "filename": "external.csv", "write_xlsx": False},
                    content="Название,SKU\nлимон внешний,456\n".encode("utf-8"),
                    headers={"content-type": "text/csv"},
                )
                self.assertEqual(uploaded.status_code, 200)
                uploaded_payload = uploaded.json()
                self.assertNotEqual(uploaded_payload["input_file"], uploaded_payload["output_file"])
                self.assertTrue(Path(uploaded_payload["output_file"]).exists())
                self.assertIn("external_classification", uploaded_payload["output_file"])
                self.assertEqual(uploaded_payload["preview"]["total"], 1)
                self.assertEqual(uploaded_payload["preview"]["rows"][0]["Тип"], "Кислота")
                downloaded = client.get("/api/workflow/download-file", params={"path": uploaded_payload["output_file"]})
                self.assertEqual(downloaded.status_code, 200)
                self.assertIn("Кислота", downloaded.text)

                if _has_openpyxl():
                    xlsx_buffer = BytesIO()
                    pd.DataFrame([{"Название": "лимон excel", "SKU": "789"}]).to_excel(xlsx_buffer, index=False)
                    uploaded_xlsx = client.post(
                        "/api/workflow/classify-upload",
                        params={"project_name": "unit", "filename": "external.xlsx", "write_xlsx": False},
                        content=xlsx_buffer.getvalue(),
                        headers={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
                    )
                    self.assertEqual(uploaded_xlsx.status_code, 200)
                    self.assertEqual(uploaded_xlsx.json()["preview"]["rows"][0]["Тип"], "Кислота")

                saved = client.post(
                    "/api/workflow/save-to-db",
                    json={"project_name": "unit", "file_path": classified.json()["output_file"]},
                )
                self.assertEqual(saved.status_code, 200)
                self.assertEqual(saved.json()["rows"], 1)

    def test_smart_plan_compares_expected_tasks_with_local_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                categories = client.get("/api/workflow/categories").json()["categories"]
                category_ids = [
                    row["category_id"]
                    for row in categories
                    if row["category_name"] in {"Лимонная кислота", "Масло"}
                ]
                plan_response = client.post(
                    "/api/workflow/pipeline/plans",
                    json={
                        "project_name": "smart-plan-unit",
                        "run_type": "historical_backfill",
                        "category_ids": category_ids,
                        "start_year": 2025,
                        "start_month": 1,
                        "end_year": 2025,
                        "end_month": 2,
                        "settings": {
                            "overwrite_raw": False,
                            "overwrite_processed": False,
                            "overwrite_db": False,
                            "max_parallel_downloads": 1,
                            "retry_count": 0,
                            "timeout_seconds": 300,
                            "pause_between_requests": 0,
                            "max_weight_kg": 40,
                        },
                    },
                )
                self.assertEqual(plan_response.status_code, 200)
                run_id = plan_response.json()["id"]
                tasks = client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks").json()["tasks"]
                self.assertEqual(len(tasks), 6)

                write_semicolon_csv(pd.DataFrame([{"SKU": "raw-only"}]), Path(tasks[1]["raw_file_path"]))
                write_semicolon_csv(pd.DataFrame([{"SKU": "ready"}]), Path(tasks[2]["processed_file_path"]))
                write_semicolon_csv(pd.DataFrame([{"SKU": "ready"}]), Path(tasks[2]["classified_file_path"]))
                write_semicolon_csv(pd.DataFrame([{"SKU": "old-processed"}]), Path(tasks[3]["processed_file_path"]))
                write_semicolon_csv(pd.DataFrame([{"SKU": "new-raw"}]), Path(tasks[3]["raw_file_path"]))
                old_time = 1_700_000_000
                new_time = old_time + 100
                os.utime(tasks[3]["processed_file_path"], (old_time, old_time))
                os.utime(tasks[3]["raw_file_path"], (new_time, new_time))
                app.state.repository.update_download_task(tasks[4]["id"], {"status": "failed", "error_message": "unit failure"})

                smart_plan = client.get(f"/api/workflow/pipeline/runs/{run_id}/smart-plan")
                self.assertEqual(smart_plan.status_code, 200)
                summary = smart_plan.json()["summary"]
                self.assertEqual(summary["total"], 6)
                self.assertEqual(summary["missing"], 2)
                self.assertEqual(summary["incomplete"], 1)
                self.assertEqual(summary["ready"], 1)
                self.assertEqual(summary["stale"], 1)
                self.assertEqual(summary["failed"], 1)
                self.assertEqual(smart_plan.json()["recommended_action"]["key"], "retry_failed")

                ready_only = client.get(f"/api/workflow/pipeline/runs/{run_id}/smart-plan", params={"status": "ready"})
                self.assertEqual(ready_only.status_code, 200)
                self.assertEqual(len(ready_only.json()["tasks"]), 1)
                self.assertEqual(ready_only.json()["tasks"][0]["recommended_action"], "Собрать куб из готовых файлов")

    def test_smart_pipeline_plan_rebuild_dedup_retry_and_monthly_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                categories = client.get("/api/workflow/categories").json()["categories"]
                category_ids = [row["category_id"] for row in categories if row["category_name"] == "Лимонная кислота"][:2]

                plan_response = client.post(
                    "/api/workflow/pipeline/plans",
                    json={
                        "project_name": "unit",
                        "run_type": "historical_backfill",
                        "category_ids": category_ids,
                        "start_year": 2025,
                        "start_month": 1,
                        "end_year": 2025,
                        "end_month": 2,
                        "settings": {
                            "overwrite_raw": False,
                            "overwrite_processed": False,
                            "overwrite_db": False,
                            "max_parallel_downloads": 1,
                            "retry_count": 0,
                            "timeout_seconds": 300,
                            "pause_between_requests": 0,
                            "max_weight_kg": 40,
                        },
                    },
                )
                self.assertEqual(plan_response.status_code, 200)
                run_id = plan_response.json()["id"]
                tasks_response = client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks")
                self.assertEqual(tasks_response.status_code, 200)
                tasks = tasks_response.json()["tasks"]
                self.assertEqual(len(tasks), 4)
                smart_pipeline = app.state.smart_pipeline_service
                oz_task = next(task for task in tasks if task["marketplace_code"] == "oz")
                wb_task = next(task for task in tasks if task["marketplace_code"] == "wb")
                self.assertEqual(smart_pipeline._task_to_export_task(oz_task).get("fbs"), 1)
                self.assertNotIn("fbs", smart_pipeline._task_to_export_task(wb_task))

                switched_ids = [row["category_id"] for row in categories if row["category_name"] == "Смена пути"]
                switched_plan = client.post(
                    "/api/workflow/pipeline/plans",
                    json={
                        "project_name": "period-unit",
                        "run_type": "historical_backfill",
                        "category_ids": switched_ids,
                        "start_year": 2025,
                        "start_month": 1,
                        "end_year": 2025,
                        "end_month": 2,
                        "settings": {
                            "overwrite_raw": False,
                            "overwrite_processed": False,
                            "overwrite_db": False,
                            "max_parallel_downloads": 1,
                            "retry_count": 0,
                            "timeout_seconds": 300,
                            "pause_between_requests": 0,
                            "max_weight_kg": 40,
                        },
                    },
                )
                self.assertEqual(switched_plan.status_code, 200)
                switched_tasks = client.get(f"/api/workflow/pipeline/runs/{switched_plan.json()['id']}/tasks").json()["tasks"]
                self.assertEqual([(task["year"], task["month"], task["category_path"]) for task in switched_tasks], [
                    (2025, 1, "Продукты/Старый путь"),
                    (2025, 2, "Продукты/Новый путь"),
                ])

                for index, task in enumerate(tasks):
                    write_semicolon_csv(
                        pd.DataFrame(
                            [
                                {
                                    "Маркетплейс": task["marketplace"],
                                    "Категория": task["category_name"],
                                    "Артикул": f"sku-{index}",
                                    "SKU": f"лимон исправленный {index}",
                                    "Бренд": "brand",
                                    "Продажи, шт": 5,
                                    "Средняя цена, руб": 10,
                                    "Выручка, руб": 50,
                                }
                            ]
                        ),
                        Path(task["processed_file_path"]),
                    )
                    write_semicolon_csv(
                        pd.DataFrame(
                            [
                                {
                                    "Маркетплейс": task["marketplace"],
                                    "Категория": task["category_name"],
                                    "Артикул": f"sku-{index}",
                                    "SKU": f"лимон тест {index}",
                                    "Бренд": "brand",
                                    "Тип": "Старая классификация",
                                    "Продажи, шт": 5,
                                    "Средняя цена, руб": 10,
                                    "Выручка, руб": 50,
                                }
                            ]
                        ),
                        Path(task["classified_file_path"]),
                    )

                rebuilt = client.post(f"/api/workflow/pipeline/runs/{run_id}/rebuild-cube", json={"wait": True})
                self.assertEqual(rebuilt.status_code, 200)
                self.assertEqual(rebuilt.json()["completed_tasks"], 4)
                self.assertEqual(rebuilt.json()["failed_tasks"], 0)

                search = client.get("/api/products", params={"query": "лимон", "limit": 100})
                self.assertEqual(search.status_code, 200)
                self.assertEqual(search.json()["total"], 4)

                rebuilt_again = client.post(f"/api/workflow/pipeline/runs/{run_id}/rebuild-cube", json={"wait": True})
                self.assertEqual(rebuilt_again.status_code, 200)
                search_again = client.get("/api/products", params={"query": "лимон", "limit": 100})
                self.assertEqual(search_again.json()["total"], 4)

                reclassified = client.post(f"/api/workflow/pipeline/runs/{run_id}/reclassify-cube", json={"wait": True})
                self.assertEqual(reclassified.status_code, 200)
                self.assertEqual(reclassified.json()["completed_tasks"], 4)
                self.assertEqual(reclassified.json()["failed_tasks"], 0)
                corrected_search = client.get("/api/products", params={"query": "исправленный", "limit": 100})
                self.assertEqual(corrected_search.status_code, 200)
                corrected_payload = corrected_search.json()
                self.assertEqual(corrected_payload["total"], 4)
                self.assertEqual({row["Тип"] for row in corrected_payload["rows"]}, {"Кислота"})
                old_search = client.get("/api/products", params={"query": "тест", "limit": 100})
                self.assertEqual(old_search.json()["total"], 0)

                repository: DuckDbAppRepository = app.state.repository
                repository.update_download_task(tasks[0]["id"], {"status": "failed", "error_message": "unit failure"})
                retry_errors = client.post(f"/api/workflow/pipeline/runs/{run_id}/retry-errors", json={"wait": True})
                self.assertEqual(retry_errors.status_code, 200)
                retried_error_task = client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks").json()["tasks"][0]
                self.assertEqual(retried_error_task["status"], "saved_to_db")

                repository.update_download_task(tasks[1]["id"], {"status": "failed", "error_message": "unit failure"})
                retry = client.post(f"/api/workflow/pipeline/tasks/{tasks[1]['id']}/retry", json={"wait": True})
                self.assertEqual(retry.status_code, 200)
                retried_task = next(
                    task for task in client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks").json()["tasks"]
                    if task["id"] == tasks[1]["id"]
                )
                self.assertEqual(retried_task["status"], "saved_to_db")

                monthly = client.post(
                    "/api/workflow/pipeline/monthly-sync",
                    json={
                        "project_name": "unit",
                        "start_immediately": False,
                        "wait": False,
                        "settings": {
                            "overwrite_raw": False,
                            "overwrite_processed": False,
                            "overwrite_db": False,
                            "max_parallel_downloads": 1,
                            "retry_count": 0,
                            "timeout_seconds": 300,
                            "pause_between_requests": 0,
                            "max_weight_kg": 40,
                        },
                    },
                )
                self.assertEqual(monthly.status_code, 200)
                self.assertEqual(monthly.json()["period_from"], "2025-03")
                self.assertEqual(monthly.json()["period_to"], "2025-03")

    def test_smart_pipeline_pause_and_stop_interrupt_background_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                categories = client.get("/api/workflow/categories").json()["categories"]
                category_ids = [row["category_id"] for row in categories if row["category_name"] == "Лимонная кислота"][:2]

                def create_ready_plan(project_name: str) -> tuple[str, list[dict[str, object]]]:
                    plan_response = client.post(
                        "/api/workflow/pipeline/plans",
                        json={
                            "project_name": project_name,
                            "run_type": "historical_backfill",
                            "category_ids": category_ids,
                            "start_year": 2025,
                            "start_month": 1,
                            "end_year": 2025,
                            "end_month": 1,
                            "settings": {
                                "overwrite_raw": False,
                                "overwrite_processed": False,
                                "overwrite_db": False,
                                "max_parallel_downloads": 1,
                                "retry_count": 0,
                                "timeout_seconds": 300,
                                "pause_between_requests": 0,
                                "max_weight_kg": 40,
                            },
                        },
                    )
                    self.assertEqual(plan_response.status_code, 200)
                    run_id = plan_response.json()["id"]
                    tasks = client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks").json()["tasks"]
                    for index, task in enumerate(tasks):
                        write_semicolon_csv(pd.DataFrame([{"SKU": f"готовый {project_name} {index}"}]), Path(str(task["classified_file_path"])))
                    return run_id, tasks

                pause_run_id, pause_tasks = create_ready_plan("pause-unit")
                service = app.state.smart_pipeline_service
                original_classify = service._classify_task
                pause_started = Event()
                pause_release = Event()

                def classify_and_wait_for_pause(task_id: str, *, settings: dict[str, object], force_reclassify: bool = False) -> None:
                    original_classify(task_id, settings=settings, force_reclassify=force_reclassify)
                    if task_id == pause_tasks[0]["id"]:
                        pause_started.set()
                        self.assertTrue(pause_release.wait(2.0))

                with patch.object(service, "_classify_task", side_effect=classify_and_wait_for_pause):
                    started = client.post(f"/api/workflow/pipeline/runs/{pause_run_id}/rebuild-cube", json={"wait": False})
                    self.assertEqual(started.status_code, 200)
                    self.assertTrue(pause_started.wait(2.0))
                    paused_request = client.post(f"/api/workflow/pipeline/runs/{pause_run_id}/pause")
                    self.assertEqual(paused_request.status_code, 200)
                    self.assertEqual(paused_request.json()["status"], "pausing")
                    pause_release.set()

                    paused_payload = {}
                    for _ in range(40):
                        paused_payload = client.get(f"/api/workflow/pipeline/runs/{pause_run_id}").json()
                        if paused_payload["status"] == "paused":
                            break
                        time.sleep(0.05)
                    self.assertEqual(paused_payload["status"], "paused")

                paused_tasks = client.get(f"/api/workflow/pipeline/runs/{pause_run_id}/tasks").json()["tasks"]
                self.assertEqual(paused_tasks[0]["status"], "classified")
                self.assertEqual(paused_tasks[0]["save_status"], "pending")
                self.assertEqual(paused_tasks[1]["status"], "pending")

                stop_run_id, stop_tasks = create_ready_plan("stop-unit")
                stop_started = Event()
                stop_release = Event()

                def classify_and_wait_for_stop(task_id: str, *, settings: dict[str, object], force_reclassify: bool = False) -> None:
                    original_classify(task_id, settings=settings, force_reclassify=force_reclassify)
                    if task_id == stop_tasks[0]["id"]:
                        stop_started.set()
                        self.assertTrue(stop_release.wait(2.0))

                with patch.object(service, "_classify_task", side_effect=classify_and_wait_for_stop):
                    started = client.post(f"/api/workflow/pipeline/runs/{stop_run_id}/rebuild-cube", json={"wait": False})
                    self.assertEqual(started.status_code, 200)
                    self.assertTrue(stop_started.wait(2.0))
                    stopped_request = client.post(f"/api/workflow/pipeline/runs/{stop_run_id}/stop")
                    self.assertEqual(stopped_request.status_code, 200)
                    self.assertEqual(stopped_request.json()["status"], "stopping")
                    self.assertTrue(app.state.repository.is_pipeline_stop_requested(stop_run_id))
                    stop_release.set()

                    stopped_payload = {}
                    for _ in range(40):
                        stopped_payload = client.get(f"/api/workflow/pipeline/runs/{stop_run_id}").json()
                        if stopped_payload["status"] == "stopped":
                            break
                        time.sleep(0.05)
                    self.assertEqual(stopped_payload["status"], "stopped")
                    self.assertEqual(stopped_payload["current_step"], "Остановлено")

                stopped_tasks = client.get(f"/api/workflow/pipeline/runs/{stop_run_id}/tasks").json()["tasks"]
                self.assertEqual(stopped_tasks[0]["status"], "classified")
                self.assertEqual(stopped_tasks[0]["save_status"], "pending")
                self.assertEqual(stopped_tasks[1]["status"], "pending")

    def test_smart_pipeline_deletes_cube_file_and_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                categories = client.get("/api/workflow/categories").json()["categories"]
                category_id = next(row["category_id"] for row in categories if row["category_name"] == "Лимонная кислота")
                plan_response = client.post(
                    "/api/workflow/pipeline/plans",
                    json={
                        "project_name": "unit",
                        "run_type": "historical_backfill",
                        "category_ids": [category_id],
                        "start_year": 2025,
                        "start_month": 1,
                        "end_year": 2025,
                        "end_month": 1,
                        "settings": {
                            "overwrite_raw": False,
                            "overwrite_processed": False,
                            "overwrite_db": False,
                            "max_parallel_downloads": 1,
                            "retry_count": 0,
                            "timeout_seconds": 300,
                            "pause_between_requests": 0,
                            "max_weight_kg": 40,
                        },
                    },
                )
                self.assertEqual(plan_response.status_code, 200)
                run_id = plan_response.json()["id"]
                task = client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks").json()["tasks"][0]
                processed_path = Path(task["processed_file_path"])
                classified_path = Path(task["classified_file_path"])
                processed_path.parent.mkdir(parents=True, exist_ok=True)
                source_frame = pd.DataFrame(
                    [
                        {
                            "Маркетплейс": task["marketplace"],
                            "Категория": task["category_name"],
                            "Артикул": "sku-1",
                            "SKU": "лимон удаление",
                            "Бренд": "brand",
                            "Тип": "Кислота",
                            "Продажи, шт": 5,
                        }
                    ]
                )
                write_semicolon_csv(source_frame, processed_path)
                write_semicolon_csv(source_frame, classified_path)

                rebuilt = client.post(f"/api/workflow/pipeline/runs/{run_id}/rebuild-cube", json={"wait": True})
                self.assertEqual(rebuilt.status_code, 200)
                self.assertEqual(rebuilt.json()["completed_tasks"], 1)
                cube_entry = client.get("/api/workflow/pipeline/cube", params={"project_name": "unit"}).json()["items"][0]
                search = client.get("/api/products", params={"project_name": "unit", "query": "удаление", "limit": 100})
                self.assertEqual(search.json()["total"], 1)

                deleted_cube = client.delete(f"/api/workflow/pipeline/cube/{cube_entry['id']}")
                self.assertEqual(deleted_cube.status_code, 200)
                self.assertEqual(deleted_cube.json()["deleted"]["cube_registry"], 1)
                self.assertEqual(deleted_cube.json()["deleted"]["product_rows"], 1)
                self.assertEqual(client.get("/api/workflow/pipeline/cube", params={"project_name": "unit"}).json()["items"], [])
                task_after_cube_delete = client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks").json()["tasks"][0]
                self.assertEqual(task_after_cube_delete["status"], "classified")
                self.assertEqual(task_after_cube_delete["save_status"], "pending")
                search_after_cube_delete = client.get("/api/products", params={"project_name": "unit", "query": "удаление", "limit": 100})
                self.assertEqual(search_after_cube_delete.json()["total"], 0)

                rebuilt_again = client.post(f"/api/workflow/pipeline/runs/{run_id}/rebuild-cube", json={"wait": True})
                self.assertEqual(rebuilt_again.status_code, 200)
                self.assertEqual(rebuilt_again.json()["completed_tasks"], 1)
                deleted_file = client.delete(
                    "/api/workflow/pipeline/files",
                    params={"project_name": "unit", "path": str(classified_path), "delete_cube": True},
                )
                self.assertEqual(deleted_file.status_code, 200)
                self.assertFalse(classified_path.exists())
                self.assertEqual(deleted_file.json()["deleted"]["files"], 1)
                self.assertEqual(deleted_file.json()["deleted"]["download_tasks"], 1)
                self.assertEqual(deleted_file.json()["cube_deletions"][0]["deleted"]["cube_registry"], 1)
                self.assertEqual(client.get("/api/workflow/pipeline/cube", params={"project_name": "unit"}).json()["items"], [])
                task_after_file_delete = client.get(f"/api/workflow/pipeline/runs/{run_id}/tasks").json()["tasks"][0]
                self.assertEqual(task_after_file_delete["classify_status"], "pending")
                self.assertEqual(task_after_file_delete["save_status"], "pending")

                deleted_plan = client.delete(f"/api/workflow/pipeline/runs/{run_id}")
                self.assertEqual(deleted_plan.status_code, 200)
                self.assertEqual(deleted_plan.json()["deleted"]["pipeline_runs"], 1)
                self.assertEqual(deleted_plan.json()["deleted"]["download_tasks"], 1)
                runs = client.get("/api/workflow/pipeline/runs", params={"project_name": "unit"}).json()["runs"]
                self.assertNotIn(run_id, {row["id"] for row in runs})

    def test_smart_pipeline_files_classify_kinds_and_hide_merged(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            project_root = root / "data" / "projects" / "unit"
            files = {
                "raw/2025-01/oz/sugar.csv": "raw",
                "processed/2025-01/oz/sugar.csv": "processed",
                "processed/2025-01/oz/sugar_classified.csv": "classified",
                "exports/MPStats_unit.xlsx": "export",
                "merged/cube_unit_2025_01.csv": "merged",
            }
            for relative_path in files:
                path = project_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("sku\n1\n", encoding="utf-8")

            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                response = client.get("/api/workflow/pipeline/files", params={"project_name": "unit"})

            self.assertEqual(response.status_code, 200)
            rows = response.json()["files"]
            by_relative_path = {row["relative_path"]: row for row in rows}
            self.assertEqual(by_relative_path["raw/2025-01/oz/sugar.csv"]["kind"], "raw")
            self.assertEqual(by_relative_path["processed/2025-01/oz/sugar.csv"]["kind"], "processed")
            self.assertEqual(by_relative_path["processed/2025-01/oz/sugar_classified.csv"]["kind"], "classified")
            self.assertEqual(by_relative_path["exports/MPStats_unit.xlsx"]["kind"], "export")
            self.assertNotIn("merged/cube_unit_2025_01.csv", by_relative_path)

    def test_quality_projects_report_and_missing_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_project(root)
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Название": "Лимон 1 кг", "Маркетплейс": "Ozon", "Категория": "Кислота", "SKU": "sku-1", "Вес, кг": 1.0, "Подкатегория": "Лимонная"},
                        {"Название": "Лимон 2 кг", "Маркетплейс": "WB", "Категория": "Кислота", "SKU": "sku-2", "Вес, кг": 2.0, "Подкатегория": "Лимонная"},
                    ]
                ),
                root / "pipeline" / "03_unit_merged_classified.csv",
            )
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Название": "Мыло 1 кг", "Маркетплейс": "Ozon", "Категория": "Мыло", "SKU": "soap-1", "Вес, кг": 1.0},
                    ]
                ),
                root / "pipeline" / "03_merged_only_merged.csv",
            )
            settings = make_settings(root)
            app = create_app(settings, start_workers=False)

            with TestClient(app) as client:
                projects = client.get("/api/quality/projects")
                self.assertEqual(projects.status_code, 200)
                rows = projects.json()["projects"]
                by_name = {row["project_name"]: row for row in rows}
                self.assertEqual(by_name["unit"]["source_kind"], "classified")
                self.assertTrue(by_name["merged_only"]["fallback_used"])

                report = client.get("/api/quality/report", params={"project_name": "unit"})
                self.assertEqual(report.status_code, 200)
                payload = report.json()
                self.assertEqual(payload["status"], "OK")
                self.assertEqual(payload["total_rows"], 2)
                self.assertEqual(payload["source"]["kind"], "classified")

                missing = client.get("/api/quality/report", params={"project_name": "missing"})
                self.assertEqual(missing.status_code, 404)
                self.assertIn("не найден", missing.json()["detail"])


if __name__ == "__main__":
    unittest.main()
