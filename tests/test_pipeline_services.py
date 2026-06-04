from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from classifiers.engine import apply_classifiers, default_rules_path
from pipeline.repositories.file_repository import read_semicolon_csv, write_semicolon_csv
from pipeline.services.classification_service import classify_file
from pipeline.services.enrich_service import (
    extract_category_from_filename,
    extract_first_date_from_filename,
    extract_marketplace_from_filename,
)
from pipeline.services.merge_service import merge_csv_files_with_duckdb, merge_dataframes, merge_directory
from pipeline.services.run_service import parse_steps
from scripts.classifier_perf_utils import classifier_sizes_for_args, compare_classified_outputs
from scripts.duckdb_benchmark import merge_sizes_for_args


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

    def test_classification_preserves_date_column(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rules_file = root / "rules.csv"
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        "1;1;*;Подкатегория;Название;contains;лимон;Лимонная;fill_empty;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            input_file = root / "classified-input.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {
                            "Дата": "01.06.2025",
                            "Маркетплейс": "Ozon",
                            "Категория": "Тест",
                            "SKU": "sku-1",
                            "Название": "лимон 1 кг",
                        }
                    ]
                ),
                input_file,
            )

            result, _, _ = classify_file(input_file, root / "classified-output.csv", rules_path=rules_file)

            self.assertEqual(result["Дата"].tolist(), ["01.06.2025"])
            self.assertEqual(result["месяц"].tolist(), ["июн."])

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

    def test_duckdb_merge_matches_pandas_merge_and_preserves_first_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            input_dir.mkdir()
            file_a = input_dir / "a.csv"
            file_b = input_dir / "b.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"SKU": "a", "Продажи": "10", "Название": "one"},
                        {"SKU": "a", "Продажи": "10", "Название": "one"},
                        {"SKU": "b", "Продажи": "0", "Название": "two"},
                        {"SKU": "c", "Продажи": "50000", "Название": "three"},
                    ]
                ),
                file_a,
            )
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"SKU": "d", "Продажи": "11", "Название": "four"},
                        {"SKU": "a", "Продажи": "10", "Название": "one"},
                        {"SKU": "e", "Продажи": "12", "Название": "five"},
                    ]
                ),
                file_b,
            )

            old_frame = merge_dataframes([read_semicolon_csv(file_a), read_semicolon_csv(file_b)], min_sales=0, max_sales=40_000)
            old_output = root / "old.csv"
            write_semicolon_csv(old_frame, old_output)
            new_output = root / "new.csv"

            result = merge_csv_files_with_duckdb([file_a, file_b], new_output, min_sales=0, max_sales=40_000)

            self.assertTrue(new_output.exists())
            self.assertGreater(new_output.stat().st_size, 0)
            self.assertTrue(new_output.read_bytes().startswith(b"\xef\xbb\xbf"))
            self.assertEqual(new_output.read_text(encoding="utf-8-sig").splitlines()[0], "SKU;Продажи, шт;Название")
            self.assertEqual(result.rows_in, 7)
            self.assertEqual(result.filtered_rows, 5)
            self.assertEqual(result.rows_out, 3)
            self.assertEqual(result.duplicates_removed, 2)
            self.assertEqual(result.input_files_count, 2)

            old_saved = read_semicolon_csv(old_output)
            new_saved = read_semicolon_csv(new_output)
            self.assertEqual(list(new_saved.columns), list(old_saved.columns))
            self.assertEqual(new_saved["SKU"].tolist(), ["a", "d", "e"])
            pd.testing.assert_frame_equal(old_saved, new_saved, check_dtype=False)

    def test_merge_directory_uses_duckdb_without_pandas_concat(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            input_dir.mkdir()
            write_semicolon_csv(pd.DataFrame([{"SKU": "a", "Продажи, шт": 1, "Название": "one"}]), input_dir / "a.csv")
            output_file = root / "merged.csv"

            with patch("pipeline.services.merge_service.pd.concat", side_effect=AssertionError("merge_directory must not use pandas concat")):
                merged, step = merge_directory(input_dir, output_file)

            self.assertTrue(output_file.exists())
            self.assertEqual(step.ok, 1)
            self.assertEqual(step.rows, 1)
            self.assertEqual(merged.rows_out, 1)
            saved = read_semicolon_csv(output_file)
            self.assertEqual(saved["SKU"].tolist(), ["a"])

    def test_duckdb_merge_subset_dedup_keeps_first_row(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            file_path = root / "input.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"SKU": "a", "Продажи, шт": 10, "Название": "first"},
                        {"SKU": "a", "Продажи, шт": 11, "Название": "second"},
                        {"SKU": "b", "Продажи, шт": 12, "Название": "third"},
                    ]
                ),
                file_path,
            )
            output_file = root / "out.csv"

            result = merge_csv_files_with_duckdb([file_path], output_file, dedup_columns=["SKU"])

            self.assertEqual(result.rows_out, 2)
            self.assertEqual(result.duplicates_removed, 1)
            saved = read_semicolon_csv(output_file)
            self.assertEqual(saved["SKU"].tolist(), ["a", "b"])
            self.assertEqual(saved["Название"].tolist(), ["first", "third"])

    def test_merge_benchmark_large_requires_explicit_flag(self) -> None:
        self.assertEqual(
            merge_sizes_for_args(size="small", all_sizes=True, include_large_merge=False),
            ["small", "medium"],
        )
        self.assertEqual(
            merge_sizes_for_args(size="small", all_sizes=True, include_large_merge=True),
            ["small", "medium", "large"],
        )
        with self.assertRaises(ValueError):
            merge_sizes_for_args(size="large", all_sizes=False, include_large_merge=False)

    def test_classifier_benchmark_large_requires_explicit_flag(self) -> None:
        self.assertEqual(
            classifier_sizes_for_args(size="small", all_sizes=True, include_large=False),
            ["small", "medium"],
        )
        self.assertEqual(
            classifier_sizes_for_args(size="small", all_sizes=True, include_large=True),
            ["small", "medium", "large"],
        )
        with self.assertRaises(ValueError):
            classifier_sizes_for_args(size="large", all_sizes=False, include_large=False)

    def test_classifier_output_comparison_reports_differences(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            baseline = root / "baseline.csv"
            candidate = root / "candidate.csv"
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Категория": "Мясо", "Подкатегория": "Кулинария", "Тип": "Котлеты"},
                        {"Категория": "Мясо", "Подкатегория": "Прочее", "Тип": ""},
                    ]
                ),
                baseline,
            )
            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Категория": "Мясо", "Подкатегория": "Кулинария", "Тип": "Котлеты"},
                        {"Категория": "Мясо", "Подкатегория": "Маринады", "Тип": ""},
                    ]
                ),
                candidate,
            )

            comparison = compare_classified_outputs(baseline, candidate, columns=["Подкатегория", "Тип"])

            self.assertTrue(comparison["row_count_match"])
            self.assertTrue(comparison["columns_match"])
            self.assertEqual(comparison["diff_counts"]["Подкатегория"], 1)
            self.assertEqual(comparison["diff_counts"]["Тип"], 0)
            self.assertEqual(comparison["first_differences"][0]["row_index"], 1)

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
            self.assertIn("месяц", result.columns)
            self.assertEqual(result["месяц"].tolist(), ["янв.", "янв."])
            self.assertEqual(result.iloc[0]["Подкатегория"], "Мясо")
            self.assertEqual(result.iloc[1]["Подкатегория"], "Прочее")
            self.assertEqual(result.iloc[0]["Тип"], "Прочие")
            saved = read_semicolon_csv(output_file)
            self.assertIn("месяц", saved.columns)
            self.assertEqual(saved["месяц"].tolist(), ["янв.", "янв."])

    def test_otherwise_rule_never_overwrites_filled_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_file = root / "input.csv"
            output_file = root / "output.csv"
            rules_file = root / "rules.csv"

            write_semicolon_csv(
                pd.DataFrame(
                    [
                        {"Категория": "Мясо", "Название": "мясной продукт"},
                        {"Категория": "Мясо", "Название": "непонятный продукт"},
                    ]
                ),
                input_file,
            )
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        "1;1;*;Подкатегория;Название;contains;мясной;Мясо;fill_empty;;",
                        "1;999;*;Подкатегория;;otherwise;;Прочее;overwrite;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result, report, _ = classify_file(input_file, output_file, rules_path=rules_file, write_xlsx=False)

            self.assertEqual(report["applied_rows"].sum(), 2)
            self.assertEqual(result["Подкатегория"].tolist(), ["Мясо", "Прочее"])

    def test_contains_rule_treats_regex_special_chars_as_literal_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rules_file = root / "rules.csv"
            pattern = "a.b+(x)[y]*?"
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        f"1;1;*;Подкатегория;Название;contains;{pattern};Literal;fill_empty;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            frame = pd.DataFrame(
                [
                    {"Категория": "Тест", "Название": f"prefix {pattern} suffix"},
                    {"Категория": "Тест", "Название": "prefix axbxxxy suffix"},
                ]
            )

            result, report = apply_classifiers(frame, rules_file)

            self.assertEqual(int(report.iloc[0]["applied_rows"]), 1)
            self.assertEqual(result["Подкатегория"].fillna("").tolist(), ["Literal", ""])

    def test_numeric_classifier_rules_compare_weight_kg(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rules_file = root / "rules.csv"
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        "1;1;*;Вес lt;Вес, кг;lt;10 кг;yes;fill_empty;;",
                        "1;2;*;Вес lte;Вес, кг;lte;10;yes;fill_empty;;",
                        "1;3;*;Вес gt;Вес, кг;gt;10,0;yes;fill_empty;;",
                        "1;4;*;Вес gte;Вес, кг;gte;10.0;yes;fill_empty;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            frame = pd.DataFrame(
                [
                    {"Категория": "Тест", "Название": "малый", "Вес, кг": 9.5},
                    {"Категория": "Тест", "Название": "ровно", "Вес, кг": "10,0"},
                    {"Категория": "Тест", "Название": "большой", "Вес, кг": 12},
                    {"Категория": "Тест", "Название": "без веса", "Вес, кг": ""},
                ]
            )

            result, report = apply_classifiers(frame, rules_file)

            self.assertEqual(result["Вес lt"].fillna("").tolist(), ["yes", "", "", ""])
            self.assertEqual(result["Вес lte"].fillna("").tolist(), ["yes", "yes", "", ""])
            self.assertEqual(result["Вес gt"].fillna("").tolist(), ["", "", "yes", ""])
            self.assertEqual(result["Вес gte"].fillna("").tolist(), ["", "yes", "yes", ""])
            self.assertEqual(report["candidate_rows"].tolist(), [1, 2, 1, 2])

    def test_numeric_classifier_rule_rejects_non_numeric_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rules_file = root / "rules.csv"
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        "1;1;*;Группа веса;Вес, кг;lt;десять;До 10 кг;fill_empty;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            frame = pd.DataFrame([{"Категория": "Тест", "Название": "малый", "Вес, кг": 9.5}])

            with self.assertRaisesRegex(ValueError, "requires a numeric pattern"):
                apply_classifiers(frame, rules_file)

    def test_category_prefilter_uses_category_changed_by_previous_rule(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rules_file = root / "rules.csv"
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        "1;1;Мыло хозяйственное;Категория;Категория;equals;Мыло хозяйственное;Мыло;overwrite;;",
                        "1;2;Мыло;Подкатегория;Название;contains;жидк;Жидкое;fill_empty;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            frame = pd.DataFrame(
                [
                    {"Категория": "Мыло хозяйственное", "Название": "Жидкое хозяйственное мыло"},
                ]
            )

            result, report = apply_classifiers(frame, rules_file)

            self.assertEqual(report["applied_rows"].tolist(), [1, 1])
            self.assertEqual(result.iloc[0]["Категория"], "Мыло")
            self.assertEqual(result.iloc[0]["Подкатегория"], "Жидкое")

    def test_category_prefilter_skips_match_when_no_candidate_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            rules_file = root / "rules.csv"
            rules_file.write_text(
                "\n".join(
                    [
                        "active;priority;category;target_column;match_field;match_type;pattern;set_value;mode;comment;conditions_json",
                        "1;1;Мясо;Подкатегория;Название;regex;.*;Мясо;fill_empty;;",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            frame = pd.DataFrame([{"Категория": "Мыло", "Название": "Жидкое мыло"}])

            with patch("classifiers.engine._build_match_mask", side_effect=AssertionError("match should be skipped")):
                result, report = apply_classifiers(frame, rules_file)

            self.assertEqual(int(report.iloc[0]["candidate_rows"]), 0)
            self.assertEqual(int(report.iloc[0]["applied_rows"]), 0)
            self.assertTrue(result["Подкатегория"].isna().all())

    def test_default_meat_rules_follow_reference_categories(self) -> None:
        cases = [
            ("Котлеты Три мяса Слово Мясника, 360 г", "Кулинария"),
            ("Биточки для всей семьи Слово Мясника, 360 г", "Кулинария"),
            ("Митболы Скандинавские Слово Мясника из говядины и свинины, 360 г", "Кулинария"),
            ("Сатэ по-тайски Слово Мясника, 700 г", "Маринады"),
            ("Слово мясника Корейка свиная Каджун охлажденная, 0.7-0.8кг", "Маринады"),
            ("Корейка свиная без кости Слово Мясника, 800 г", "Крупнокусковые"),
            ("Чевапчичи Слово Мясника из свинины и говядины, 300 г", "Колбаски"),
            ("Гуляш свиной Слово Мясника 400 г", "Мелкокусковые"),
            ("Шашлык свиной Слово мясника Классический в маринаде охлажденный, 1.7кг", "Шашлык"),
            ("Ребрышки свиные в ягодном маринаде Слово Мясника, 700 г", "Ребрышки маринадные"),
            ("Ребра свиные Слово Мясника, 500 г", "Ребра крупнокусковые"),
            ("Фарш Классический Слово Мясника из свинины и говядины, 400 г", "Фарш"),
            ("Шницель свиной Слово Мясника, 400 г", "Порционные"),
            ("Бефстроганов из свинины СЛОВО МЯСНИКА категория Б, 700г", "Прочее"),
            ("Стейк Порк свиной Слово Мясника, 280 г", "Маринады"),
            ("К Люля-кебаб Три мяса в оболочке, 370 г, Слово Мясника, охлажденные", "Колбаски"),
            ("Сосиски сливочные Слово Мясника, 420 г", "Колбаски"),
            ("Колбаса варёная Докторская Слово Мясника, 500 г", "Колбаски"),
            ("Сервелат Слово Мясника варёно-копчёный, 350 г", "Колбаски"),
        ]
        frame = pd.DataFrame(
            [{"Категория": "Мясо", "Название": name} for name, _ in cases]
        )

        result, _ = apply_classifiers(frame, default_rules_path())

        self.assertEqual(result["Подкатегория"].tolist(), [expected for _, expected in cases])

    def test_default_meat_sausage_rules_fill_type(self) -> None:
        cases = [
            ("Сосиски сливочные Слово Мясника, 420 г", "Сосиски"),
            ("Сардельки говяжьи Слово Мясника, 400 г", "Сосиски"),
            ("Колбаса варёная Докторская Слово Мясника, 500 г", "Варёная колбаса"),
            ("Вареная колбаса Молочная Слово Мясника, 500 г", "Варёная колбаса"),
            ("Сервелат Слово Мясника варёно-копчёный, 350 г", "Сервелат (варёно-копчёная)"),
            ("Колбаса варено-копченая Слово Мясника, 350 г", "Сервелат (варёно-копчёная)"),
            ("Колбаски свиные Том Ям 290 г, Слово Мясника", "Другое"),
            ("Чевапчичи Слово Мясника из свинины и говядины, 300 г", "Другое"),
        ]
        frame = pd.DataFrame(
            [{"Категория": "Мясо", "Название": name} for name, _ in cases]
        )

        result, _ = apply_classifiers(frame, default_rules_path())

        self.assertEqual(result["Подкатегория"].tolist(), ["Колбаски"] * len(cases))
        self.assertEqual(result["Тип"].tolist(), [expected for _, expected in cases])


if __name__ == "__main__":
    unittest.main()
