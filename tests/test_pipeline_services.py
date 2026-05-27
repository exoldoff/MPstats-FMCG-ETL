from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from classifiers.engine import apply_classifiers, default_rules_path
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
