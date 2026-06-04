from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from pipeline.services.weight_parser_service import (
    extract_total_weight_kg_from_name,
    extract_weight_kg_from_name,
    parse_weights_dataframe,
    sanitize_weight_kg,
)


class WeightParserServiceTest(unittest.TestCase):
    def test_extracts_single_and_pack_weights(self) -> None:
        self.assertEqual(extract_weight_kg_from_name("Лимонная кислота пищевая 1кг"), 1.0)
        self.assertAlmostEqual(extract_weight_kg_from_name("Мыло 3 шт. - 175 г"), 0.175)
        self.assertAlmostEqual(extract_total_weight_kg_from_name("Мыло 3 шт. - 175 г"), 0.525)
        self.assertEqual(extract_weight_kg_from_name("Пюре 4 x 90 г"), 0.09)
        self.assertEqual(extract_total_weight_kg_from_name("Пюре 4 x 90 г"), 0.36)
        self.assertEqual(extract_weight_kg_from_name("Напиток 2 по 1,5 л"), 1.5)
        self.assertEqual(extract_total_weight_kg_from_name("Напиток 2 по 1,5 л"), 3.0)
        self.assertEqual(extract_weight_kg_from_name("Товар 10 г * 100 шт"), 0.01)
        self.assertEqual(extract_total_weight_kg_from_name("Товар 10 г * 100 шт"), 1.0)

    def test_sanitize_weight_fixes_big_liters(self) -> None:
        fixed, is_anomaly, reason = sanitize_weight_kg("Средство 174л", 174.0, 40.0)
        self.assertEqual(fixed, 1.74)
        self.assertTrue(is_anomaly)
        self.assertIn("fixed_liters", reason)

    def test_parse_weights_dataframe_adds_expected_columns(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Дата": "01.06.2025",
                    "Маркетплейс": "Ozon",
                    "Категория": "Мыло",
                    "SKU": "123",
                    "Бренд": "Brand",
                    "Название": "Мыло 3 шт. - 175 г",
                    "Продажи": "10",
                    "Продавец": "Seller",
                    "Средняя цена": "100",
                    "Выручка": "1000",
                }
            ]
        )
        parsed = parse_weights_dataframe(df)
        self.assertAlmostEqual(parsed.iloc[0]["Вес, кг"], 0.175)
        self.assertAlmostEqual(parsed.iloc[0]["Вес, кг (сумм.)"], 0.525)
        self.assertAlmostEqual(parsed.iloc[0]["Объем, кг"], 5.25)
        self.assertFalse(bool(parsed.iloc[0]["Вес аномалия"]))
        self.assertAlmostEqual(parsed.iloc[0]["Цена за кг"], 100 / 0.525)


if __name__ == "__main__":
    unittest.main()
