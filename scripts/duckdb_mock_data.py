from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path
import random
import time


DEFAULT_SIZES = {
    "small": 10_000,
    "medium": 500_000,
    "large": 2_000_000,
}

CATEGORIES = [
    "sugar",
    "soap",
    "lemon_acid",
    "tea",
    "coffee",
    "pasta",
    "cereal",
    "sauce",
]
NETWORKS = ["Ozon", "Wildberries", "Yandex Market"]
BRANDS = [f"Brand {index:02d}" for index in range(1, 81)]
REGIONS = ["Moscow", "Saint Petersburg", "Volga", "Ural", "Siberia", "South", "Far East"]

FIELDNAMES = [
    "period",
    "date",
    "category",
    "network",
    "brand",
    "sku",
    "price",
    "volume",
    "stores_count",
    "region",
]


def generate_mock_csv(path: str | Path, *, rows: int, seed: int = 42) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    start = time.perf_counter()
    with target.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES, delimiter=";")
        writer.writeheader()
        for index in range(rows):
            month_index = index % 24
            year = 2024 + month_index // 12
            month = month_index % 12 + 1
            category = CATEGORIES[index % len(CATEGORIES)]
            network = NETWORKS[index % len(NETWORKS)]
            brand = BRANDS[(index * 17) % len(BRANDS)]
            region = REGIONS[(index * 11) % len(REGIONS)]
            sku_bucket = index % 250_000
            price = round(50 + rng.random() * 950, 2)
            volume = round(rng.random() * 120, 3)
            if index % 997 == 0:
                volume = 0.0
            stores_count = 1 + index % 250
            writer.writerow(
                {
                    "period": f"{year}-{month:02d}",
                    "date": date(year, month, 1).isoformat(),
                    "category": category,
                    "network": network,
                    "brand": brand,
                    "sku": f"{category}-{network[:2].lower()}-{sku_bucket:06d}",
                    "price": price,
                    "volume": volume,
                    "stores_count": stores_count,
                    "region": region,
                }
            )
    elapsed = time.perf_counter() - start
    print(f"generated {rows:,} rows: {target} ({target.stat().st_size:,} bytes, {elapsed:.2f}s)")
    return target


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate reproducible DuckDB benchmark mock CSV data.")
    parser.add_argument("--output", type=Path, required=True, help="CSV path to write.")
    parser.add_argument("--rows", type=int, default=None, help="Number of rows. Overrides --size.")
    parser.add_argument("--size", choices=sorted(DEFAULT_SIZES), default="small", help="Named dataset size.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = int(args.rows if args.rows is not None else DEFAULT_SIZES[args.size])
    if rows <= 0:
        raise ValueError("--rows must be positive")
    generate_mock_csv(args.output, rows=rows, seed=args.seed)


if __name__ == "__main__":
    main()
