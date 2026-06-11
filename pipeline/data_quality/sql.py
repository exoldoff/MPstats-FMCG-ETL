from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import duckdb

from pipeline.data_quality.models import PreparedQualityData
from pipeline.repositories.sql_repository import quote_identifier, sql_literal


CANONICAL_COLUMNS: dict[str, tuple[str, ...]] = {
    "date": ("Дата", "date", "Дата отчета", "Период", "period"),
    "year": ("__year", "Год", "year"),
    "month": ("__month", "Месяц", "month"),
    "project": ("__project_name", "project_name"),
    "marketplace": ("Маркетплейс", "Marketplace", "МП", "marketplace"),
    "marketplace_code": ("__marketplace_code", "marketplace_code"),
    "network": ("Сеть", "network", "Network", "retailer", "Ритейлер"),
    "category": ("Категория", "Category", "category", "category_name"),
    "category_key": ("__category_key", "category_key"),
    "subcategory": ("Подкатегория", "Subcategory", "subcategory"),
    "sku": ("SKU", "Артикул", "ID товара", "id товара", "nmId", "nm_id", "product_id", "offer_id"),
    "brand": ("Бренд", "Brand", "brand"),
    "product_name": ("Название", "Название товара", "Наименование", "Name", "name", "product_name"),
    "sales": ("Продажи, шт", "Продажи", "Sales", "sales"),
    "revenue": ("Выручка, руб", "Выручка", "Revenue", "revenue", "turnover"),
    "price": ("Средняя цена, руб", "Средняя цена", "Цена", "Price", "price", "average_price"),
    "unit_price": ("Цена за кг", "Цена за л", "price_per_unit", "unit_price"),
    "weight": ("Вес, кг (ед.)", "Вес, кг", "weight_kg", "parsed_weight_kg"),
    "stock": ("Остаток", "Остатки", "stock", "stocks"),
    "acb": ("АКБ", "acb", "ACB"),
    "source_file": ("filename", "__quality_source_file"),
}


@dataclass(frozen=True)
class SqlCheckRow:
    values: dict[str, object]


def prepare_quality_tables(con: duckdb.DuckDBPyConnection, paths: tuple[Path, ...]) -> PreparedQualityData:
    if not paths:
        raise FileNotFoundError("Источник для проверки качества не найден.")

    con.execute("SET preserve_insertion_order = false")
    con.execute(
        f"""
        CREATE TEMP VIEW dq_raw AS
        SELECT *
        FROM read_csv_auto(
            {_path_list_sql(paths)},
            union_by_name = true,
            all_varchar = true,
            filename = true,
            ignore_errors = true
        )
        """
    )
    raw_columns = [str(row[0]) for row in con.execute("DESCRIBE dq_raw").fetchall()]
    columns = _resolve_columns(raw_columns)
    _create_prepared_tables(con, columns)
    total_rows = int(con.execute("SELECT COUNT(*) FROM dq_base").fetchone()[0])
    latest = con.execute("SELECT MAX(period) FROM dq_base WHERE period IS NOT NULL").fetchone()
    latest_period = str(latest[0]) if latest and latest[0] is not None else None
    return PreparedQualityData(
        total_rows=total_rows,
        columns=columns,
        raw_columns=raw_columns,
        latest_period=latest_period,
        source_paths=paths,
    )


def prepare_quality_tables_from_cube(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    project_name: str,
    db_path: Path,
) -> PreparedQualityData:
    quote_identifier(table_name)
    con.execute("SET preserve_insertion_order = false")
    con.execute(
        f"""
        CREATE TEMP VIEW dq_raw AS
        SELECT *
        FROM {quote_identifier(table_name)}
        WHERE {_quote('__project_name')} = {sql_literal(project_name)}
        """
    )
    raw_columns = [str(row[0]) for row in con.execute("DESCRIBE dq_raw").fetchall()]
    columns = _resolve_columns(raw_columns)
    _create_prepared_tables(con, columns)
    total_rows = int(con.execute("SELECT COUNT(*) FROM dq_base").fetchone()[0])
    latest = con.execute("SELECT MAX(period) FROM dq_base WHERE period IS NOT NULL").fetchone()
    latest_period = str(latest[0]) if latest and latest[0] is not None else None
    return PreparedQualityData(
        total_rows=total_rows,
        columns=columns,
        raw_columns=raw_columns,
        latest_period=latest_period,
        source_paths=(db_path,),
    )


def _create_prepared_tables(con: duckdb.DuckDBPyConnection, columns: dict[str, str | None]) -> None:
    date_value = _date_expr(columns.get("date"), columns.get("year"), columns.get("month"))
    period_expr = f"STRFTIME(DATE_TRUNC('month', {date_value}), '%Y-%m')"
    category_expr = _coalesce_text_expr(columns.get("category"), columns.get("category_key"))
    network_expr = _coalesce_text_expr(columns.get("network"), columns.get("marketplace"), columns.get("marketplace_code"))
    source_expr = _text_expr(columns.get("source_file"))

    con.execute(
        f"""
        CREATE TEMP TABLE dq_base AS
        SELECT
            ROW_NUMBER() OVER () AS row_id,
            {source_expr} AS source_file,
            {date_value} AS date_value,
            {period_expr} AS period,
            DATE_TRUNC('month', {date_value})::DATE AS period_date,
            {category_expr} AS category,
            {_sku_expr(columns.get("sku"))} AS sku,
            {_text_expr(columns.get("brand"))} AS brand,
            {network_expr} AS network,
            {_text_expr(columns.get("subcategory"))} AS subcategory,
            {_text_expr(columns.get("product_name"))} AS product_name,
            {_numeric_expr(columns.get("sales"))} AS sales,
            {_numeric_expr(columns.get("revenue"))} AS revenue,
            {_numeric_expr(columns.get("price"))} AS price,
            {_numeric_expr(columns.get("unit_price"))} AS unit_price,
            {_numeric_expr(columns.get("weight"))} AS weight,
            {_numeric_expr(columns.get("stock"))} AS stock,
            {_numeric_expr(columns.get("acb"))} AS acb
        FROM dq_raw
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE dq_category_period AS
        SELECT
            period,
            period_date,
            category,
            COUNT(*) AS row_count,
            COUNT(DISTINCT sku) FILTER (WHERE sku IS NOT NULL AND sku <> '') AS sku_count,
            SUM(COALESCE(sales, 0)) AS sales,
            SUM(COALESCE(revenue, 0)) AS revenue,
            AVG(price) FILTER (WHERE price IS NOT NULL) AS avg_price,
            AVG(unit_price) FILTER (WHERE unit_price IS NOT NULL) AS avg_unit_price
        FROM dq_base
        WHERE period IS NOT NULL AND category IS NOT NULL AND category <> ''
        GROUP BY period, period_date, category
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE dq_sku_period AS
        SELECT
            base.period,
            base.period_date,
            base.category,
            base.sku,
            MIN(base.brand) FILTER (WHERE base.brand IS NOT NULL AND base.brand <> '') AS brand,
            MIN(base.network) FILTER (WHERE base.network IS NOT NULL AND base.network <> '') AS network,
            MIN(base.product_name) FILTER (WHERE base.product_name IS NOT NULL AND base.product_name <> '') AS product_name,
            COUNT(*) AS row_count,
            SUM(COALESCE(base.sales, 0)) AS sales,
            SUM(COALESCE(base.revenue, 0)) AS revenue,
            AVG(base.price) FILTER (WHERE base.price IS NOT NULL) AS price,
            AVG(base.unit_price) FILTER (WHERE base.unit_price IS NOT NULL) AS unit_price,
            SUM(base.stock) FILTER (WHERE base.stock IS NOT NULL) AS stock,
            SUM(base.acb) FILTER (WHERE base.acb IS NOT NULL) AS acb,
            category.sales AS category_sales,
            category.revenue AS category_revenue,
            CASE WHEN category.sales > 0 THEN SUM(COALESCE(base.sales, 0)) / category.sales ELSE NULL END AS sales_share,
            CASE WHEN category.revenue > 0 THEN SUM(COALESCE(base.revenue, 0)) / category.revenue ELSE NULL END AS revenue_share
        FROM dq_base AS base
        LEFT JOIN dq_category_period AS category
            ON category.period = base.period AND category.category = base.category
        WHERE base.period IS NOT NULL
            AND base.category IS NOT NULL AND base.category <> ''
            AND base.sku IS NOT NULL AND base.sku <> ''
        GROUP BY
            base.period,
            base.period_date,
            base.category,
            base.sku,
            category.sales,
            category.revenue
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE dq_brand_period AS
        SELECT
            period,
            period_date,
            category,
            brand,
            SUM(sales) AS sales,
            SUM(revenue) AS revenue,
            SUM(row_count) AS row_count,
            MAX(category_sales) AS category_sales,
            MAX(category_revenue) AS category_revenue,
            CASE WHEN MAX(category_sales) > 0 THEN SUM(sales) / MAX(category_sales) ELSE NULL END AS sales_share,
            CASE WHEN MAX(category_revenue) > 0 THEN SUM(revenue) / MAX(category_revenue) ELSE NULL END AS revenue_share
        FROM dq_sku_period
        WHERE brand IS NOT NULL AND brand <> ''
        GROUP BY period, period_date, category, brand
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE dq_network_category_period AS
        SELECT
            period,
            period_date,
            category,
            network,
            SUM(sales) AS sales,
            SUM(revenue) AS revenue,
            SUM(row_count) AS row_count
        FROM dq_sku_period
        WHERE network IS NOT NULL AND network <> ''
        GROUP BY period, period_date, category, network
        """
    )


def fetch_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[object] | None = None) -> list[dict[str, object]]:
    cursor = con.execute(sql, params or [])
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _path_list_sql(paths: tuple[Path, ...]) -> str:
    values = ", ".join(sql_literal(str(path)) for path in paths)
    return f"[{values}]"


def _resolve_columns(raw_columns: list[str]) -> dict[str, str | None]:
    lookup = {_normalized(column): column for column in raw_columns}
    result: dict[str, str | None] = {}
    for key, candidates in CANONICAL_COLUMNS.items():
        result[key] = next((lookup[_normalized(candidate)] for candidate in candidates if _normalized(candidate) in lookup), None)
    return result


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _text_expr(column: str | None) -> str:
    if not column:
        return "NULL::VARCHAR"
    return f"NULLIF(TRIM(CAST({_quote(column)} AS VARCHAR)), '')"


def _coalesce_text_expr(*columns: str | None) -> str:
    expressions = [_text_expr(column) for column in columns if column]
    if not expressions:
        return "NULL::VARCHAR"
    return "COALESCE(" + ", ".join(expressions) + ")"


def _sku_expr(column: str | None) -> str:
    text = _text_expr(column)
    if not column:
        return text
    return f"REGEXP_REPLACE({text}, '^(\\d+)\\.0+$', '\\1')"


def _numeric_expr(column: str | None) -> str:
    if not column:
        return "NULL::DOUBLE"
    text = _text_expr(column)
    clean = f"REPLACE(REPLACE(REPLACE({text}, '\u00a0', ''), ' ', ''), ',', '.')"
    return f"TRY_CAST({clean} AS DOUBLE)"


def _date_expr(column: str | None, year_column: str | None = None, month_column: str | None = None) -> str:
    if not column:
        if year_column and month_column:
            year_expr = _numeric_expr(year_column)
            month_expr = _numeric_expr(month_column)
            return f"MAKE_DATE(TRY_CAST({year_expr} AS INTEGER), TRY_CAST({month_expr} AS INTEGER), 1)"
        return "NULL::DATE"
    text = _text_expr(column)
    return (
        "COALESCE("
        f"CAST(try_strptime({text}, '%d.%m.%Y') AS DATE), "
        f"CAST(try_strptime({text}, '%Y-%m-%d') AS DATE), "
        f"CAST(try_strptime({text}, '%Y-%m') AS DATE), "
        f"TRY_CAST({text} AS DATE)"
        ")"
    )


def _normalized(value: object) -> str:
    text = str(value).strip().lower().replace("ё", "е")
    return re.sub(r"[^a-zа-я0-9]+", "", text)
