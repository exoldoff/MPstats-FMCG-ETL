from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_consistency_checks(ctx: QualityContext) -> list[QualityIssue]:
    out: list[QualityIssue] = []
    if ctx.has("sales"):
        out.extend(_negative_metric(ctx, "sales", "Продажи, шт", "negative_sales", "Отрицательные продажи"))
    if ctx.has("revenue"):
        out.extend(_negative_metric(ctx, "revenue", "Выручка, руб", "negative_revenue", "Отрицательный ТО"))
    if ctx.has("stock"):
        out.extend(_negative_metric(ctx, "stock", "Остаток", "negative_stock", "Отрицательный остаток"))
    if ctx.has("acb"):
        out.extend(_negative_metric(ctx, "acb", "АКБ", "negative_acb", "Отрицательный АКБ"))

    if not ctx.has("sales", "price", "revenue"):
        ctx.skip("Согласованность ТО = продажи × цена", "Нужны колонки продаж, средней цены и выручки.")
        return out

    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        SELECT
            row_id,
            period,
            category,
            sku,
            product_name,
            source_file,
            sales,
            price,
            revenue,
            sales * price AS expected_revenue,
            ABS(revenue - sales * price) / GREATEST(ABS(revenue), ABS(sales * price), 1) AS mismatch_share
        FROM dq_base
        WHERE sales > 0
            AND price > 0
            AND revenue > 0
            AND ABS(revenue - sales * price) / GREATEST(ABS(revenue), ABS(sales * price), 1) > ?
        ORDER BY mismatch_share DESC, ABS(revenue - sales * price) DESC
        LIMIT {limit(ctx)}
        """,
        [cfg.tolerance_revenue_price_sales],
    )
    out.extend(
        issue(
            check_id="revenue_price_sales_mismatch",
            check_name="ТО не согласуется с продажи × цена",
            severity="WARNING",
            entity_type="row",
            entity_id=row["row_id"],
            category=row["category"],
            period=row["period"],
            metric_name="Выручка, руб",
            current_value=row["revenue"],
            baseline_value=row["expected_revenue"],
            absolute_delta=float(row["revenue"] or 0) - float(row["expected_revenue"] or 0),
            relative_delta=row["mismatch_share"],
            message=(
                f"Для SKU {row['sku']} ТО {fmt(row['revenue'], ' руб.')} заметно отличается "
                f"от продажи × цена {fmt(row['expected_revenue'], ' руб.')}."
            ),
            details={"sku": row["sku"], "product_name": row["product_name"], "sales": row["sales"], "price": row["price"], "source_file": row["source_file"]},
            suggested_action="Проверь округления MPStats, валюту/НДС и исходные метрики строки.",
        )
        for row in rows
    )

    rows = query_rows(
        ctx,
        f"""
        SELECT row_id, period, category, sku, product_name, source_file, sales, price, revenue
        FROM dq_base
        WHERE sales > 0 AND COALESCE(price, 0) = 0
        ORDER BY sales DESC
        LIMIT {limit(ctx)}
        """
    )
    out.extend(
        issue(
            check_id="sales_with_zero_price",
            check_name="Продажи есть, цена нулевая",
            severity="CRITICAL",
            entity_type="row",
            entity_id=row["row_id"],
            category=row["category"],
            period=row["period"],
            metric_name="Средняя цена, руб",
            current_value=row["price"],
            message=f"SKU {row['sku']} имеет продажи {fmt(row['sales'], ' шт.')}, но цена равна 0 или пустая.",
            details={"sku": row["sku"], "product_name": row["product_name"], "revenue": row["revenue"], "source_file": row["source_file"]},
            suggested_action="Проверь исходную цену; такая строка опасна для расчётов ТО и price-per-unit.",
        )
        for row in rows
    )

    rows = query_rows(
        ctx,
        f"""
        SELECT row_id, period, category, sku, product_name, source_file, sales, price, revenue
        FROM dq_base
        WHERE sales > 0 AND price > 0 AND COALESCE(revenue, 0) = 0
        ORDER BY sales * price DESC
        LIMIT {limit(ctx)}
        """
    )
    out.extend(
        issue(
            check_id="sales_price_with_zero_revenue",
            check_name="Цена и продажи есть, ТО нулевой",
            severity="WARNING",
            entity_type="row",
            entity_id=row["row_id"],
            category=row["category"],
            period=row["period"],
            metric_name="Выручка, руб",
            current_value=row["revenue"],
            baseline_value=float(row["sales"] or 0) * float(row["price"] or 0),
            message=f"SKU {row['sku']} имеет продажи и цену, но ТО равен 0.",
            details={"sku": row["sku"], "product_name": row["product_name"], "sales": row["sales"], "price": row["price"], "source_file": row["source_file"]},
            suggested_action="Проверь, не потерялась ли колонка выручки при обработке.",
        )
        for row in rows
    )
    return out


def _negative_metric(ctx: QualityContext, field: str, metric_name: str, check_id: str, check_name: str) -> list[QualityIssue]:
    rows = query_rows(
        ctx,
        f"""
        SELECT row_id, period, category, sku, product_name, source_file, {field} AS metric_value
        FROM dq_base
        WHERE {field} < 0
        ORDER BY ABS({field}) DESC
        LIMIT {limit(ctx)}
        """
    )
    return [
        issue(
            check_id=check_id,
            check_name=check_name,
            severity="CRITICAL",
            entity_type="row",
            entity_id=row["row_id"],
            category=row["category"],
            period=row["period"],
            metric_name=metric_name,
            current_value=row["metric_value"],
            message=f"SKU {row['sku']} имеет отрицательное значение `{metric_name}`: {fmt(row['metric_value'])}.",
            details={"sku": row["sku"], "product_name": row["product_name"], "source_file": row["source_file"]},
            suggested_action="Проверь исходный CSV и правила нормализации числовых колонок.",
        )
        for row in rows
    ]
