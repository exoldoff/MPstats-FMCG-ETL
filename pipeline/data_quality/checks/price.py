from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_price_checks(ctx: QualityContext) -> list[QualityIssue]:
    if not ctx.has("price"):
        ctx.skip("Аномалии цен", "Колонка средней цены не найдена.")
        return []
    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        SELECT period, category, sku, product_name, sales, revenue, price, source_file, row_id
        FROM dq_base
        WHERE price <= 0 AND (COALESCE(sales, 0) > 0 OR COALESCE(revenue, 0) > 0)
        ORDER BY ABS(price) DESC, COALESCE(revenue, 0) DESC
        LIMIT {limit(ctx)}
        """
    )
    out = [
        issue(
            check_id="zero_or_negative_price",
            check_name="Нулевая или отрицательная цена при продажах",
            severity="CRITICAL",
            entity_type="row",
            entity_id=row["row_id"],
            category=row["category"],
            period=row["period"],
            metric_name="Средняя цена, руб",
            current_value=row["price"],
            message=f"Строка SKU {row['sku']} имеет цену {fmt(row['price'], ' руб.')} при продажах/ТО.",
            details={"sku": row["sku"], "product_name": row["product_name"], "sales": row["sales"], "revenue": row["revenue"], "source_file": row["source_file"]},
            suggested_action="Проверь цену в исходной выгрузке MPStats.",
        )
        for row in rows
    ]

    if ctx.has("date", "category", "sku"):
        rows = query_rows(
            ctx,
            f"""
            WITH hist AS (
                SELECT
                    *,
                    COUNT(*) OVER w AS hist_periods,
                    LAG(price) OVER (PARTITION BY category, sku ORDER BY period_date) AS previous_price,
                    MEDIAN(price) OVER w AS baseline_price
                FROM dq_sku_period
                WHERE price > 0
                WINDOW w AS (
                    PARTITION BY category, sku
                    ORDER BY period_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
            )
            SELECT
                period, category, sku, brand, product_name, sales, price, previous_price, baseline_price,
                GREATEST(price / NULLIF(baseline_price, 0), baseline_price / NULLIF(price, 0)) AS ratio,
                price - baseline_price AS abs_delta,
                CASE
                    WHEN GREATEST(price / NULLIF(baseline_price, 0), baseline_price / NULLIF(price, 0)) >= ? THEN 'CRITICAL'
                    ELSE 'WARNING'
                END AS severity
            FROM hist
            WHERE hist_periods >= ?
                AND sales >= ?
                AND baseline_price > 0
                AND GREATEST(price / NULLIF(baseline_price, 0), baseline_price / NULLIF(price, 0)) >= ?
            ORDER BY ratio DESC
            LIMIT {limit(ctx)}
            """,
            [
                cfg.critical_price_change_ratio,
                cfg.min_history_periods,
                cfg.min_sales_for_price_checks,
                cfg.max_price_change_ratio,
            ],
        )
        out.extend(
            issue(
                check_id="sku_price_change",
                check_name="Резкое изменение цены SKU",
                severity=str(row["severity"]),  # type: ignore[arg-type]
                entity_type="sku",
                entity_id=row["sku"],
                category=row["category"],
                period=row["period"],
                metric_name="Средняя цена, руб",
                current_value=row["price"],
                previous_value=row["previous_price"],
                baseline_value=row["baseline_price"],
                absolute_delta=row["abs_delta"],
                relative_delta=row["ratio"],
                message=(
                    f"Цена SKU {row['sku']} стала {fmt(row['price'], ' руб.')} "
                    f"против медианы {fmt(row['baseline_price'], ' руб.')}."
                ),
                details={"brand": row["brand"], "product_name": row["product_name"], "sales": row["sales"]},
                suggested_action="Проверь единицы измерения, фасовку и цену в исходнике.",
            )
            for row in rows
        )

    if ctx.has("unit_price", "date", "category", "sku"):
        rows = query_rows(
            ctx,
            f"""
            WITH priced AS (
                SELECT
                    *,
                    MEDIAN(unit_price) OVER (PARTITION BY period, category) AS category_unit_median
                FROM dq_sku_period
                WHERE unit_price > 0
            )
            SELECT
                period, category, sku, brand, product_name, sales, unit_price, category_unit_median,
                GREATEST(unit_price / NULLIF(category_unit_median, 0), category_unit_median / NULLIF(unit_price, 0)) AS ratio
            FROM priced
            WHERE sales >= ?
                AND category_unit_median > 0
                AND GREATEST(unit_price / NULLIF(category_unit_median, 0), category_unit_median / NULLIF(unit_price, 0)) >= ?
            ORDER BY ratio DESC
            LIMIT {limit(ctx)}
            """,
            [cfg.min_sales_for_price_checks, cfg.unit_price_category_ratio],
        )
        out.extend(
            issue(
                check_id="unit_price_category_outlier",
                check_name="Цена за единицу выбивается из категории",
                severity="WARNING",
                entity_type="sku",
                entity_id=row["sku"],
                category=row["category"],
                period=row["period"],
                metric_name="Цена за кг/л",
                current_value=row["unit_price"],
                baseline_value=row["category_unit_median"],
                absolute_delta=float(row["unit_price"] or 0) - float(row["category_unit_median"] or 0),
                relative_delta=row["ratio"],
                message=(
                    f"Цена за единицу SKU {row['sku']} равна {fmt(row['unit_price'], ' руб.')} "
                    f"при медиане категории {fmt(row['category_unit_median'], ' руб.')}."
                ),
                details={"brand": row["brand"], "product_name": row["product_name"], "sales": row["sales"]},
                suggested_action="Проверь распарсенный вес/объём и цену товара.",
            )
            for row in rows
        )
    elif ctx.has("date", "category", "sku"):
        ctx.skip("Цена за единицу веса/объёма", "Колонка `Цена за кг` или аналог не найдена.")
    return out
