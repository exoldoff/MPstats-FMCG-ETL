from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, pct, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_sales_checks(ctx: QualityContext) -> list[QualityIssue]:
    if not ctx.has("date", "category", "sku", "sales"):
        ctx.skip("Аномалии продаж SKU", "Нужны колонки даты, категории, SKU и продаж.")
        return []
    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        WITH hist AS (
            SELECT
                *,
                COUNT(*) OVER w AS hist_periods,
                LAG(sales) OVER (PARTITION BY category, sku ORDER BY period_date) AS previous_sales,
                MEDIAN(sales) OVER w AS baseline_sales,
                QUANTILE_CONT(sales, 0.25) OVER w AS q1_sales,
                QUANTILE_CONT(sales, 0.75) OVER w AS q3_sales
            FROM dq_sku_period
            WINDOW w AS (
                PARTITION BY category, sku
                ORDER BY period_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        )
        SELECT
            period, category, sku, brand, product_name, sales, previous_sales, baseline_sales,
            sales - baseline_sales AS abs_delta,
            sales / NULLIF(baseline_sales, 0) AS ratio,
            sales_share,
            q1_sales,
            q3_sales,
            CASE
                WHEN sales - baseline_sales >= ? AND sales / NULLIF(baseline_sales, 0) >= ? THEN 'CRITICAL'
                ELSE 'WARNING'
            END AS severity
        FROM hist
        WHERE hist_periods >= ?
            AND baseline_sales >= ?
            AND sales - baseline_sales >= ?
            AND (
                sales / NULLIF(baseline_sales, 0) >= ?
                OR (q3_sales > q1_sales AND sales > q3_sales + (? * (q3_sales - q1_sales)))
            )
            AND (COALESCE(sales_share, 0) >= ? OR sales - baseline_sales >= ?)
        ORDER BY abs_delta DESC
        LIMIT {limit(ctx)}
        """,
        [
            cfg.critical_abs_sales_delta,
            cfg.critical_growth_threshold,
            cfg.min_history_periods,
            max(1.0, cfg.min_abs_sales_delta / 5),
            cfg.min_abs_sales_delta,
            cfg.relative_growth_threshold,
            cfg.percentile_iqr_multiplier,
            cfg.min_category_share_for_alert,
            cfg.critical_abs_sales_delta,
        ],
    )
    out = [
        issue(
            check_id="sku_sales_spike",
            check_name="Резкий рост продаж SKU",
            severity=str(row["severity"]),  # type: ignore[arg-type]
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Продажи, шт",
            current_value=row["sales"],
            previous_value=row["previous_sales"],
            baseline_value=row["baseline_sales"],
            absolute_delta=row["abs_delta"],
            relative_delta=row["ratio"],
            message=(
                f"SKU {row['sku']} в категории {row['category']} показал продажи "
                f"{fmt(row['sales'], ' шт.')} против медианы {fmt(row['baseline_sales'], ' шт.')}."
            ),
            details={
                "brand": row["brand"],
                "product_name": row["product_name"],
                "category_share": row["sales_share"],
                "q1_sales": row["q1_sales"],
                "q3_sales": row["q3_sales"],
            },
            suggested_action="Проверь источник MPStats, дубли SKU и корректность периода.",
        )
        for row in rows
    ]

    rows = query_rows(
        ctx,
        f"""
        WITH hist AS (
            SELECT
                *,
                COUNT(*) OVER w AS hist_periods,
                LAG(sales) OVER (PARTITION BY category, sku ORDER BY period_date) AS previous_sales,
                MEDIAN(sales) OVER w AS baseline_sales
            FROM dq_sku_period
            WINDOW w AS (
                PARTITION BY category, sku
                ORDER BY period_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        )
        SELECT
            period, category, sku, brand, product_name, sales, previous_sales, baseline_sales,
            baseline_sales - sales AS abs_delta,
            sales / NULLIF(baseline_sales, 0) AS ratio,
            sales_share,
            CASE
                WHEN sales <= baseline_sales * ? THEN 'CRITICAL'
                ELSE 'WARNING'
            END AS severity
        FROM hist
        WHERE hist_periods >= ?
            AND baseline_sales >= ?
            AND baseline_sales - sales >= ?
            AND sales <= baseline_sales * ?
        ORDER BY abs_delta DESC
        LIMIT {limit(ctx)}
        """,
        [
            cfg.critical_drop_threshold,
            cfg.min_history_periods,
            cfg.min_abs_sales_delta,
            cfg.min_abs_sales_delta,
            cfg.relative_drop_threshold,
        ],
    )
    out.extend(
        issue(
            check_id="sku_sales_drop",
            check_name="Резкое падение продаж SKU",
            severity=str(row["severity"]),  # type: ignore[arg-type]
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Продажи, шт",
            current_value=row["sales"],
            previous_value=row["previous_sales"],
            baseline_value=row["baseline_sales"],
            absolute_delta=-float(row["abs_delta"] or 0),
            relative_delta=row["ratio"],
            message=(
                f"SKU {row['sku']} в категории {row['category']} упал до "
                f"{fmt(row['sales'], ' шт.')} против обычных {fmt(row['baseline_sales'], ' шт.')}."
            ),
            details={"brand": row["brand"], "product_name": row["product_name"], "category_share": row["sales_share"]},
            suggested_action="Проверь, не пропала ли часть периода, категория или SKU в выгрузке.",
        )
        for row in rows
    )

    rows = query_rows(
        ctx,
        f"""
        WITH sku_hist AS (
            SELECT
                sku.*,
                COUNT(*) OVER (
                    PARTITION BY sku.category, sku.sku
                    ORDER BY sku.period_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS sku_hist_periods,
                (
                    SELECT COUNT(*)
                    FROM dq_category_period AS category_history
                    WHERE category_history.category = sku.category
                        AND category_history.period_date < sku.period_date
                ) AS category_hist_periods
            FROM dq_sku_period AS sku
        )
        SELECT
            period, category, sku, brand, product_name, sales, revenue, sales_share,
            CASE
                WHEN sales >= ? OR revenue >= ? THEN 'CRITICAL'
                ELSE 'WARNING'
            END AS severity
        FROM sku_hist
        WHERE sku_hist_periods = 0
            AND category_hist_periods >= ?
            AND (sales >= ? OR revenue >= ?)
        ORDER BY GREATEST(sales, revenue / 1000) DESC
        LIMIT {limit(ctx)}
        """,
        [
            cfg.new_sku_critical_sales_threshold,
            cfg.new_sku_critical_revenue_threshold,
            cfg.min_history_periods,
            cfg.new_sku_high_sales_threshold,
            cfg.new_sku_high_revenue_threshold,
        ],
    )
    out.extend(
        issue(
            check_id="new_sku_high_sales",
            check_name="Новый SKU с высокими продажами",
            severity=str(row["severity"]),  # type: ignore[arg-type]
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Продажи, шт",
            current_value=row["sales"],
            baseline_value=0,
            absolute_delta=row["sales"],
            relative_delta=None,
            message=(
                f"Новый SKU {row['sku']} в категории {row['category']} сразу дал "
                f"{fmt(row['sales'], ' шт.')} и долю {pct(row['sales_share'])}."
            ),
            details={"brand": row["brand"], "product_name": row["product_name"], "revenue": row["revenue"]},
            suggested_action="Проверь, это реальная новинка/хит или дубль/ошибка MPStats.",
        )
        for row in rows
    )
    return out
