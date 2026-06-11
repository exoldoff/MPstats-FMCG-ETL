from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, pct, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_market_share_checks(ctx: QualityContext) -> list[QualityIssue]:
    if not ctx.has("date", "category", "sku", "sales"):
        ctx.skip("Доли и структура рынка", "Нужны колонки даты, категории, SKU и продаж.")
        return []
    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        WITH hist AS (
            SELECT
                *,
                COUNT(*) OVER w AS hist_periods,
                LAG(sales_share) OVER (PARTITION BY category, sku ORDER BY period_date) AS previous_share,
                MEDIAN(sales_share) OVER w AS baseline_share
            FROM dq_sku_period
            WINDOW w AS (
                PARTITION BY category, sku
                ORDER BY period_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        )
        SELECT
            period, category, sku, brand, product_name, sales, sales_share, previous_share, baseline_share,
            sales_share - COALESCE(baseline_share, 0) AS share_delta
        FROM hist
        WHERE hist_periods >= ?
            AND sales_share >= ?
            AND sales_share - COALESCE(baseline_share, 0) >= ?
        ORDER BY share_delta DESC, sales DESC
        LIMIT {limit(ctx)}
        """,
        [cfg.min_history_periods, cfg.min_category_share_for_alert, cfg.share_delta_for_alert],
    )
    out = [
        issue(
            check_id="sku_category_share_spike",
            check_name="SKU резко занял долю категории",
            severity="WARNING",
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Доля продаж категории",
            current_value=row["sales_share"],
            previous_value=row["previous_share"],
            baseline_value=row["baseline_share"],
            absolute_delta=row["share_delta"],
            relative_delta=None,
            message=(
                f"SKU {row['sku']} занял {pct(row['sales_share'])} продаж категории "
                f"{row['category']} против обычных {pct(row['baseline_share'])}."
            ),
            details={"brand": row["brand"], "product_name": row["product_name"], "sales": row["sales"]},
            suggested_action="Проверь, не задвоился ли SKU и не изменилась ли категория.",
        )
        for row in rows
    ]

    rows = query_rows(
        ctx,
        f"""
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY period, category ORDER BY sales DESC) AS rank_in_category,
                LEAD(sales) OVER (PARTITION BY period, category ORDER BY sales DESC) AS next_sales
            FROM dq_sku_period
            WHERE sales > 0
        )
        SELECT period, category, sku, brand, product_name, sales, next_sales, sales_share,
            sales / NULLIF(next_sales, 0) AS dominance_ratio
        FROM ranked
        WHERE rank_in_category = 1
            AND next_sales > 0
            AND sales_share >= ?
            AND sales / NULLIF(next_sales, 0) >= ?
        ORDER BY dominance_ratio DESC
        LIMIT {limit(ctx)}
        """,
        [cfg.min_category_share_for_alert, cfg.top_sku_dominance_ratio],
    )
    out.extend(
        issue(
            check_id="top_sku_dominance",
            check_name="Топ-1 SKU непропорционально больше остальных",
            severity="WARNING",
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Продажи, шт",
            current_value=row["sales"],
            previous_value=row["next_sales"],
            baseline_value=None,
            absolute_delta=float(row["sales"] or 0) - float(row["next_sales"] or 0),
            relative_delta=row["dominance_ratio"],
            message=(
                f"Топ SKU {row['sku']} продаёт {fmt(row['sales'], ' шт.')}, "
                f"что в {fmt(row['dominance_ratio'])} раз больше следующего SKU."
            ),
            details={"brand": row["brand"], "product_name": row["product_name"], "category_share": row["sales_share"]},
            suggested_action="Проверь дубли, промо-скачок или ошибочную агрегацию товара.",
        )
        for row in rows
    )
    return out
