from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_sku_lifecycle_checks(ctx: QualityContext) -> list[QualityIssue]:
    if not ctx.has("date", "category", "sku", "sales"):
        ctx.skip("Новые и исчезнувшие SKU", "Нужны колонки даты, категории, SKU и продаж.")
        return []
    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        WITH latest_category AS (
            SELECT category, MAX(period) AS latest_period, MAX(period_date) AS latest_period_date
            FROM dq_category_period
            GROUP BY category
        ),
        history AS (
            SELECT
                sku.category,
                sku.sku,
                MIN(sku.brand) FILTER (WHERE sku.brand IS NOT NULL AND sku.brand <> '') AS brand,
                MIN(sku.product_name) FILTER (WHERE sku.product_name IS NOT NULL AND sku.product_name <> '') AS product_name,
                COUNT(*) AS hist_periods,
                MAX(sku.period) AS last_seen_period,
                MAX(sku.period_date) AS last_seen_period_date,
                MEDIAN(sku.sales) AS baseline_sales,
                MAX(sku.sales) AS max_sales
            FROM dq_sku_period AS sku
            JOIN latest_category AS latest ON latest.category = sku.category
            WHERE sku.period_date < latest.latest_period_date
            GROUP BY sku.category, sku.sku
        )
        SELECT
            latest.latest_period AS period,
            history.category,
            history.sku,
            history.brand,
            history.product_name,
            history.last_seen_period,
            history.hist_periods,
            history.baseline_sales,
            history.max_sales,
            CASE WHEN history.baseline_sales >= ? THEN 'WARNING' ELSE 'INFO' END AS severity
        FROM history
        JOIN latest_category AS latest ON latest.category = history.category
        LEFT JOIN dq_sku_period AS current
            ON current.category = history.category
            AND current.sku = history.sku
            AND current.period = latest.latest_period
        WHERE current.sku IS NULL
            AND history.hist_periods >= ?
            AND history.baseline_sales >= ?
        ORDER BY history.baseline_sales DESC
        LIMIT {min(limit(ctx), cfg.max_business_changes)}
        """,
        [cfg.critical_abs_sales_delta, cfg.min_history_periods, cfg.min_abs_sales_delta],
    )
    issues = [
        issue(
            check_id="sku_sales_drop_to_zero",
            check_name="Значимый SKU исчез из продаж",
            severity=str(row["severity"]),  # type: ignore[arg-type]
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Продажи, шт",
            current_value=0,
            previous_value=None,
            baseline_value=row["baseline_sales"],
            absolute_delta=-float(row["baseline_sales"] or 0),
            relative_delta=0,
            message=(
                f"SKU {row['sku']} не найден в последнем периоде {row['period']}, "
                f"хотя раньше продавался с медианой {fmt(row['baseline_sales'], ' шт.')}."
            ),
            details={
                "brand": row["brand"],
                "product_name": row["product_name"],
                "last_seen_period": row["last_seen_period"],
                "history_periods": row["hist_periods"],
                "max_sales": row["max_sales"],
            },
            suggested_action="Проверь полноту последнего периода и не сменился ли SKU/категория.",
        )
        for row in rows
    ]

    rows = query_rows(
        ctx,
        f"""
        WITH presence AS (
            SELECT
                *,
                LAG(period_date) OVER (PARTITION BY category, sku ORDER BY period_date) AS previous_period_date,
                LAG(period) OVER (PARTITION BY category, sku ORDER BY period_date) AS previous_period
            FROM dq_sku_period
        )
        SELECT period, category, sku, brand, product_name, sales, previous_period
        FROM presence
        WHERE previous_period_date IS NOT NULL
            AND DATE_DIFF('month', previous_period_date, period_date) > 1
            AND sales >= ?
        ORDER BY sales DESC
        LIMIT {min(limit(ctx), cfg.max_business_changes)}
        """,
        [cfg.min_abs_sales_delta],
    )
    issues.extend(
        issue(
            check_id="sku_returned_after_gap",
            check_name="SKU вернулся после отсутствия",
            severity="INFO",
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Продажи, шт",
            current_value=row["sales"],
            previous_value=0,
            baseline_value=None,
            absolute_delta=row["sales"],
            relative_delta=None,
            message=f"SKU {row['sku']} снова появился в {row['period']} после периода {row['previous_period']}.",
            details={"brand": row["brand"], "product_name": row["product_name"]},
            suggested_action="Отметь как бизнес-изменение или проверь пропущенную выгрузку между периодами.",
        )
        for row in rows
    )
    return issues
