from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_duplicate_checks(ctx: QualityContext) -> list[QualityIssue]:
    if not ctx.has("date", "category", "sku"):
        ctx.skip("Бизнес-дубли SKU", "Нужны колонки даты, категории и SKU.")
        return []
    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        SELECT
            period,
            category,
            sku,
            COUNT(*) AS duplicate_rows,
            COUNT(DISTINCT source_file) AS source_files,
            SUM(COALESCE(sales, 0)) AS sales,
            SUM(COALESCE(revenue, 0)) AS revenue,
            CASE WHEN COUNT(*) >= ? THEN 'CRITICAL' ELSE 'WARNING' END AS severity
        FROM dq_base
        WHERE period IS NOT NULL
            AND category IS NOT NULL AND category <> ''
            AND sku IS NOT NULL AND sku <> ''
        GROUP BY period, category, sku
        HAVING COUNT(*) > 1
        ORDER BY duplicate_rows DESC, revenue DESC, sales DESC
        LIMIT {limit(ctx)}
        """,
        [cfg.duplicate_sku_critical_count],
    )
    out = [
        issue(
            check_id="duplicate_sku_period_count",
            check_name="SKU повторяется в одном периоде и категории",
            severity=str(row["severity"]),  # type: ignore[arg-type]
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Количество строк",
            current_value=row["duplicate_rows"],
            message=(
                f"SKU {row['sku']} встречается {fmt(row['duplicate_rows'])} раза в категории "
                f"{row['category']} за {row['period']}."
            ),
            details={"source_files": row["source_files"], "sales": row["sales"], "revenue": row["revenue"]},
            suggested_action="Проверь, это разные продавцы/варианты или дубль одной позиции.",
        )
        for row in rows
    ]

    rows = query_rows(
        ctx,
        f"""
        SELECT
            period,
            category,
            sku,
            sales,
            revenue,
            price,
            COUNT(*) AS duplicate_rows
        FROM dq_base
        WHERE period IS NOT NULL
            AND category IS NOT NULL AND category <> ''
            AND sku IS NOT NULL AND sku <> ''
        GROUP BY period, category, sku, sales, revenue, price
        HAVING COUNT(*) > 1
        ORDER BY duplicate_rows DESC, COALESCE(revenue, 0) DESC
        LIMIT {limit(ctx)}
        """
    )
    out.extend(
        issue(
            check_id="duplicate_metric_rows",
            check_name="SKU повторяется с одинаковыми метриками",
            severity="WARNING",
            entity_type="sku",
            entity_id=row["sku"],
            category=row["category"],
            period=row["period"],
            metric_name="Количество строк",
            current_value=row["duplicate_rows"],
            message=(
                f"SKU {row['sku']} имеет {fmt(row['duplicate_rows'])} строк с одинаковыми продажами, ТО и ценой."
            ),
            details={"sales": row["sales"], "revenue": row["revenue"], "price": row["price"]},
            suggested_action="Проверь точный дубль внутри выгрузки или повторное попадание файла.",
        )
        for row in rows
    )
    return out
