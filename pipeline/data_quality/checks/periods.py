from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_period_checks(ctx: QualityContext) -> list[QualityIssue]:
    if not ctx.has("date", "category"):
        ctx.skip("Непрерывность периодов", "Нужны колонки даты и категории.")
        return []
    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        WITH bounds AS (
            SELECT category, MIN(period_date) AS min_period_date, MAX(period_date) AS max_period_date
            FROM dq_category_period
            GROUP BY category
        ),
        expected AS (
            SELECT
                bounds.category,
                STRFTIME(CAST(series.generate_series AS DATE), '%Y-%m') AS period
            FROM bounds
            CROSS JOIN generate_series(bounds.min_period_date, bounds.max_period_date, INTERVAL 1 MONTH) AS series
        )
        SELECT expected.category, expected.period
        FROM expected
        LEFT JOIN dq_category_period AS actual
            ON actual.category = expected.category AND actual.period = expected.period
        WHERE actual.period IS NULL
        ORDER BY expected.category, expected.period
        LIMIT {limit(ctx)}
        """
    )
    out = [
        issue(
            check_id="missing_period",
            check_name="Пропущенный период в категории",
            severity="WARNING",
            entity_type="period",
            entity_id=f"{row['category']}::{row['period']}",
            category=row["category"],
            period=row["period"],
            metric_name="Период",
            current_value=0,
            message=f"В категории {row['category']} отсутствует период {row['period']} между загруженными месяцами.",
            details={},
            suggested_action="Проверь, был ли месяц выгружен из MPStats и сохранён в проект.",
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
                MEDIAN(row_count) OVER w AS baseline_rows,
                MEDIAN(sku_count) OVER w AS baseline_skus
            FROM dq_category_period
            WINDOW w AS (
                PARTITION BY category
                ORDER BY period_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        )
        SELECT
            period, category, row_count, sku_count, baseline_rows, baseline_skus,
            row_count / NULLIF(baseline_rows, 0) AS row_ratio,
            sku_count / NULLIF(baseline_skus, 0) AS sku_ratio
        FROM hist
        WHERE hist_periods >= ?
            AND baseline_rows >= ?
            AND (
                row_count / NULLIF(baseline_rows, 0) >= ?
                OR row_count / NULLIF(baseline_rows, 0) <= ?
                OR sku_count / NULLIF(baseline_skus, 0) >= ?
                OR sku_count / NULLIF(baseline_skus, 0) <= ?
            )
        ORDER BY GREATEST(
            row_count / NULLIF(baseline_rows, 0),
            baseline_rows / NULLIF(row_count, 0),
            sku_count / NULLIF(baseline_skus, 0),
            baseline_skus / NULLIF(sku_count, 0)
        ) DESC
        LIMIT {limit(ctx)}
        """,
        [
            cfg.min_history_periods,
            cfg.min_rows_for_period_checks,
            cfg.row_count_spike_ratio,
            cfg.row_count_drop_ratio,
            cfg.sku_count_spike_ratio,
            cfg.row_count_drop_ratio,
        ],
    )
    latest_period = ctx.prepared.latest_period
    for row in rows:
        is_latest = bool(latest_period and row["period"] == latest_period)
        severity = "INFO" if is_latest and cfg.ignore_latest_incomplete_period else "WARNING"
        if float(row["row_ratio"] or 0) >= cfg.row_count_spike_ratio or float(row["sku_ratio"] or 0) >= cfg.sku_count_spike_ratio:
            check_id = "category_period_row_spike"
            check_name = "Резкий рост строк/SKU в периоде"
            message = (
                f"В категории {row['category']} за {row['period']} стало {fmt(row['row_count'])} строк "
                f"против медианы {fmt(row['baseline_rows'])}."
            )
            suggested = "Проверь дубли файлов, расширение категории или повторную загрузку."
        else:
            check_id = "category_period_row_drop"
            check_name = "Резкое падение строк/SKU в периоде"
            message = (
                f"В категории {row['category']} за {row['period']} осталось {fmt(row['row_count'])} строк "
                f"против медианы {fmt(row['baseline_rows'])}."
            )
            suggested = "Проверь частичную загрузку периода; для последнего месяца это может быть нормально."
        out.append(
            issue(
                check_id=check_id,
                check_name=check_name,
                severity=severity,  # type: ignore[arg-type]
                entity_type="category",
                entity_id=row["category"],
                category=row["category"],
                period=row["period"],
                metric_name="Количество строк",
                current_value=row["row_count"],
                baseline_value=row["baseline_rows"],
                absolute_delta=float(row["row_count"] or 0) - float(row["baseline_rows"] or 0),
                relative_delta=row["row_ratio"],
                message=message,
                details={"sku_count": row["sku_count"], "baseline_skus": row["baseline_skus"], "sku_ratio": row["sku_ratio"]},
                suggested_action=suggested,
            )
        )

    if ctx.has("sales") or ctx.has("revenue"):
        rows = query_rows(
            ctx,
            f"""
            SELECT
                period,
                category,
                COUNT(*) AS row_count,
                SUM(CASE WHEN COALESCE(sales, 0) = 0 AND COALESCE(revenue, 0) = 0 THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS zero_metric_share
            FROM dq_base
            WHERE period IS NOT NULL AND category IS NOT NULL AND category <> ''
            GROUP BY period, category
            HAVING COUNT(*) >= ? AND zero_metric_share >= ?
            ORDER BY zero_metric_share DESC, row_count DESC
            LIMIT {limit(ctx)}
            """,
            [cfg.min_rows_for_period_checks, cfg.zero_metric_share_threshold],
        )
        out.extend(
            issue(
                check_id="period_mostly_zero_metrics",
                check_name="В периоде большинство метрик нулевые",
                severity="WARNING",
                entity_type="period",
                entity_id=f"{row['category']}::{row['period']}",
                category=row["category"],
                period=row["period"],
                metric_name="Доля нулевых продаж/ТО",
                current_value=row["zero_metric_share"],
                message=(
                    f"В категории {row['category']} за {row['period']} доля строк с нулевыми продажами и ТО "
                    f"составляет {float(row['zero_metric_share'] or 0) * 100:.1f}%."
                ),
                details={"row_count": row["row_count"]},
                suggested_action="Проверь фильтры выгрузки и полноту метрик MPStats.",
            )
            for row in rows
        )
    return out
