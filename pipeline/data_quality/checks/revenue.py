from __future__ import annotations

from pipeline.data_quality.checks.utils import fmt, issue, limit, pct, query_rows
from pipeline.data_quality.models import QualityContext, QualityIssue


def run_revenue_checks(ctx: QualityContext) -> list[QualityIssue]:
    if not ctx.has("date", "category", "revenue"):
        ctx.skip("Аномалии выручки/ТО", "Нужны колонки даты, категории и выручки.")
        return []
    cfg = ctx.config
    rows = query_rows(
        ctx,
        f"""
        WITH hist AS (
            SELECT
                *,
                COUNT(*) OVER w AS hist_periods,
                LAG(revenue) OVER (PARTITION BY category ORDER BY period_date) AS previous_revenue,
                MEDIAN(revenue) OVER w AS baseline_revenue
            FROM dq_category_period
            WINDOW w AS (
                PARTITION BY category
                ORDER BY period_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        )
        SELECT
            period, category, revenue, previous_revenue, baseline_revenue,
            revenue - baseline_revenue AS abs_delta,
            revenue / NULLIF(baseline_revenue, 0) AS ratio,
            CASE
                WHEN revenue - baseline_revenue >= ? AND revenue / NULLIF(baseline_revenue, 0) >= ? THEN 'CRITICAL'
                ELSE 'WARNING'
            END AS severity
        FROM hist
        WHERE hist_periods >= ?
            AND baseline_revenue >= ?
            AND revenue - baseline_revenue >= ?
            AND revenue / NULLIF(baseline_revenue, 0) >= ?
        ORDER BY abs_delta DESC
        LIMIT {limit(ctx)}
        """,
        [
            cfg.critical_abs_revenue_delta,
            cfg.critical_revenue_growth_threshold,
            cfg.min_history_periods,
            max(1.0, cfg.min_abs_revenue_delta / 5),
            cfg.min_abs_revenue_delta,
            cfg.revenue_growth_threshold,
        ],
    )
    out = [
        issue(
            check_id="category_revenue_spike",
            check_name="Резкий рост ТО категории",
            severity=str(row["severity"]),  # type: ignore[arg-type]
            entity_type="category",
            entity_id=row["category"],
            category=row["category"],
            period=row["period"],
            metric_name="Выручка, руб",
            current_value=row["revenue"],
            previous_value=row["previous_revenue"],
            baseline_value=row["baseline_revenue"],
            absolute_delta=row["abs_delta"],
            relative_delta=row["ratio"],
            message=(
                f"Категория {row['category']} в {row['period']} выросла до "
                f"{fmt(row['revenue'], ' руб.')} против медианы {fmt(row['baseline_revenue'], ' руб.')}."
            ),
            details={},
            suggested_action="Проверь, не попал ли в период дубль файла или чужая категория.",
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
                LAG(revenue) OVER (PARTITION BY category ORDER BY period_date) AS previous_revenue,
                MEDIAN(revenue) OVER w AS baseline_revenue
            FROM dq_category_period
            WINDOW w AS (
                PARTITION BY category
                ORDER BY period_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        )
        SELECT
            period, category, revenue, previous_revenue, baseline_revenue,
            baseline_revenue - revenue AS abs_delta,
            revenue / NULLIF(baseline_revenue, 0) AS ratio,
            CASE WHEN revenue <= baseline_revenue * ? THEN 'CRITICAL' ELSE 'WARNING' END AS severity
        FROM hist
        WHERE hist_periods >= ?
            AND baseline_revenue >= ?
            AND baseline_revenue - revenue >= ?
            AND revenue <= baseline_revenue * ?
        ORDER BY abs_delta DESC
        LIMIT {limit(ctx)}
        """,
        [
            cfg.critical_drop_threshold,
            cfg.min_history_periods,
            cfg.min_abs_revenue_delta,
            cfg.min_abs_revenue_delta,
            cfg.revenue_drop_threshold,
        ],
    )
    out.extend(
        issue(
            check_id="category_revenue_drop",
            check_name="Резкое падение ТО категории",
            severity=str(row["severity"]),  # type: ignore[arg-type]
            entity_type="category",
            entity_id=row["category"],
            category=row["category"],
            period=row["period"],
            metric_name="Выручка, руб",
            current_value=row["revenue"],
            previous_value=row["previous_revenue"],
            baseline_value=row["baseline_revenue"],
            absolute_delta=-float(row["abs_delta"] or 0),
            relative_delta=row["ratio"],
            message=(
                f"Категория {row['category']} в {row['period']} упала до "
                f"{fmt(row['revenue'], ' руб.')} против медианы {fmt(row['baseline_revenue'], ' руб.')}."
            ),
            details={},
            suggested_action="Проверь частичную загрузку периода и фильтры категории.",
        )
        for row in rows
    )

    if ctx.has("brand"):
        rows = query_rows(
            ctx,
            f"""
            WITH hist AS (
                SELECT
                    *,
                    COUNT(*) OVER w AS hist_periods,
                    LAG(revenue_share) OVER (PARTITION BY category, brand ORDER BY period_date) AS previous_share,
                    MEDIAN(revenue_share) OVER w AS baseline_share
                FROM dq_brand_period
                WINDOW w AS (
                    PARTITION BY category, brand
                    ORDER BY period_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                )
            )
            SELECT
                period, category, brand, revenue, revenue_share, previous_share, baseline_share,
                revenue_share - baseline_share AS share_delta
            FROM hist
            WHERE hist_periods >= ?
                AND revenue_share >= ?
                AND revenue_share - COALESCE(baseline_share, 0) >= ?
            ORDER BY share_delta DESC, revenue DESC
            LIMIT {limit(ctx)}
            """,
            [cfg.min_history_periods, cfg.min_brand_share_for_alert, cfg.share_delta_for_alert],
        )
        out.extend(
            issue(
                check_id="brand_revenue_share_spike",
                check_name="Бренд резко занял долю ТО категории",
                severity="WARNING",
                entity_type="brand",
                entity_id=row["brand"],
                category=row["category"],
                period=row["period"],
                metric_name="Доля ТО",
                current_value=row["revenue_share"],
                previous_value=row["previous_share"],
                baseline_value=row["baseline_share"],
                absolute_delta=row["share_delta"],
                relative_delta=None,
                message=(
                    f"Бренд {row['brand']} вырос до {pct(row['revenue_share'])} ТО категории "
                    f"{row['category']} против обычных {pct(row['baseline_share'])}."
                ),
                details={"revenue": row["revenue"]},
                suggested_action="Проверь, это промо/реальный рост или размножение строк бренда.",
            )
            for row in rows
        )
    return out
