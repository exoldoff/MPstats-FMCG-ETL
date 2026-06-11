from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DataQualityConfig:
    """Пороговые значения для объяснимых бизнес-проверок MPStats.

    Числа подобраны как безопасный MVP: они ловят крупные выбросы и не шумят на
    малых SKU, где рост с 1 до 5 штук математически большой, но бизнес-эффект
    обычно несущественный.
    """

    min_history_periods: int = 3
    min_abs_sales_delta: float = 50.0
    critical_abs_sales_delta: float = 1_000.0
    new_sku_high_sales_threshold: float = 500.0
    new_sku_critical_sales_threshold: float = 5_000.0
    relative_growth_threshold: float = 5.0
    critical_growth_threshold: float = 20.0
    relative_drop_threshold: float = 0.2
    critical_drop_threshold: float = 0.05
    robust_z_score_threshold: float = 6.0
    percentile_iqr_multiplier: float = 3.0

    min_abs_revenue_delta: float = 50_000.0
    critical_abs_revenue_delta: float = 500_000.0
    new_sku_high_revenue_threshold: float = 250_000.0
    new_sku_critical_revenue_threshold: float = 2_000_000.0
    revenue_growth_threshold: float = 5.0
    critical_revenue_growth_threshold: float = 15.0
    revenue_drop_threshold: float = 0.2

    max_price_change_ratio: float = 3.0
    critical_price_change_ratio: float = 10.0
    unit_price_category_ratio: float = 5.0
    min_sales_for_price_checks: float = 5.0

    min_category_share_for_alert: float = 0.25
    min_brand_share_for_alert: float = 0.35
    share_delta_for_alert: float = 0.2
    top_sku_dominance_ratio: float = 5.0

    row_count_spike_ratio: float = 5.0
    row_count_drop_ratio: float = 0.35
    sku_count_spike_ratio: float = 5.0
    duplicate_sku_critical_count: int = 5

    tolerance_revenue_price_sales: float = 0.25
    zero_metric_share_threshold: float = 0.8
    min_rows_for_period_checks: int = 10
    ignore_latest_incomplete_period: bool = True

    max_issues_per_check: int = 50
    max_business_changes: int = 50

