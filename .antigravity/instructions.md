# Logistics Swarm: European Transportation Analysis

## Data Schema

- **Forecast Data (`weekly_forecast_data.csv`):** - `version`: 'v_current' (latest) or 'v_prior' (previous week).
  - `qty`: Number of packages.
  - `volume`: Cubic volume (m³).
  - `country`, `lane_type`, `route`, `date`.
- **Actuals Data (`recent_actuals.csv`):**
  - `actual_qty`, `actual_volume` for recent historical dates.

## Analysis Hierarchy

1. **Level 1 (Country):** Aggregate all KPIs by country. Identify where `v_current` vs `v_prior` delta is > 5%.
2. **Level 2 (Granularity):** Drill into `lane_type` (e.g., Road, Air) for problematic countries.
3. **Level 3 (Route):** Analyze individual routes for both version changes and anomalies vs actuals.

## Thresholds for Action

- Flag any route where `v_current` deviates from the average of the last 3 weeks of actuals by > 15%.
- Prioritize overrides for high-volume routes (top 10% by qty).
