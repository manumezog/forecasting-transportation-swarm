"""
dummy_fcst_generator.py
========================
Synthetic data generator for the EU Logistics Forecast Health-Check pipeline.

PURPOSE
-------
Produces two Excel files that mimic the structure of real EU transportation
forecast exports and their corresponding historical actuals. These files
are used to test the health-check pipeline end-to-end without exposing any
real operational data.

OUTPUT FILES
------------
weekly_forecast_data.xlsx
    One row per route × forecasted date (wide format).
    Both forecast versions are stored as COLUMNS, not separate rows.
    Columns: route, country, lane_type, date,
             qty_v_prior, volume_v_prior,
             qty_v_current, volume_v_current

historical_actuals.xlsx
    Two full years of weekly actual shipment data per route, designed to
    expose real seasonality signals (holiday peaks, summer dips, Q1 slumps).
    Columns: route, date, actual_qty, actual_volume

SEASONALITY MODEL (built into historical_actuals.xlsx)
------------------------------------------------------
Multipliers are applied on top of a stable per-route baseline:
    - Jan–Feb (Q1 slump)    : 0.78 × baseline  (post-holiday demand collapse)
    - Mar–May (Spring ramp) : 1.05 × baseline  (steadily recovering)
    - Jun–Aug (Summer dip)  : 0.90 × baseline  (holiday slowdowns, esp. EU)
    - Sep–Oct (Autumn ramp) : 1.10 × baseline  (back-to-school + pre-Q4)
    - Nov–Dec (Peak season) : 1.40 × baseline  (Black Friday / Christmas surge)

INJECTED ANOMALIES
------------------
Two deliberate anomalies are "rigged" into the *forecast* data so that the
health-check pipeline has real signals to detect:

    1. VARIANCE SPIKE (Germany · Rail)
       All German Rail routes receive a +40–50% uplift in qty_v_current vs
       qty_v_prior, simulating a sudden demand shift (e.g. modal shift from road).

    2. REALITY GAP (Air lanes)
       All Air routes have actuals that are 20–30% below the prior forecast,
       simulating systematic Air over-forecasting (e.g. post-COVID air demand
       collapse not yet reflected in the statistical model). This anomaly has
       also been visible in the 2-year historical trend.

USAGE
-----
    python dummy_fcst_generator.py           # standalone
    import dummy_fcst_generator              # called automatically by master.py
    dummy_fcst_generator.generate_massive_logistics_data()
"""

import math
import pandas as pd
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Seasonality multiplier: returns a float based on the week's month
# ---------------------------------------------------------------------------
def _seasonality_multiplier(dt: datetime, lane_type: str) -> float:
    """
    Returns a seasonality scale factor for a given datetime and lane type.
    Air lanes carry an additional structural under-performance bias to
    simulate systematic over-forecasting for that modality.
    """
    month = dt.month

    # Base seasonal curve (common across all modes)
    if month in (1, 2):
        base = 0.78   # Q1 slump
    elif month in (3, 4, 5):
        base = 1.05   # Spring ramp
    elif month in (6, 7, 8):
        base = 0.90   # Summer holiday dip (especially strong in EU)
    elif month in (9, 10):
        base = 1.10   # Autumn / pre-Q4 ramp
    else:             # Nov, Dec
        base = 1.40   # Peak season (Black Friday + Christmas)

    # A subtle secondary harmonic (adds realistic intra-quarter wobble)
    week_of_year = dt.isocalendar()[1]
    harmonic = 1 + 0.04 * math.sin(2 * math.pi * week_of_year / 52)

    multiplier = base * harmonic

    # Air-specific chronic under-performance vs forecast
    if lane_type == "Air":
        multiplier *= random.uniform(0.70, 0.80)

    return multiplier


def generate_massive_logistics_data():
    """
    Generate synthetic EU logistics forecast and historical actuals Excel files.

    Network topology
    ----------------
    7 countries × 5-6 cities each → up to 1,500 unique origin-destination
    routes. Lane types are assigned at route creation with a realistic
    modal split: Road 60%, Rail/Air/Sea 40% combined.

    Forecast data volume
    --------------------
    Each route × 7 forecasted dates (1 row per date, wide columns).
    ~10,500 forecast rows.

    Historical actuals volume
    -------------------------
    Each route × ~104 weekly dates (2 years), ~88% coverage.
    ~136,500 actuals rows.

    Anomalies
    ---------
    See module docstring above.
    """
    print("Generating enterprise-scale logistics data...")

    # ── 1. Base network definitions (Major European Hubs) ─────────────────
    cities = {
        "Germany":     ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne", "Stuttgart"],
        "France":      ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes"],
        "Spain":       ["Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Malaga"],
        "Italy":       ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa"],
        "UK":          ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool", "Bristol"],
        "Netherlands": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"],
        "Poland":      ["Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan"],
    }

    # Forecasted delivery dates (7 weeks starting 2026-04-01)
    forecast_dates = [
        (datetime(2026, 4, 1) + timedelta(weeks=i)).strftime("%Y-%m-%d")
        for i in range(7)
    ]

    # Historical actuals: 2 years of weekly data, ending the week before the forecast
    HISTORY_WEEKS = 104
    history_start = datetime(2024, 4, 1)   # 2 years before forecast start
    historical_dates = [
        history_start + timedelta(weeks=i)
        for i in range(HISTORY_WEEKS)
    ]

    # ── 2. Generate ~1,500 unique routes ──────────────────────────────────
    master_routes: dict = {}
    while len(master_routes) < 1500:
        orig_country = random.choice(list(cities.keys()))
        dest_country = random.choice(list(cities.keys()))
        orig_city    = random.choice(cities[orig_country])
        dest_city    = random.choice(cities[dest_country])

        if orig_city == dest_city:
            continue

        route_name = f"{orig_city}-{dest_city}"
        lane = "Road" if random.random() < 0.60 else random.choice(["Rail", "Air", "Sea"])

        master_routes[route_name] = {
            "route":      route_name,
            "country":    orig_country,  # Origin country for regional aggregation
            "lane_type":  lane,
        }

    master_routes = list(master_routes.values())

    forecast_data  = []
    actuals_data   = []

    # ── 3. Populate data ──────────────────────────────────────────────────
    for r in master_routes:
        # Coverage flags (mimicking real-world data gaps)
        has_forecast = random.random() < 0.92
        has_actuals  = random.random() < 0.88
        if not has_forecast and not has_actuals:
            has_forecast = True

        # Each route has a stable per-route baseline (50–3,500 packages/week)
        # This ensures the same route has consistent magnitude across time.
        route_base_qty = random.randint(50, 3500)

        # ── FORECAST (wide format: one row per route × forecast date) ─────
        if has_forecast:
            for date_str in forecast_dates:
                # v_prior: add mild ±5% noise on top of route baseline
                v_prior_qty = int(route_base_qty * random.uniform(0.95, 1.05))

                # RIGGED ANOMALY 1: Germany Rail spike in v_current
                if r["country"] == "Germany" and r["lane_type"] == "Rail":
                    v_current_qty = int(v_prior_qty * random.uniform(1.40, 1.50))
                else:
                    v_current_qty = int(v_prior_qty * random.uniform(0.95, 1.05))

                forecast_data.append({
                    "route":            r["route"],
                    "country":          r["country"],
                    "lane_type":        r["lane_type"],
                    "date":             date_str,
                    # Prior forecast version columns
                    "qty_v_prior":      int(v_prior_qty),
                    "volume_v_prior":   int(v_prior_qty * 1.5),
                    # Current forecast version columns
                    "qty_v_current":    int(v_current_qty),
                    "volume_v_current": int(v_current_qty * 1.5),
                })

        # ── HISTORICAL ACTUALS (2 years of weekly data) ───────────────────
        if has_actuals:
            for hist_dt in historical_dates:
                # Apply seasonality + per-week noise
                season_mult = _seasonality_multiplier(hist_dt, r["lane_type"])
                noise       = random.uniform(0.97, 1.03)
                actual_qty  = int(route_base_qty * season_mult * noise)
                # Clamp to a minimum of 1 package
                actual_qty  = max(1, actual_qty)

                actuals_data.append({
                    "route":          r["route"],
                    "date":           hist_dt.strftime("%Y-%m-%d"),
                    "actual_qty":     int(actual_qty),
                    "actual_volume":  int(actual_qty * 1.5),
                })

    # ── 4. Save to Excel ──────────────────────────────────────────────────
    df_forecasts = pd.DataFrame(forecast_data)
    df_actuals   = pd.DataFrame(actuals_data)

    # Sort for readability
    df_forecasts = df_forecasts.sort_values(["country", "route", "date"]).reset_index(drop=True)
    df_actuals   = df_actuals.sort_values(["route", "date"]).reset_index(drop=True)

    try:
        df_forecasts.to_excel("forecasts/weekly_forecast_data.xlsx", index=False)
        df_actuals.to_excel("actuals/historical_actuals.xlsx", index=False)
        
        print(f"✅ Created 'forecasts/weekly_forecast_data.xlsx'  ({len(df_forecasts):,} rows, wide-format)")
        print(f"✅ Created 'actuals/historical_actuals.xlsx'      ({len(df_actuals):,} rows, 2-year history)")
    except Exception as e:
        print(f"❌ Error saving to Excel: {e}")
        print("💡 Make sure you have 'openpyxl' installed: pip install openpyxl")

    print(f"📊 Total Unique Routes: {len(master_routes):,}")
    print(f"📅 Actuals date range:  {historical_dates[0].strftime('%Y-%m-%d')} → {historical_dates[-1].strftime('%Y-%m-%d')}")
    print("🎯 Anomalies Injected:")
    print("   · Germany (Rail) — +40-50% v_current Variance Spike")
    print("   · Air lanes      — structural 20-30% actuals underperformance (visible across 2yr history)")
    print("   · Seasonality    — Q1 slump · spring ramp · summer dip · autumn ramp · peak season")


if __name__ == "__main__":
    # Run standalone to regenerate the Excel files independently of the pipeline
    generate_massive_logistics_data()
    print("   · Germany (Rail) — +40-50% v_current Variance Spike")
    print("   · Air lanes      — structural 20-30% actuals underperformance (visible across 2yr history)")
    print("   · Seasonality    — Q1 slump · spring ramp · summer dip · autumn ramp · peak season")


if __name__ == "__main__":
    # Run standalone to regenerate the CSV files independently of the pipeline
    generate_massive_logistics_data()