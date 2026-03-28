"""
main_swarm.py — Logistics Analysis Swarm v2
============================================
8-agent pipeline for EU logistics forecast quality analysis.
  1  Orchestrator       — Country Z-Score flagging (reads wide-format Excel)
  2  GranularitySpec    — Lane-type Z-Score drill-down
  3  AnomalyHunter      — WMAPE bias tracker + YoY seasonality check
  4  RouteSpecialist    — Per-route Z-Score + ghost-route detection
  5  CorrectionAgent    — Gemini Flash: overrides with confidence scores
  6  CriticAgent        — Gemini Flash: second-pass critique
  7  ForecastMerger     — Write override columns to Excel copy
  8  DashboardAgent     — Inject data bundle into dashboard_template.html
"""

import json, math, os
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
if api_key:
    genai.configure(api_key=api_key)
else:
    print("WARNING: No Gemini API key found in environment.")

FORECAST_PATH = "forecasts/weekly_forecast_data.xlsx"
ACTUALS_PATH  = "actuals/historical_actuals.xlsx"
OVERRIDES_OUT = "proposed_overrides.json"
REVIEW_OUT    = "flagged_for_review.json"
TEMPLATE_PATH = "dashboard_template.html"
DASHBOARD_OUT = "forecast_quality_dashboard.html"


def _zscore(series: pd.Series) -> pd.Series:
    mu, sigma = series.mean(), series.std()
    if sigma == 0:
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - mu) / sigma


def _safe_pct(val) -> str:
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return "N/A"
        return f"{'+' if val > 0 else ''}{round(val * 100, 1)}%"
    except Exception:
        return "N/A"


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 1 — ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────
class Orchestrator:
    def __init__(self, path: str = FORECAST_PATH):
        print("[Agent 1: Orchestrator] Loading forecast Excel...")
        self.df   = pd.read_excel(path, engine="openpyxl")
        self.path = path

    def run(self):
        print("[Agent 1: Orchestrator] Z-Score analysis at Country level...")
        agg = (self.df.groupby("country")
               .agg(vol_prior=("volume_v_prior", "sum"),
                    vol_current=("volume_v_current", "sum"))
               .reset_index())
        agg["pct_diff"] = (agg["vol_current"] - agg["vol_prior"]) / agg["vol_prior"]
        agg["z_score"]  = _zscore(agg["pct_diff"])

        flagged = agg[agg["z_score"].abs() > 1.5]["country"].tolist()
        print(f"  Flagged Countries: {flagged} (|Z| > 1.5)")
        return self.df[self.df["country"].isin(flagged)].copy(), agg, self.df


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 2 — GRANULARITY SPECIALIST
# ─────────────────────────────────────────────────────────────────────────────
class GranularitySpecialist:
    def run(self, df_flagged: pd.DataFrame):
        print("[Agent 2: Granularity Specialist] Z-Score on Country × Lane Type...")
        agg = (df_flagged.groupby(["country", "lane_type"])
               .agg(vol_prior=("volume_v_prior", "sum"),
                    vol_current=("volume_v_current", "sum"))
               .reset_index())
        agg["pct_diff"] = (agg["vol_current"] - agg["vol_prior"]) / agg["vol_prior"]
        agg["z_score"]  = _zscore(agg["pct_diff"])

        flagged_lanes = agg[agg["z_score"].abs() > 1.0]
        print(f"  Flagged {len(flagged_lanes)} Country-Lane slices (|Z| > 1.0).")
        merged = pd.merge(df_flagged, flagged_lanes[["country", "lane_type"]],
                          on=["country", "lane_type"], how="inner")
        return merged, agg


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 3 — ANOMALY HUNTER  (WMAPE proxy + YoY)
# ─────────────────────────────────────────────────────────────────────────────
class AnomalyHunter:
    def __init__(self, path: str = ACTUALS_PATH):
        print("[Agent 3: Anomaly Hunter] Loading 2-year historical actuals...")
        self.actuals      = pd.read_excel(path, engine="openpyxl")
        self.actuals["date"] = pd.to_datetime(self.actuals["date"])

    def run(self, df_lanes: pd.DataFrame):
        print("[Agent 3: Anomaly Hunter] Computing WMAPE & YoY seasonality...")
        # Per-route historical stats
        hist = (self.actuals.groupby("route")
                .agg(hist_mean=("actual_volume", "mean"),
                     hist_std=("actual_volume", "std"))
                .reset_index())

        # YoY: same week 2025-04-01 ± 10 days
        yoy_centre = pd.Timestamp("2025-04-01")
        yoy_act = (self.actuals[
                       (self.actuals["date"] >= yoy_centre - pd.Timedelta(days=7)) &
                       (self.actuals["date"] <= yoy_centre + pd.Timedelta(days=14))]
                   .groupby("route")
                   .agg(yoy_vol=("actual_volume", "sum"))
                   .reset_index())

        # Aggregate forecast per route
        route_fcst = (df_lanes.groupby(["route", "country", "lane_type"])
                      .agg(vol_prior=("volume_v_prior", "sum"),
                           vol_current=("volume_v_current", "sum"),
                           qty_prior=("qty_v_prior", "sum"),
                           qty_current=("qty_v_current", "sum"))
                      .reset_index())

        df = route_fcst.merge(hist, on="route", how="left").merge(yoy_act, on="route", how="left")

        df["current_vs_prior_pct"] = (df["vol_current"] - df["vol_prior"]) / df["vol_prior"]
        df["z_score_route"]        = _zscore(df["current_vs_prior_pct"])
        df["forecast_bias_pct"]    = ((df["vol_prior"] - df["hist_mean"])
                                      .div(df["hist_mean"].replace(0, float("nan"))))
        df["yoy_delta_pct"]        = ((df["vol_current"] - df["yoy_vol"])
                                      .div(df["yoy_vol"].replace(0, float("nan"))))
        return df


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 4 — ROUTE SPECIALIST  (Top-20 + ghost detection)
# ─────────────────────────────────────────────────────────────────────────────
class RouteSpecialist:
    def run(self, df_trends: pd.DataFrame, df_full: pd.DataFrame, actuals: pd.DataFrame):
        print("[Agent 4: Route Specialist] Z-Score route filtering & ghost-route detection...")
        df_dev = df_trends[df_trends["z_score_route"].abs() > 1.0].copy()
        df_dev["abs_vol_diff"] = (df_dev["vol_current"] - df_dev["vol_prior"]).abs()
        top20 = df_dev.sort_values("abs_vol_diff", ascending=False).head(20).reset_index(drop=True)
        print(f"  Top {len(top20)} routes identified (|Z| > 1.0).")

        # Ghost routes: high-volume forecast, zero actuals history
        routes_with_actuals = set(actuals["route"].unique())
        route_vol = df_full.groupby("route")["volume_v_current"].sum()
        p75 = route_vol.quantile(0.75)
        ghost_routes = [
            {"route": r, "total_forecast_volume": round(float(route_vol[r]), 1)}
            for r in route_vol.index
            if r not in routes_with_actuals and route_vol[r] >= p75
        ]
        ghost_routes = sorted(ghost_routes, key=lambda x: x["total_forecast_volume"], reverse=True)[:20]
        print(f"  Ghost routes: {len(ghost_routes)}")
        return top20, ghost_routes


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 5 — CORRECTION AGENT  (Gemini -> JSON w/ confidence)
# ─────────────────────────────────────────────────────────────────────────────
class CorrectionAgent:
    MODEL = "gemini-flash-latest"

    def _call_gemini(self, prompt: str, save_path: str) -> list:
        try:
            model = genai.GenerativeModel(self.MODEL)
            raw = model.generate_content(prompt).text.strip()
            # Strip markdown fences if present
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip().rstrip("```")
            result = json.loads(raw)
            with open(save_path, "w") as f:
                json.dump(result, f, indent=2)
            print(f"  [SUCCESS] Saved {len(result)} items to {save_path}.")
            return result
        except Exception as e:
            print(f"  [ERROR] Gemini call failed: {e}")
            if os.path.exists(save_path):
                with open(save_path) as f:
                    return json.load(f)
            return []

    def run(self, top20: pd.DataFrame) -> list:
        print("[Agent 5: Correction Agent] Generating overrides with confidence scores...")
        cols = ["route", "country", "lane_type", "vol_prior", "vol_current",
                "hist_mean", "hist_std", "forecast_bias_pct",
                "current_vs_prior_pct", "yoy_delta_pct", "z_score_route"]
        payload = top20[cols].round(4).to_json(orient="records")

        prompt = f"""You are the Correction Agent in a Logistics Analysis Swarm.
Review the following JSON array of flagged logistics routes and propose corrections.

Field glossary:
- vol_prior / vol_current: Forecasted cubic volume (prior vs current versions)
- hist_mean / hist_std: 2-year historical weekly average and std-dev of actual volume
- forecast_bias_pct: chronic bias of v_prior vs history (positive = over-forecast)
- current_vs_prior_pct: % change between forecast versions
- yoy_delta_pct: current forecast vs same week last year actuals (null = no data)
- z_score_route: how many std-devs the route's change is from the group norm

Override logic:
- z_score > 2.5 AND small yoy_delta_pct -> planned seasonal peak, adjust toward hist_mean × 1.15
- z_score > 2.5 AND null yoy data -> spike unvalidated, revert 70% toward vol_prior
- forecast_bias_pct > 0.10 -> apply standing haircut proportional to bias
- proposed_qty = round(proposed_volume / 1.5)
- confidence_score 0.0–1.0: high if rich actuals, low if yoy null/missing
- review_flag = true if confidence_score < 0.5

Return ONLY a raw JSON array (no markdown), schema:
[{{"route":"string","proposed_volume":float,"proposed_qty":int,
   "confidence_score":float,"review_flag":bool,"justification":"string"}}]

Data:
{payload}"""
        return self._call_gemini(prompt, OVERRIDES_OUT)


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 6 — CRITIC AGENT  (Second-pass Gemini challenge)
# ─────────────────────────────────────────────────────────────────────────────
class CriticAgent(CorrectionAgent):
    def run(self, overrides: list) -> list:
        print("[Agent 6: Critic Agent] Challenging proposed overrides...")
        if not overrides:
            return overrides

        prompt = f"""You are a Logistics Forecast Critic Agent.
Review these proposed overrides generated by a Correction Agent.

For each override:
1. Evaluate if proposed_volume is defensible given the justification.
2. Add a 'critic_comment': one concise sentence validating or challenging the override.
3. Adjust 'confidence_score' by up to ±0.15 based on your critique.
4. Do NOT change proposed_volume unless there is a clear logical error.
5. Keep review_flag = true if adjusted confidence_score < 0.5.

Return ONLY the same JSON array augmented with 'critic_comment' and adjusted 'confidence_score' (no markdown).

Proposed overrides:
{json.dumps(overrides, indent=2)}"""

        criticized = self._call_gemini(prompt, OVERRIDES_OUT)

        # Separate items needing human review
        review_items = [o for o in criticized
                        if o.get("review_flag") or o.get("confidence_score", 1.0) < 0.5]
        if review_items:
            with open(REVIEW_OUT, "w") as f:
                json.dump(review_items, f, indent=2)
            print(f"  [INFO] {len(review_items)} items flagged for human review -> {REVIEW_OUT}")
        return criticized


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 7 — FORECAST MERGER
# ─────────────────────────────────────────────────────────────────────────────
class ForecastMerger:
    def run(self, forecast_path: str, overrides: list) -> str:
        print("[Agent 7: Forecast Merger] Stamping override columns onto Excel copy...")
        if not overrides:
            print("  No overrides to merge.")
            return ""
        omap = {o["route"]: {"override_qty": o.get("proposed_qty"),
                              "override_volume": o.get("proposed_volume")}
                for o in overrides}
        df = pd.read_excel(forecast_path, engine="openpyxl")
        df["override_qty"]    = df["route"].map(lambda r: omap.get(r, {}).get("override_qty"))
        df["override_volume"] = df["route"].map(lambda r: omap.get(r, {}).get("override_volume"))
        p = Path(forecast_path)
        out = p.parent / f"{p.stem}_overrides{p.suffix}"
        df.to_excel(out, index=False, engine="openpyxl")
        print(f"  [SUCCESS] Saved: {out}")
        return str(out)


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 8 — DASHBOARD AGENT  (injects data bundle into HTML template)
# ─────────────────────────────────────────────────────────────────────────────
class DashboardAgent:
    def run(self, country_agg, lane_agg, top20, overrides, ghost_routes, forecast_path):
        print("[Agent 8: Dashboard] Building interactive dashboard...")

        def _f(v, pct=False):
            try:
                if v is None or (isinstance(v, float) and math.isnan(v)):
                    return None
                return round(float(v) * 100, 2) if pct else round(float(v), 4)
            except Exception:
                return None

        c = country_agg.fillna(0)
        la = lane_agg.fillna(0)
        la["label"] = la["country"] + " · " + la["lane_type"]
        la = la.sort_values("z_score", ascending=False)
        t  = top20.fillna(0)

        data = {
            "fc_name": Path(forecast_path).name,
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "kpis": {
                "n_flagged":   int((c["z_score"].abs() > 1.5).sum()),
                "n_top20":     len(top20),
                "n_overrides": len(overrides),
                "n_review":    sum(1 for o in overrides
                                   if o.get("review_flag") or o.get("confidence_score", 1) < 0.5),
                "avg_conf":    round(sum(o.get("confidence_score", 0.8)
                                        for o in overrides) / max(len(overrides), 1), 2),
                "n_ghost":     len(ghost_routes),
            },
            "country": {
                "labels":   c["country"].tolist(),
                "prior":    c["vol_prior"].tolist(),
                "current":  c["vol_current"].tolist(),
                "pct":      [_f(v, pct=True) for v in c["pct_diff"]],
                "z":        [_f(v) for v in c["z_score"]],
            },
            "lane": {
                "labels":  la["label"].tolist(),
                "prior":   la["vol_prior"].tolist(),
                "current": la["vol_current"].tolist(),
                "pct":     [_f(v, pct=True) for v in la["pct_diff"]],
                "z":       [_f(v) for v in la["z_score"]],
            },
            "top20": [
                {
                    "route":   r["route"],
                    "country": r["country"],
                    "lane":    r["lane_type"],
                    "vol_prior":   round(float(r["vol_prior"]), 1),
                    "vol_current": round(float(r["vol_current"]), 1),
                    "pct":     _f(r["current_vs_prior_pct"], pct=True),
                    "z":       _f(r["z_score_route"]),
                    "bias":    _f(r["forecast_bias_pct"], pct=True),
                    "yoy":     _f(r["yoy_delta_pct"], pct=True),
                }
                for _, r in t.iterrows()
            ],
            "wmape": {
                "labels": (t.groupby("lane_type")["forecast_bias_pct"].mean()
                           .reset_index()["lane_type"].tolist()),
                "values": [_f(v, pct=True) for v in
                           t.groupby("lane_type")["forecast_bias_pct"].mean().values],
            },
            "yoy": {
                "routes": t["route"].tolist(),
                "values": [_f(v, pct=True) for v in t["yoy_delta_pct"]],
            },
            "overrides": [
                {
                    "route":      o["route"],
                    "qty":        o.get("proposed_qty"),
                    "volume":     o.get("proposed_volume"),
                    "confidence": o.get("confidence_score", 0.8),
                    "review":     bool(o.get("review_flag") or
                                       o.get("confidence_score", 1) < 0.5),
                    "justification": o.get("justification", ""),
                    "critic":     o.get("critic_comment", ""),
                }
                for o in overrides
            ],
            "ghost_routes": ghost_routes,
        }

        # Read template and inject data
        if not os.path.exists(TEMPLATE_PATH):
            print(f"  [ERROR] {TEMPLATE_PATH} not found. Skipping dashboard.")
            return
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            template = f.read()

        data_script = f"<script>const D={json.dumps(data)};</script>"
        output = template.replace("<!-- __DATA__ -->", data_script)

        with open(DASHBOARD_OUT, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"  [SUCCESS] Dashboard saved: {DASHBOARD_OUT}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  Logistics Analysis Swarm v2")
    print("=" * 60)

    # Agent 1
    orch = Orchestrator(FORECAST_PATH)
    df_flagged, country_agg, df_full = orch.run()

    if df_flagged.empty:
        print("No country-level anomalies detected. Pipeline complete.")
        exit(0)

    # Agent 2
    gran = GranularitySpecialist()
    df_lanes, lane_agg = gran.run(df_flagged)

    # Agent 3
    hunter = AnomalyHunter(ACTUALS_PATH)
    df_trends = hunter.run(df_lanes)

    # Agent 4
    router = RouteSpecialist()
    top20, ghost_routes = router.run(df_trends, df_full, hunter.actuals)

    # Agent 5
    corrector = CorrectionAgent()
    overrides = corrector.run(top20)

    # Agent 6
    critic = CriticAgent()
    overrides = critic.run(overrides)

    # Agent 7
    merger = ForecastMerger()
    merger.run(FORECAST_PATH, overrides)

    # Agent 8
    dash = DashboardAgent()
    dash.run(country_agg, lane_agg, top20, overrides, ghost_routes, FORECAST_PATH)

    print("\n" + "=" * 60)
    print("  Swarm Complete.")
    print(f"  Overrides:  {OVERRIDES_OUT}")
    print(f"  Review:     {REVIEW_OUT}")
    print(f"  Dashboard:  {DASHBOARD_OUT}")
    print("=" * 60)
