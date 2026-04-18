"""
FITPAC Alert Engine - SQLite-Backed
====================================
Reads data from fitpac.db (populated by fitpac_scrapers.py), emits alerts.json
and alerts.js. Signal logic is unchanged — only the data source moved from
CSV to SQLite.
"""

import json
import math
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime, timedelta, timezone

import fitpac_db as db

logging.basicConfig(level=logging.INFO, format='FITPAC_EMIT: [%(levelname)s] %(message)s')
logger = logging.getLogger("FITPAC_Alert_Backend")

OUTPUT_ALERTS_JSON = Path(__file__).parent / "alerts.json"
OUTPUT_ALERTS_JS = Path(__file__).parent / "alerts.js"

# FITPAC Tunable Thresholds
RECENT_WINDOW_MIN = 30
PRIOR_WINDOW_MIN = 30
VETO_INSIDER_THRESHOLD = 0.40
BOT_SWARM_AUTH_FLOOR = 0.30
DYNAMIC_FIRE_SCORE = 1.4
VETO_INFLUENCE_THRESHOLD = 3.0
IGNITION_CONFIDENCE_FLOOR = 0.70


def _parse_ts(s: str) -> datetime:
    # Accept naive ISO or tz-aware ISO
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class SocialTransformerModule:
    """Neural NLP Pipeline, Bot Filtering, VIP Weighting (SQLite-backed)."""

    def __init__(self, vip_registry: Dict[str, float]):
        self.vip_registry = vip_registry

    def process_social_stream(
        self, ticker: str, now: datetime
    ) -> Tuple[float, float, float, List[str]]:
        recent_start = now - timedelta(minutes=RECENT_WINDOW_MIN)
        prior_start = recent_start - timedelta(minutes=PRIOR_WINDOW_MIN)

        recent_posts = db.recent_posts(ticker, recent_start, now)
        prior_posts = db.recent_posts(ticker, prior_start, recent_start)

        r_total = len(recent_posts)
        r_org = sum(1 for p in recent_posts if not p["bot_flag"])
        p_total = len(prior_posts)
        p_org = sum(1 for p in prior_posts if not p["bot_flag"])

        if r_total + p_total == 0:
            logger.info(f"{ticker} | no social posts in either window.")
            return 0.0, 1.0, 0.0, []

        # Hype velocity: organic mention acceleration, squashed via tanh
        ratio = r_org / max(1, p_org)
        hype_velocity = max(0.0, min(1.0, round(math.tanh((ratio - 0.5) * 0.7), 3)))

        # Authenticity: organic share of recent-window posts
        authenticity_score = round(r_org / max(1, r_total), 3) if r_total else 1.0

        if authenticity_score < BOT_SWARM_AUTH_FLOOR:
            logger.warning(f"Bot swarm detected for {ticker} (auth={authenticity_score}). Suppressed.")
            return 0.0, authenticity_score, 0.0, []

        # VIP detection across the full ticker stream
        all_authors = {p["author"] for p in recent_posts + prior_posts}
        triggered_vips = []
        max_mult = 1.0
        for h in all_authors:
            if h in self.vip_registry:
                max_mult = max(max_mult, self.vip_registry[h])
                triggered_vips.append(h)
        triggered_vips.sort()

        influence_weight = round(max_mult * DYNAMIC_FIRE_SCORE, 3)
        logger.info(
            f"{ticker} | posts={r_total}r/{p_total}p | hype={hype_velocity} "
            f"auth={authenticity_score} | VIPs={triggered_vips or '—'}"
        )
        return hype_velocity, authenticity_score, influence_weight, triggered_vips


class OnChainVetoEngine:
    """Ground Truth Validation and Exit Liquidity Protection."""

    def analyze_chain_data(self, ticker: str) -> Tuple[float, float, bool, bool]:
        """Returns (insider_ratio, liquidity_usd, chain_veto_active, chain_data_missing)."""
        snap = db.latest_chain_snapshot(ticker)
        if snap is None:
            logger.warning(f"No chain snapshot for {ticker}; chain_data_missing=True.")
            # TAO rule: without chain data we cannot verify exit liquidity.
            # Flag chain_data_missing so the ensemble blocks the trade from
            # ever reaching VIRAL_IGNITION regardless of social confidence.
            return 0.0, 0.0, False, True

        insider = snap["insider_distribution_ratio"] or 0.0
        liquidity = snap["liquidity_depth_usd"] or 0.0

        veto_active = insider > VETO_INSIDER_THRESHOLD
        if not snap["lp_locked"] and (snap["contract_age_hours"] or 0) < 48:
            veto_active = True
            logger.error(f"{ticker} | LP unlocked on <48h contract. Implicit veto.")

        return float(insider), float(liquidity), bool(veto_active), False


class GBDTVotingEnsemble:
    """XGBoost/LightGBM/CatBoost Stand-In."""

    def predict_pump_probability(self, features: Dict) -> float:
        if features["chain_veto_active"] and features["influence_weight"] > VETO_INFLUENCE_THRESHOLD:
            logger.error("Trap_Evaded: High social hype + insider distribution. VETOED.")
            return 0.0

        hype = features["hype_velocity"]
        auth = features["authenticity_score"]
        infl = min(5.0, features["influence_weight"]) / 5.0
        liq_norm = min(1.0, features["liquidity_depth"] / 500_000)
        insider_penalty = max(0.0, 1.0 - features["insider_distribution_ratio"] * 2.0)

        raw = 0.40 * hype + 0.20 * auth + 0.20 * infl + 0.10 * liq_norm + 0.10 * insider_penalty
        return round(max(0.0, min(1.0, raw)), 3)


def build_payload(ticker: str, prob: float, features: Dict, now: datetime) -> Dict:
    chain_missing = features.get("chain_data_missing", False)
    payload = {
        "timestamp": now.isoformat(),
        "ticker": ticker,
        "signal_status": "VIRAL_IGNITION" if prob > IGNITION_CONFIDENCE_FLOOR else "STANDBY",
        "confidence_score": round(prob, 3),
        "social_metrics": {
            "hype_velocity": features["hype_velocity"],
            "authenticity_score": features["authenticity_score"],
            "influence_weight": round(features["influence_weight"], 2),
            "vip_triggers": features["vip_triggers"],
        },
        "chain_metrics": {
            "insider_distribution_ratio": features["insider_distribution_ratio"],
            "liquidity_depth_usd": features["liquidity_depth"],
            "hard_veto_active": features["chain_veto_active"],
            "chain_data_missing": chain_missing,
        },
        "system_warnings": [],
    }
    if features["chain_veto_active"]:
        payload["system_warnings"].append(
            "EXIT LIQUIDITY TRAP DETECTED: Insiders are distributing."
        )
        payload["signal_status"] = "VETOED"
    # TAO rule: no on-chain snapshot → cannot verify exit liquidity → force STANDBY
    if chain_missing and payload["signal_status"] != "VETOED":
        payload["signal_status"] = "STANDBY"
        payload["system_warnings"].append(
            "CHAIN DATA MISSING: no DEX snapshot available; exit liquidity unverified. "
            "Ignition suppressed per TAO rule."
        )
    return payload


def run_backend_cycle(tickers: List[str] = None, now: datetime = None) -> List[Dict]:
    db.init_db()

    vip_registry = db.vip_registry()

    if tickers is None:
        tickers = [r["ticker"] for r in db.list_tickers()]

    if now is None:
        # Use latest DB timestamp as "now" (so seeded/mock data stays in-window)
        import sqlite3
        with db.connect() as conn:
            row = conn.execute(
                "SELECT MAX(ts) AS ts FROM ("
                "  SELECT MAX(timestamp) AS ts FROM social_posts "
                "  UNION ALL SELECT MAX(snapshot_time) FROM chain_snapshots"
                ")"
            ).fetchone()
            latest = row["ts"] if row and row["ts"] else None
        now = _parse_ts(latest) if latest else datetime.now(timezone.utc)

    nlp = SocialTransformerModule(vip_registry)
    chain = OnChainVetoEngine()
    ensemble = GBDTVotingEnsemble()

    payloads = []
    for ticker in tickers:
        logger.info(f"--- Processing {ticker} @ {now.isoformat()} ---")
        hype, auth, influence, vips = nlp.process_social_stream(ticker, now)
        insider, liquidity, veto, chain_missing = chain.analyze_chain_data(ticker)

        features = {
            "hype_velocity": hype,
            "authenticity_score": auth,
            "influence_weight": influence,
            "vip_triggers": vips,
            "insider_distribution_ratio": insider,
            "liquidity_depth": liquidity,
            "chain_veto_active": veto,
            "chain_data_missing": chain_missing,
        }

        prob = ensemble.predict_pump_probability(features)
        payload = build_payload(ticker, prob, features, now)
        payloads.append(payload)
        db.append_alert(payload)

        logger.info(f"{ticker} → {payload['signal_status']} (conf={payload['confidence_score']})")

    OUTPUT_ALERTS_JSON.write_text(json.dumps(payloads, indent=2))
    OUTPUT_ALERTS_JS.write_text(
        "// Auto-generated by fitpac_backend.py — consumed by fitpac_dashboard.html\n"
        "window.FITPAC_ALERTS = " + json.dumps(payloads, indent=2) + ";\n"
    )
    logger.info(f"Wrote {len(payloads)} alerts to alerts.json / alerts.js")
    return payloads


if __name__ == "__main__":
    alerts = run_backend_cycle()
    print("\n--- JSON PAYLOAD ARRAY ---")
    print(json.dumps(alerts, indent=2))
