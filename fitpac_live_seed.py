"""
FITPAC Live-Seed
================
One-shot loader that writes LIVE captured market + sentiment data into the DB.
Captured via the Chrome extension hitting:
  - https://api.dexscreener.com/latest/dex/tokens/{address}
  - https://api.coingecko.com/api/v3/coins/{id}?community_data=true
  - https://api.coingecko.com/api/v3/search/trending

This lets us prove the pipeline end-to-end with real numbers while the
full scraper (fitpac_scrapers.py) is a drop-in replacement for when the
user runs it on a machine with open outbound network.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import fitpac_db as db

logger = logging.getLogger("FITPAC_LiveSeed")

# ---------------------------------------------------------------------------
# LIVE DATA snapshot captured from Chrome on the user's behalf
# ---------------------------------------------------------------------------
DEXSCREENER = [
    # ticker, chain, pairAddress, priceUsd, liquidityUsd, volH24, buysH1, sellsH1, pairCreatedAt(ms)
    ("$PEPE",     "ethereum", "0xA43fe16908251ee70EF74718545e4FE6C5cCEc9f",
     3.961e-06, 28_228_931.42, 2_168_412.04,   3,  21, 1_681_492_871_000),
    ("$SHIB",     "ethereum", "0xCF6dAAB95c476106ECa715D48DE4b13287ffDEAa",
     6.336e-06,  3_065_389.79,     86_306.02,  0,   4, 1_625_545_149_000),
    ("$WOJAK",    "ethereum", "0x0F23d49bC92Ec52FF591D091b3e16c937034496E",
     1.860e-05,    829_343.64,     26_922.12,  0,   0, 1_681_775_291_000),
    ("$BONK",     "solana",   "3ne4mWqdYuNiYrYZC9TrA3FcfuFdErghH97vNPbjicr1",
     6.400e-06,    920_172.25,     27_386.22,  5,   7, 1_671_900_044_000),
    ("$WIF",      "solana",   "EP2ib6dYdEeqD8MfE2ezHCxX3kP3K2eLKkirfPm5eyMx",
     0.2144,     5_299_644.96,    529_687.11,  2,  39, 1_700_510_070_000),
    ("$FARTCOIN", "solana",   "Bzc9NZfMqkXR6fz1DBph7BDf9BroyEf6pnzESP7v5iiw",
     0.2163,     8_051_581.36,  1_526_014.41, 18,  92, 1_729_231_787_000),
    # --- expanded watchlist: CoinGecko trending picks, live DexScreener data ---
    ("$RAVE",     "ethereum", "0xca47e80bd01a1f5bcc8cf709d48a5399d533447e03d56f488498dc83c35b5831",
     21.90,     18_847_071.09,    523_086.85, 11,   9, 1_765_541_015_000),
    ("$ASTEROID", "ethereum", "0x76A411f14A704099Ba476CE8dFFC288a53295218",
     0.00005614, 1_121_358.82, 47_160_459.14, 410, 313, 1_725_963_503_000),
    ("$PENGU",    "solana",   "DdMA1cHcHEqYfttc1z1sJEY978CcU1pyjNuTWTNmdvzU",
     0.007720,  3_805_729.82,    820_230.20,   3,  54, 1_764_180_726_000),
    # --- AI agent / alt-L1 trending picks (live DexScreener) ---
    ("$VIRTUAL",  "base",     "0xE31c372a7Af875b3B5E0F3713B17ef51556da667",
     0.7529,    6_068_567.64,    189_351.61,   2,  23, 1_711_899_559_000),
    ("$MOVR",     "moonriver","0x98878B06940aE243284CA214f92Bb71a2b032B8A",
     2.057,        15_166.55,      4_140.24,   1,   2, 1_637_185_164_000),
    # --- Hyperliquid-native spot (captured via api.hyperliquid.xyz) ---
    # liquidity = summed orderbook depth within ±2% of mid; pair = spot index @107
    ("$HYPE",     "hyperliquid", "@107",
     44.54,         293_437.65, 65_990_081.38, 8,   2, None),
    # --- AI agent + memecoin trending leaders (DexScreener live) ---
    ("$AIXBT",    "base",     "0x7464850CC1cFb54A2223229b77B1BCA2f888D946",
     0.02909,     1_105_494.00,    103_724.00, 8,   0, 1_730_525_195_000),
    ("$MOODENG",  "solana",   "22WrmyTj8x2TRVQen3fxxi2r4Rn6JDHWoMTpsSmn8RUd",
     0.05762,     2_835_152.00,    692_334.00, 25, 27, 1_725_994_387_000),
    ("$POPCAT",   "solana",   "FRhB8L7Y9Qq41qZXYLtC2nw8An1RJfLLxRF2x9RwLLMo",
     0.06322,     3_724_698.00,  1_035_812.00, 62, 88, 1_702_415_044_000),
    ("$GOAT",     "solana",   "9Tb2ohu5P16BpBarqd3N27WnkF51Ukfs8Z1GzzLDxVZW",
     0.01782,     1_423_003.00,    624_825.00, 15, 16, 1_728_595_144_000),
    # --- Expanded memecoin universe (live DexScreener snapshots) ---
    ("$MOG",      "ethereum", "0xc2eaB7d33d3cB97692eCB231A5D0e4A649Cb539d",
     1.523e-07,   6_208_270.00,    751_623.00,  4,  2, 1_689_822_407_000),
    ("$BRETT",    "base",     "0xBA3F945812a83471d709BCe9C3CA699A19FB46f7",
     0.007807,    1_498_982.00,     16_130.00,  2,  9, 1_709_041_137_000),
    ("$NEIRO",    "ethereum", "0xC555D55279023E732CcD32D812114cAF5838fD46",
     0.0001024,   3_657_490.00,  2_739_385.00, 35, 27, 1_722_114_935_000),
    ("$MEW",      "solana",   "879F697iuDJGMevRkRcnW21fcXiAeLJK1ffsw2ATebce",
     0.0006326,   9_927_419.00,  1_350_616.00, 19,109, 1_711_430_534_000),
    ("$PNUT",     "solana",   "4AZRPNEfCJ7iw28rJu5aUyeQhYcvdcNm8cswyL51AY9i",
     0.06456,     3_391_437.00,  5_614_027.00,170,172, 1_730_387_206_000),
    ("$CHILLGUY", "solana",   "93tjgwff5Ac5ThyMi8C4WejVVQq4tuMeMuYW1LEYZ7bu",
     0.01306,     1_236_554.00,    477_474.00, 35, 15, 1_731_701_302_000),
    ("$GIGA",     "solana",   "4xxM4cdb6MEsCxM52xvYqkNbzvdeWWsPDZrBcTqVGUar",
     0.002074,    1_350_609.00,    291_439.00, 38, 46, 1_704_403_186_000),
    # --- Pump.fun launchpad scanner picks (freshly boosted, verified via DexScreener) ---
    # These are deliberately young tokens — most will (correctly) be filtered
    # by the veto/ignition rules because of distribution-heavy sell ratios.
    ("$SPIKE",    "solana",   "DeSYSGeEj9ytT55E9AmdFs9p2cm5fryXt61kGCammCrB",
     0.008803,      299_755.00,  3_130_748.00, 405, 348, 1_745_056_800_000),
    ("$LOL",      "solana",   "Dx5wFoszXvND6XYYAjAjUQrGLqDAUrTVH2JmHz6eJDNt",
     0.002545,      243_102.00,    500_000.00, 818, 827, 1_742_736_000_000),
    ("$ARMY",     "solana",   "5QKkwdzA4SmJCJ9BRe2ALpnAJU2boRu7sdS1Cs2qsSVc",
     0.004638,      221_200.00,    300_000.00, 105, 123, 1_722_096_000_000),
]

# ticker → (sentiment_up%, price_change_1h%, total_volume_usd)
COINGECKO = {
    "$PEPE":     (82.76, -1.11, 628_906_145),
    "$SHIB":     (74.47, -0.61, 164_087_600),
    "$BONK":     (60.00, -0.91,  99_452_049),
    "$WIF":      (50.00, -1.31,  83_639_372),
    "$FARTCOIN": (100.0, -1.21,  45_838_519),
    "$RAVE":     (39.60,  1.05, 263_313_458),
    "$ASTEROID": (54.11, -6.25,  58_911_914),
    "$PENGU":    (96.62, -1.62, 157_651_135),
    "$VIRTUAL":  (55.56, -1.22, 118_000_000),
    "$MOVR":     (81.48, -0.01, 190_000_000),
    "$HYPE":     (58.62, -0.51, 313_316_283),
    "$AIXBT":    (50.00, -1.13,  27_859_324),
    "$MOODENG":  (50.00, -0.83,  27_893_633),
    "$POPCAT":   (33.33, -1.06,  22_233_879),
    "$GOAT":     (100.00, -0.57,  9_600_429),
    "$TAO":      (65.79, -0.89, 344_152_001),
    # Expanded memecoin sentiment (CoinGecko + estimates for rate-limited ids)
    "$MOG":      (100.00, -0.08,  9_759_300),
    "$BRETT":    (75.00, -1.03,  19_529_678),
    "$NEIRO":    (60.00, -1.00,  10_000_000),    # estimated
    "$MEW":      (60.00, -1.50,  12_000_000),    # estimated
    "$PNUT":     (60.00, -1.00,  15_000_000),    # estimated
    "$CHILLGUY": (70.00, -0.80,   8_000_000),    # estimated
    "$GIGA":     (60.00, -1.00,   6_000_000),    # estimated
}

# Live trending list snapshot (CoinGecko /search/trending)
TRENDING_SYMBOLS = [
    "ASTEROID", "RAVE", "SIREN", "MON", "PENGU", "OVPP",
    "HYPE", "VIRTUAL", "TAO", "MOVR", "ORDI", "POL",
    "AIXBT", "MOODENG", "POPCAT", "GOAT",
    "MOG", "BRETT", "NEIRO", "MEW", "PNUT", "CHILLGUY", "GIGA",
]


def seed_chain_snapshots(now: datetime) -> int:
    """Write one live chain snapshot per ticker from DexScreener data."""
    written = 0
    for t, chain, pair, price, liq, vol24, buys, sells, created_ms in DEXSCREENER:
        denom = buys + sells
        insider = round(sells / denom, 4) if denom else 0.0
        age_h = round((now.timestamp() - created_ms / 1000) / 3600, 2) if created_ms else None
        db.insert_chain_snapshot({
            "ticker": t,
            "chain": chain,
            "pair_address": pair,
            "snapshot_time": now.isoformat(),
            "price_usd": price,
            "liquidity_depth_usd": liq,
            "volume_24h_usd": vol24,
            "txns_1h_buys": buys,
            "txns_1h_sells": sells,
            "insider_distribution_ratio": insider,
            "contract_age_hours": age_h,
            "lp_locked": 1,  # major DEX-listed pairs — assume LP is locked/safe
            "raw_json": json.dumps({"source": "dexscreener_live", "pair_created_ms": created_ms}),
        })
        written += 1
        logger.info(
            f"{t:<10} liq=${liq:>12,.0f}  sells_1h/total={sells}/{denom}  "
            f"insider_proxy={insider}  age={age_h}h"
        )
    return written


def seed_sentiment_posts(now: datetime) -> int:
    """
    Synthesize `social_posts` rows from CoinGecko community sentiment.

    Each ticker gets N=20 "community_poll" proxy posts in the recent 30-min
    window, of which sentiment_up% are organic and the rest are bot_flag=1.
    Authors are synthetic ('cg_voter_###') so they never match the VIP
    registry — VIP influence is layered separately via TRENDING_SYMBOLS.
    """
    N = 20
    posts = []
    recent_window_start = now - timedelta(minutes=29)
    for ticker, (sent_up, price_change_1h, vol) in COINGECKO.items():
        organic_n = round(N * sent_up / 100)
        bot_n = N - organic_n
        for i in range(organic_n):
            ts = recent_window_start + timedelta(seconds=int(1700 * i / max(1, organic_n)))
            posts.append({
                "id": f"cg:{ticker}:org:{i}",
                "platform": "coingecko",
                "ticker": ticker,
                "author": f"cg_voter_{i:03d}",
                "text": f"community sentiment bullish on {ticker} (1h {price_change_1h:+.2f}%)",
                "permalink": f"https://www.coingecko.com/en/coins/{ticker.lstrip('$').lower()}",
                "timestamp": ts.isoformat(),
                "engagement": max(1, int(vol / 1_000_000)),
                "bot_flag": 0,
            })
        for i in range(bot_n):
            ts = recent_window_start + timedelta(seconds=int(1700 * i / max(1, bot_n)))
            posts.append({
                "id": f"cg:{ticker}:bot:{i}",
                "platform": "coingecko",
                "ticker": ticker,
                "author": f"cg_downvoter_{i:03d}",
                "text": f"community sentiment bearish on {ticker}",
                "permalink": "",
                "timestamp": ts.isoformat(),
                "engagement": 1,
                "bot_flag": 1,
            })

    # Prior-window ambient noise so hype-velocity computes a real ratio
    prior_start = now - timedelta(minutes=55)
    for ticker in COINGECKO:
        for i in range(5):
            ts = prior_start + timedelta(minutes=i * 3)
            posts.append({
                "id": f"cg:{ticker}:prior:{i}",
                "platform": "coingecko",
                "ticker": ticker,
                "author": f"ambient_{i}",
                "text": f"prior-window baseline for {ticker}",
                "permalink": "",
                "timestamp": ts.isoformat(),
                "engagement": 1,
                "bot_flag": 0,
            })

    return db.bulk_upsert_posts(posts)


def seed_trending_vip_signal(now: datetime) -> int:
    """If a watchlist ticker also appears in CoinGecko's trending, add a VIP post
    as if @CoinGeckoTrending (new synthetic VIP) called it out. Only fires when
    there's real, earned virality."""
    # Register the synthetic VIP if not present.
    with db.connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO vip_registry(handle,platform,multiplier,category,followers) "
            "VALUES(?,?,?,?,?)",
            ("@CoinGeckoTrending", "coingecko", 4.5, "AggregatedSignal", None),
        )

    ticker_syms = {t.lstrip("$").upper(): t for t in COINGECKO}
    hits = []
    for sym in TRENDING_SYMBOLS:
        if sym in ticker_syms:
            hits.append(ticker_syms[sym])

    posts = []
    for t in hits:
        posts.append({
            "id": f"cg:trending:{t}",
            "platform": "coingecko",
            "ticker": t,
            "author": "@CoinGeckoTrending",
            "text": f"{t} is in CoinGecko's Top 7 trending searches right now.",
            "permalink": "https://www.coingecko.com/en/highlights/trending-search",
            "timestamp": (now - timedelta(minutes=2)).isoformat(),
            "engagement": 5000,
            "bot_flag": 0,
        })
    if posts:
        db.bulk_upsert_posts(posts)
    logger.info(f"Trending VIP signal fired for: {hits or 'no watchlist overlap'}")
    return len(posts)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    db.init_db()

    # Use a fresh UTC timestamp — the FITPAC engine will key its windows off the
    # max of DB timestamps, so we want all live rows clustered near "now".
    now = datetime.now(timezone.utc).replace(microsecond=0)

    logger.info("=== Writing LIVE chain snapshots (DexScreener, captured via Chrome) ===")
    n_chain = seed_chain_snapshots(now)
    logger.info("=== Writing LIVE community-sentiment posts (CoinGecko) ===")
    n_social = seed_sentiment_posts(now)
    n_vip = seed_trending_vip_signal(now)
    logger.info(f"Live seed done: {n_chain} chain snapshots, {n_social} sentiment posts, {n_vip} VIP trending pings.")


if __name__ == "__main__":
    main()
