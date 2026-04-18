"""
FITPAC SQLite Store
===================
Single-file database. Zero dependencies (stdlib sqlite3).
Schema is idempotent — safe to re-run init_db() on every boot.
"""

import os
import sqlite3
import json
import logging
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# DB path resolution: env var overrides default. Some FUSE mounts (e.g. some
# Windows-shared folders) don't support SQLite's file locking, so users can set
# FITPAC_DB to a native filesystem location if they hit disk I/O errors.
DB_PATH = Path(os.environ.get("FITPAC_DB", Path(__file__).parent / "fitpac.db"))
logger = logging.getLogger("FITPAC_DB")

SCHEMA = """
CREATE TABLE IF NOT EXISTS vip_registry (
    handle      TEXT PRIMARY KEY,
    platform    TEXT NOT NULL DEFAULT 'twitter',
    multiplier  REAL NOT NULL,
    category    TEXT,
    followers   INTEGER,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS tickers (
    ticker           TEXT PRIMARY KEY,
    chain            TEXT NOT NULL,          -- ethereum | solana | bsc | ...
    token_address    TEXT,                    -- for DEX data (null for non-contract coins)
    coingecko_id     TEXT,                    -- for CG price fallback
    display_name     TEXT,
    enabled          INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS social_posts (
    id            TEXT PRIMARY KEY,           -- '{platform}:{post_id}'
    platform      TEXT NOT NULL,              -- reddit | twitter | ...
    ticker        TEXT NOT NULL,
    author        TEXT NOT NULL,
    text          TEXT,
    permalink     TEXT,
    timestamp     TEXT NOT NULL,              -- ISO-8601 UTC
    engagement    INTEGER NOT NULL DEFAULT 0,
    bot_flag      INTEGER NOT NULL DEFAULT 0  -- 0 organic, 1 bot-flagged
);
CREATE INDEX IF NOT EXISTS ix_posts_ticker_ts ON social_posts(ticker, timestamp);

CREATE TABLE IF NOT EXISTS chain_snapshots (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                      TEXT NOT NULL,
    chain                       TEXT NOT NULL,
    pair_address                TEXT,
    snapshot_time               TEXT NOT NULL,
    price_usd                   REAL,
    liquidity_depth_usd         REAL,
    volume_24h_usd              REAL,
    txns_1h_buys                INTEGER,
    txns_1h_sells               INTEGER,
    insider_distribution_ratio  REAL,          -- derived (sells / (buys+sells)) or from indexer
    contract_age_hours          REAL,
    lp_locked                   INTEGER NOT NULL DEFAULT 0,
    raw_json                    TEXT            -- provider response for audit
);
CREATE INDEX IF NOT EXISTS ix_chain_ticker_ts ON chain_snapshots(ticker, snapshot_time DESC);

CREATE TABLE IF NOT EXISTS alerts_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker            TEXT NOT NULL,
    timestamp         TEXT NOT NULL,
    signal_status     TEXT NOT NULL,
    confidence_score  REAL NOT NULL,
    payload_json      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_alerts_ticker_ts ON alerts_history(ticker, timestamp DESC);
"""

# Default FITPAC watchlist — real token addresses for DexScreener lookups.
# Expanded with current CoinGecko trending picks: $RAVE, $ASTEROID, $PENGU.
DEFAULT_TICKERS = [
    # ticker, chain, token_address, coingecko_id, display_name
    ("$PEPE",     "ethereum", "0x6982508145454ce325ddbe47a25d4ec3d2311933", "pepe",           "Pepe"),
    ("$SHIB",     "ethereum", "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce", "shiba-inu",      "Shiba Inu"),
    ("$WOJAK",    "ethereum", "0x5026f006b85729a8b14553fae6af249ad16c9aab", "wojak",          "Wojak"),
    ("$BONK",     "solana",   "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "bonk",         "Bonk"),
    ("$WIF",      "solana",   "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "dogwifcoin",   "dogwifhat"),
    ("$FARTCOIN", "solana",   "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump", "fartcoin",     "Fartcoin"),
    ("$DOGE",     "dogecoin", None, "dogecoin",                                               "Dogecoin"),
    # --- trending expansion ---
    ("$RAVE",     "ethereum",   "0x17205fab260a7a6383a81452ce6315a39370db97", "ravedao",          "RaveDAO"),
    ("$ASTEROID", "ethereum",   "0xf280b16ef293d8e534e370794ef26bf312694126", "asteroid-shiba",   "Asteroid Shiba"),
    ("$PENGU",    "solana",     "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv", "pudgy-penguins",  "Pudgy Penguins"),
    # --- L1/L2 + AI agent trending picks ---
    ("$VIRTUAL",  "base",       "0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b", "virtual-protocol", "Virtuals Protocol"),
    ("$MOVR",     "moonriver",  "0x98878b06940ae243284ca214f92bb71a2b032b8a", "moonriver",        "Moonriver"),
    # --- Hyperliquid-native asset (via HyperliquidScraper adapter) ---
    ("$HYPE",     "hyperliquid", "HYPE",                                       "hyperliquid",      "Hyperliquid"),
    # --- AI agent + memecoin trending leaders ---
    ("$AIXBT",    "base",       "0x4f9fd6be4a90f2620860d680c0d4d5fb53d1a825", "aixbt",            "aixbt by Virtuals"),
    ("$MOODENG",  "solana",     "ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY", "moo-deng",        "Moo Deng"),
    ("$POPCAT",   "solana",     "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", "popcat",         "Popcat"),
    ("$GOAT",     "solana",     "CzLSujWBLFsSjncfkh59rUFqvafWcY5tzedWJSuypump", "goatseus-maximus","Goatseus Maximus"),
    # --- Non-DEX asset (CoinGecko-only fallback; hype/veto scored on sentiment + volume) ---
    ("$TAO",      "bittensor",  None,                                          "bittensor",        "Bittensor"),
    # --- Expanded memecoin universe (ETH L1 / Base L2 / Solana) ---
    ("$MOG",      "ethereum",   "0xaaeE1A9723aaDB7afA2810263653A34bA2C21C7a",  "mog-coin",         "Mog Coin"),
    ("$BRETT",    "base",       "0x532f27101965dd16442E59d40670FaF5eBB142E4",  "based-brett",      "Brett"),
    ("$NEIRO",    "ethereum",   "0x812Ba41e071C7b7fA4EBcFB62dF5F45f6fA853Ee",  "first-neiro-on-ethereum", "Neiro (ETH)"),
    ("$MEW",      "solana",     "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5", "cat-in-a-dogs-world",     "cat in a dogs world"),
    ("$PNUT",     "solana",     "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump","peanut-the-squirrel",     "Peanut the Squirrel"),
    ("$CHILLGUY", "solana",     "Df6yfrKC8kZE3KNkrHERKzAetSxbrWeniQfyJY4Jpump","chill-guy",               "Chill Guy"),
    ("$GIGA",     "solana",     "63LfDmNb3MQ8mw9MtZ2To9bEA2M71kZUUGq5tiJxcqj9","gigachad-2",              "Gigachad"),
    # --- Pump.fun launchpad scanner picks (boosted & liquid on DexScreener) ---
    ("$SPIKE",    "solana",     "DeSYSGeEj9ytT55E9AmdFs9p2cm5fryXt61kGCammCrB", None,                     "Spike"),
    ("$LOL",      "solana",     "Dx5wFoszXvND6XYYAjAjUQrGLqDAUrTVH2JmHz6eJDNt", None,                     "LOL"),
    ("$ARMY",     "solana",     "5QKkwdzA4SmJCJ9BRe2ALpnAJU2boRu7sdS1Cs2qsSVc", None,                     "Army"),
]

DEFAULT_VIPS = [
    # handle, platform, multiplier, category, followers
    ("@VitalikButerin", "twitter", 5.0, "Founder",  5_200_000),
    ("@Cobie",          "twitter", 4.0, "KOL",       850_000),
    ("@Ansem",          "twitter", 3.8, "KOL",       620_000),
    ("@CryptoWhale",    "twitter", 3.5, "Whale",     720_000),
    ("@0xMert",         "twitter", 3.2, "Analyst",   340_000),
    ("@tier10k",        "twitter", 3.1, "NewsFeed",  520_000),
    ("@hsaka",          "twitter", 3.0, "Trader",    290_000),
    ("@CryptoKaleo",    "twitter", 2.8, "Analyst",   410_000),
    ("@DegenSpartan",   "twitter", 2.5, "Trader",    180_000),
    ("@AlphaCallerX",   "twitter", 2.0, "Analyst",    95_000),
    # Reddit VIPs (moderators / well-known posters)
    ("u/CryptoKingCole", "reddit", 2.5, "Moderator",  None),
    ("u/Tricky_Troll",   "reddit", 2.3, "Moderator",  None),
]


@contextmanager
def connect(db_path: Path = DB_PATH):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH, seed: bool = True) -> None:
    """Create tables (idempotent) and optionally seed defaults if empty."""
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)

        if seed:
            cur = conn.execute("SELECT COUNT(*) AS n FROM tickers")
            if cur.fetchone()["n"] == 0:
                conn.executemany(
                    "INSERT INTO tickers(ticker,chain,token_address,coingecko_id,display_name) "
                    "VALUES(?,?,?,?,?)",
                    DEFAULT_TICKERS,
                )
                logger.info(f"Seeded {len(DEFAULT_TICKERS)} tickers.")

            cur = conn.execute("SELECT COUNT(*) AS n FROM vip_registry")
            if cur.fetchone()["n"] == 0:
                conn.executemany(
                    "INSERT INTO vip_registry(handle,platform,multiplier,category,followers) "
                    "VALUES(?,?,?,?,?)",
                    DEFAULT_VIPS,
                )
                logger.info(f"Seeded {len(DEFAULT_VIPS)} VIP handles.")


# ---- Read helpers ---------------------------------------------------------
def list_tickers(enabled_only: bool = True) -> List[sqlite3.Row]:
    with connect() as conn:
        q = "SELECT * FROM tickers"
        if enabled_only:
            q += " WHERE enabled=1"
        q += " ORDER BY ticker"
        return list(conn.execute(q))


def vip_registry() -> Dict[str, float]:
    with connect() as conn:
        return {r["handle"]: r["multiplier"] for r in conn.execute(
            "SELECT handle, multiplier FROM vip_registry"
        )}


def recent_posts(ticker: str, since: datetime, until: datetime) -> List[sqlite3.Row]:
    with connect() as conn:
        return list(conn.execute(
            "SELECT * FROM social_posts WHERE ticker=? AND timestamp BETWEEN ? AND ? "
            "ORDER BY timestamp DESC",
            (ticker, since.isoformat(), until.isoformat()),
        ))


def latest_chain_snapshot(ticker: str) -> Optional[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM chain_snapshots WHERE ticker=? ORDER BY snapshot_time DESC LIMIT 1",
            (ticker,),
        ).fetchone()


# ---- Write helpers --------------------------------------------------------
def upsert_post(post: Dict) -> None:
    """post keys: id, platform, ticker, author, text, permalink, timestamp, engagement, bot_flag"""
    with connect() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO social_posts
               (id, platform, ticker, author, text, permalink, timestamp, engagement, bot_flag)
               VALUES (:id, :platform, :ticker, :author, :text, :permalink, :timestamp, :engagement, :bot_flag)""",
            post,
        )


def bulk_upsert_posts(posts: List[Dict]) -> int:
    if not posts:
        return 0
    with connect() as conn:
        conn.executemany(
            """INSERT OR REPLACE INTO social_posts
               (id, platform, ticker, author, text, permalink, timestamp, engagement, bot_flag)
               VALUES (:id, :platform, :ticker, :author, :text, :permalink, :timestamp, :engagement, :bot_flag)""",
            posts,
        )
    return len(posts)


def insert_chain_snapshot(snap: Dict) -> int:
    with connect() as conn:
        cur = conn.execute(
            """INSERT INTO chain_snapshots
               (ticker, chain, pair_address, snapshot_time, price_usd, liquidity_depth_usd,
                volume_24h_usd, txns_1h_buys, txns_1h_sells, insider_distribution_ratio,
                contract_age_hours, lp_locked, raw_json)
               VALUES (:ticker, :chain, :pair_address, :snapshot_time, :price_usd, :liquidity_depth_usd,
                       :volume_24h_usd, :txns_1h_buys, :txns_1h_sells, :insider_distribution_ratio,
                       :contract_age_hours, :lp_locked, :raw_json)""",
            snap,
        )
        return cur.lastrowid


def append_alert(payload: Dict) -> None:
    with connect() as conn:
        conn.execute(
            """INSERT INTO alerts_history(ticker, timestamp, signal_status, confidence_score, payload_json)
               VALUES(?,?,?,?,?)""",
            (
                payload["ticker"],
                payload["timestamp"],
                payload["signal_status"],
                payload["confidence_score"],
                json.dumps(payload),
            ),
        )


def prune_old_posts(older_than: datetime) -> int:
    with connect() as conn:
        cur = conn.execute(
            "DELETE FROM social_posts WHERE timestamp < ?", (older_than.isoformat(),)
        )
        return cur.rowcount


# ---- CSV → SQLite seed (one-shot) ----------------------------------------
def seed_from_csv(csv_dir: Path) -> None:
    """Bulk-import the legacy CSV fixtures so the DB is populated immediately."""
    import csv

    social = csv_dir / "social_stream.csv"
    chain = csv_dir / "chain_data.csv"

    posts = []
    if social.exists():
        with open(social, newline="", encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                posts.append({
                    "id": f"seed:{i}",
                    "platform": "seed",
                    "ticker": row["ticker"],
                    "author": row["author"],
                    "text": row["text"],
                    "permalink": "",
                    "timestamp": row["timestamp"],
                    "engagement": int(row["engagement"]),
                    "bot_flag": int(row["bot_flag"]),
                })
        bulk_upsert_posts(posts)
        logger.info(f"Seeded {len(posts)} posts from {social.name}.")

    if chain.exists():
        with open(chain, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                insert_chain_snapshot({
                    "ticker": row["ticker"],
                    "chain": "unknown",
                    "pair_address": None,
                    "snapshot_time": row["snapshot_time"],
                    "price_usd": None,
                    "liquidity_depth_usd": float(row["liquidity_depth_usd"]),
                    "volume_24h_usd": None,
                    "txns_1h_buys": None,
                    "txns_1h_sells": None,
                    "insider_distribution_ratio": float(row["insider_distribution_ratio"]),
                    "contract_age_hours": float(row["contract_age_hours"]),
                    "lp_locked": 1 if row["lp_locked"].lower() == "true" else 0,
                    "raw_json": json.dumps(row),
                })
        logger.info("Seeded chain snapshots from chain_data.csv.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    init_db()
    # If you still have the legacy CSVs, seed from them once for an instant-working DB.
    csv_dir = Path(__file__).parent / "data"
    if csv_dir.exists():
        seed_from_csv(csv_dir)
    print(f"DB ready at {DB_PATH}")
