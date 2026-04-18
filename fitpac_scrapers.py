"""
FITPAC Scrapers — real live data sources, no API keys required
==============================================================
  - DexScreenerScraper : market/liquidity data for EVM + Solana tokens
                         endpoint: https://api.dexscreener.com/latest/dex/tokens/{addr}
  - CoinGeckoScraper   : price/volume fallback for non-contract coins (DOGE, etc)
                         endpoint: https://api.coingecko.com/api/v3/coins/{id}
  - RedditScraper      : organic social mentions from crypto subreddits
                         endpoint: https://www.reddit.com/r/{sub}/new.json
  - BotFilter          : heuristic flagger (short + low-engagement + duplicate text)

All scrapers use stdlib urllib — zero pip installs required.
"""

import json
import logging
import re
import time
import urllib.request
import urllib.error
import urllib.parse
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import fitpac_db as db

logger = logging.getLogger("FITPAC_Scrapers")

USER_AGENT = "FITPAC-Alert-Engine/1.0 (+https://fitpac.local/bot)"
HTTP_TIMEOUT = 15

# Default subreddits to sweep for ticker mentions
DEFAULT_SUBREDDITS = [
    "CryptoCurrency",
    "CryptoMoonShots",
    "SatoshiStreetBets",
    "solana",
    "ethtrader",
    "memecoins",
]

TICKER_RE = re.compile(r"\$([A-Z][A-Z0-9]{1,9})\b")


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
def _http_get(url: str, extra_headers: Optional[Dict[str, str]] = None) -> dict:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8"))


# ---------------------------------------------------------------------------
# DexScreener — liquidity, volume, buy/sell ratio, pair age
# ---------------------------------------------------------------------------
class DexScreenerScraper:
    BASE = "https://api.dexscreener.com/latest/dex/tokens/"

    def fetch(self, token_address: str) -> List[dict]:
        """Returns all trading pairs for a token address."""
        if not token_address:
            return []
        try:
            data = _http_get(self.BASE + token_address)
            return data.get("pairs") or []
        except urllib.error.URLError as e:
            logger.warning(f"DexScreener fetch failed for {token_address}: {e}")
            return []

    @staticmethod
    def _best_pair(pairs: List[dict]) -> Optional[dict]:
        """Pick the pair with the deepest USD liquidity."""
        best = None
        best_liq = -1
        for p in pairs:
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if liq > best_liq:
                best = p
                best_liq = liq
        return best

    def snapshot(self, ticker: str, token_address: str, chain: str) -> Optional[Dict]:
        """Build a chain_snapshots row from DexScreener data."""
        pairs = self.fetch(token_address)
        pair = self._best_pair(pairs)
        if not pair:
            logger.info(f"{ticker} | no DexScreener pairs returned.")
            return None

        now = datetime.now(timezone.utc).replace(microsecond=0)
        created_ms = pair.get("pairCreatedAt")
        age_hours = (now.timestamp() - created_ms / 1000) / 3600 if created_ms else None

        liq = (pair.get("liquidity") or {}).get("usd") or 0.0
        vol24 = (pair.get("volume") or {}).get("h24") or 0.0
        txns1h = (pair.get("txns") or {}).get("h1") or {}
        buys_1h = int(txns1h.get("buys") or 0)
        sells_1h = int(txns1h.get("sells") or 0)

        # Insider distribution proxy: sell share of recent txn count.
        # Real "holder concentration" needs an indexer (Etherscan/Solscan).
        # This gives us a distribution-pressure signal from on-chain activity.
        denom = buys_1h + sells_1h
        insider_ratio = (sells_1h / denom) if denom else 0.0

        return {
            "ticker": ticker,
            "chain": chain,
            "pair_address": pair.get("pairAddress"),
            "snapshot_time": now.isoformat(),
            "price_usd": float(pair.get("priceUsd") or 0.0),
            "liquidity_depth_usd": float(liq),
            "volume_24h_usd": float(vol24),
            "txns_1h_buys": buys_1h,
            "txns_1h_sells": sells_1h,
            "insider_distribution_ratio": round(insider_ratio, 4),
            "contract_age_hours": round(age_hours, 2) if age_hours is not None else None,
            "lp_locked": 0,  # DexScreener doesn't expose this; set via Etherscan/Solscan scraper
            "raw_json": json.dumps(pair)[:8000],
        }


# ---------------------------------------------------------------------------
# CoinGecko — fallback for non-contract coins (DOGE, etc)
# ---------------------------------------------------------------------------
class CoinGeckoScraper:
    BASE = "https://api.coingecko.com/api/v3/coins/"

    def snapshot(self, ticker: str, coingecko_id: str, chain: str) -> Optional[Dict]:
        if not coingecko_id:
            return None
        try:
            url = (
                self.BASE
                + urllib.parse.quote(coingecko_id)
                + "?localization=false&tickers=false&community_data=false&developer_data=false"
            )
            data = _http_get(url)
        except urllib.error.URLError as e:
            logger.warning(f"CoinGecko fetch failed for {coingecko_id}: {e}")
            return None

        md = data.get("market_data") or {}
        now = datetime.now(timezone.utc).replace(microsecond=0)
        return {
            "ticker": ticker,
            "chain": chain,
            "pair_address": None,
            "snapshot_time": now.isoformat(),
            "price_usd": (md.get("current_price") or {}).get("usd"),
            "liquidity_depth_usd": (md.get("total_volume") or {}).get("usd") or 0.0,
            "volume_24h_usd": (md.get("total_volume") or {}).get("usd") or 0.0,
            "txns_1h_buys": None,
            "txns_1h_sells": None,
            "insider_distribution_ratio": 0.0,  # unknown without on-chain data
            "contract_age_hours": None,
            "lp_locked": 1,  # major coins — assume safe
            "raw_json": json.dumps({"source": "coingecko", "id": coingecko_id})[:8000],
        }


# ---------------------------------------------------------------------------
# Hyperliquid — orderbook-native L1 ($HYPE, $PURR, spot list)
#   POST https://api.hyperliquid.xyz/info
#     {"type": "spotMetaAndAssetCtxs"}  -> [{universe:[...]}, [ctx, ctx, ...]]
#   Each ctx has: coin, midPx, prevDayPx, dayNtlVlm, markPx, dayBaseVlm,
#                 openInterest, oraclePx, funding, premium
# ---------------------------------------------------------------------------
class HyperliquidScraper:
    BASE = "https://api.hyperliquid.xyz/info"
    ORDERBOOK = {"type": "l2Book"}  # separate call per coin

    def _post(self, body: Dict) -> dict:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self.BASE,
            data=data,
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8"))

    def _spot_meta_and_ctxs(self) -> Tuple[List[dict], List[dict]]:
        """Returns (universe, ctxs) where universe[i] corresponds to ctxs[i]."""
        try:
            data = self._post({"type": "spotMetaAndAssetCtxs"})
        except urllib.error.URLError as e:
            logger.warning(f"Hyperliquid spotMetaAndAssetCtxs failed: {e}")
            return [], []
        if not isinstance(data, list) or len(data) < 2:
            return [], []
        universe = (data[0] or {}).get("universe") or []
        ctxs = data[1] or []
        return universe, ctxs

    def _l2_book(self, coin_name: str) -> Optional[dict]:
        """Fetch the orderbook — we use summed bid/ask depth as a liquidity proxy."""
        try:
            return self._post({"type": "l2Book", "coin": coin_name})
        except urllib.error.URLError as e:
            logger.warning(f"Hyperliquid l2Book {coin_name} failed: {e}")
            return None

    def _recent_trades(self, coin_name: str) -> List[dict]:
        """Fetch recent trades so we can derive buy/sell counts (insider proxy)."""
        try:
            res = self._post({"type": "recentTrades", "coin": coin_name})
            return res if isinstance(res, list) else []
        except urllib.error.URLError as e:
            logger.warning(f"Hyperliquid recentTrades {coin_name} failed: {e}")
            return []

    @staticmethod
    def _sum_book_usd(book: dict, mid_price: float) -> float:
        """Sum ±2% depth in USD notionals across both sides of the book."""
        if not book:
            return 0.0
        levels = book.get("levels") or []
        if len(levels) < 2:
            return 0.0
        bids, asks = levels[0], levels[1]
        total = 0.0
        for side in (bids, asks):
            for lvl in side or []:
                try:
                    px = float(lvl.get("px"))
                    sz = float(lvl.get("sz"))
                    if mid_price > 0 and abs(px - mid_price) / mid_price <= 0.02:
                        total += px * sz
                except (TypeError, ValueError):
                    continue
        return total

    def snapshot(self, ticker: str, coin_name: str) -> Optional[Dict]:
        """
        Build a chain_snapshots row from Hyperliquid spot data.
        `coin_name` is the spot asset name (e.g. "HYPE", "PURR") — NOT the perp name.
        """
        universe, ctxs = self._spot_meta_and_ctxs()
        if not universe:
            return None

        ctx = None
        for u, c in zip(universe, ctxs):
            name = (u.get("name") or "").upper()
            if name == coin_name.upper():
                ctx = c
                break
        if not ctx:
            logger.info(f"{ticker} | Hyperliquid: coin {coin_name} not in spot universe.")
            return None

        try:
            price = float(ctx.get("markPx") or ctx.get("midPx") or 0.0)
            vol24_native = float(ctx.get("dayNtlVlm") or 0.0)  # already USD-notional
        except (TypeError, ValueError):
            price, vol24_native = 0.0, 0.0

        # Orderbook depth ± 2% of mid (proxy for liquidity_depth_usd)
        book = self._l2_book(coin_name)
        liq = self._sum_book_usd(book, price)

        # Recent trades → derive buys/sells counts in the last hour
        trades = self._recent_trades(coin_name)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        cutoff = now_ms - 3600_000
        buys = sells = 0
        for t in trades:
            try:
                ts = int(t.get("time") or 0)
                if ts < cutoff:
                    continue
                if (t.get("side") or "").upper() == "B":
                    buys += 1
                else:
                    sells += 1
            except (TypeError, ValueError):
                continue
        denom = buys + sells
        insider = (sells / denom) if denom else 0.0

        now = datetime.now(timezone.utc).replace(microsecond=0)
        return {
            "ticker": ticker,
            "chain": "hyperliquid",
            "pair_address": coin_name,  # spot asset name substitutes for pair address
            "snapshot_time": now.isoformat(),
            "price_usd": price,
            "liquidity_depth_usd": round(liq, 2),
            "volume_24h_usd": round(vol24_native, 2),
            "txns_1h_buys": buys,
            "txns_1h_sells": sells,
            "insider_distribution_ratio": round(insider, 4),
            "contract_age_hours": None,  # native asset, no EVM contract age
            "lp_locked": 1,  # orderbook-native, no LP concept
            "raw_json": json.dumps({"source": "hyperliquid", "coin": coin_name, "ctx": ctx})[:8000],
        }


# ---------------------------------------------------------------------------
# Pump.fun / Solana launchpad scanner
#   Surfaces trending, newly-graduated Solana tokens (pump.fun suffix on address)
#   with minimum liquidity + active transaction flow. Returns candidate ticker
#   rows ready to be registered into the watchlist.
#
#   Data sources:
#     GET https://api.dexscreener.com/token-boosts/top/v1
#     GET https://api.dexscreener.com/token-boosts/latest/v1
#     GET https://api.dexscreener.com/latest/dex/tokens/{address}
# ---------------------------------------------------------------------------
class PumpFunScanner:
    BOOSTS_TOP = "https://api.dexscreener.com/token-boosts/top/v1"
    BOOSTS_LATEST = "https://api.dexscreener.com/token-boosts/latest/v1"
    DEX_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/"

    def __init__(
        self,
        min_liquidity_usd: float = 200_000,
        min_txns_1h: int = 15,
        max_age_hours: Optional[float] = None,  # None = no age cap
        pump_fun_only: bool = False,
    ):
        self.min_liquidity = min_liquidity_usd
        self.min_txns = min_txns_1h
        self.max_age = max_age_hours
        self.pump_fun_only = pump_fun_only

    def _boost_list(self) -> List[dict]:
        out = []
        for url in (self.BOOSTS_TOP, self.BOOSTS_LATEST):
            try:
                data = _http_get(url)
                if isinstance(data, list):
                    out.extend(data)
            except urllib.error.URLError as e:
                logger.warning(f"PumpFunScanner boost fetch failed: {e}")
        return out

    def scan(self) -> List[Dict]:
        """Returns a ranked list of candidate token dicts (strongest buy
        pressure first). Each dict has: ticker, chain, token_address,
        coingecko_id (None), display_name, plus the raw snapshot for seeding.
        """
        boosts = self._boost_list()
        sol = [b for b in boosts if b.get("chainId") == "solana"]
        seen = set()
        candidates: List[Dict] = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for b in sol:
            addr = b.get("tokenAddress")
            if not addr or addr in seen:
                continue
            seen.add(addr)

            is_pump = addr.lower().endswith("pump")
            if self.pump_fun_only and not is_pump:
                continue

            try:
                resp = _http_get(self.DEX_TOKEN + addr)
            except urllib.error.URLError:
                continue
            pairs = sorted(
                resp.get("pairs") or [],
                key=lambda p: (p.get("liquidity") or {}).get("usd") or 0,
                reverse=True,
            )
            if not pairs:
                continue
            p = pairs[0]

            liq = (p.get("liquidity") or {}).get("usd") or 0
            txns1h = (p.get("txns") or {}).get("h1") or {}
            buys = int(txns1h.get("buys") or 0)
            sells = int(txns1h.get("sells") or 0)
            denom = buys + sells
            created_ms = p.get("pairCreatedAt")
            age_hours = (now_ms - created_ms) / 3600_000 if created_ms else None

            # Liquidity + activity gates
            if liq < self.min_liquidity:
                continue
            if denom < self.min_txns:
                continue
            if self.max_age is not None and age_hours is not None and age_hours > self.max_age:
                continue

            insider = sells / denom if denom else 0.0
            base = p.get("baseToken") or {}
            sym = (base.get("symbol") or "").upper()
            name = (base.get("name") or sym).strip()

            candidates.append({
                "ticker": f"${sym}" if sym else f"${addr[:6]}",
                "chain": "solana",
                "token_address": addr,
                "coingecko_id": None,
                "display_name": name,
                "is_pump_fun": is_pump,
                # Snapshot — ready for db.insert_chain_snapshot if desired
                "snapshot": {
                    "price_usd": float(p.get("priceUsd") or 0.0),
                    "liquidity_depth_usd": float(liq),
                    "volume_24h_usd": float((p.get("volume") or {}).get("h24") or 0.0),
                    "txns_1h_buys": buys,
                    "txns_1h_sells": sells,
                    "insider_distribution_ratio": round(insider, 4),
                    "contract_age_hours": round(age_hours, 2) if age_hours is not None else None,
                    "pair_address": p.get("pairAddress"),
                    "pair_created_ms": created_ms,
                    "raw_json": json.dumps(p)[:4000],
                },
            })

        # Rank by buy pressure (lowest insider first), tiebreak by liquidity desc
        candidates.sort(
            key=lambda c: (c["snapshot"]["insider_distribution_ratio"],
                           -c["snapshot"]["liquidity_depth_usd"])
        )
        return candidates

    def register(self, blocklist_tickers: Optional[List[str]] = None, limit: int = 5) -> int:
        """Scan, dedupe against existing watchlist + blocklist, and insert new
        tickers into the `tickers` table. Also writes a chain_snapshots row
        for each one so FITPAC has immediate ground-truth data. Returns count added.
        """
        blockset = {t.upper() for t in (blocklist_tickers or [])}
        existing = {r["ticker"].upper() for r in db.list_tickers(enabled_only=False)}
        cands = self.scan()
        written = 0
        for c in cands:
            if written >= limit:
                break
            if c["ticker"].upper() in existing or c["ticker"].upper() in blockset:
                logger.info(f"PumpFunScanner skipping {c['ticker']} (already tracked / blocklisted)")
                continue
            with db.connect() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO tickers(ticker,chain,token_address,coingecko_id,display_name) "
                    "VALUES(?,?,?,?,?)",
                    (c["ticker"], c["chain"], c["token_address"], None, c["display_name"]),
                )
            # Write initial snapshot so FITPAC has immediate chain data
            s = c["snapshot"]
            now = datetime.now(timezone.utc).replace(microsecond=0)
            db.insert_chain_snapshot({
                "ticker": c["ticker"],
                "chain": "solana",
                "pair_address": s["pair_address"],
                "snapshot_time": now.isoformat(),
                "price_usd": s["price_usd"],
                "liquidity_depth_usd": s["liquidity_depth_usd"],
                "volume_24h_usd": s["volume_24h_usd"],
                "txns_1h_buys": s["txns_1h_buys"],
                "txns_1h_sells": s["txns_1h_sells"],
                "insider_distribution_ratio": s["insider_distribution_ratio"],
                "contract_age_hours": s["contract_age_hours"],
                "lp_locked": 1,  # pump.fun graduates use locked-burned LP by default
                "raw_json": s["raw_json"],
            })
            logger.info(
                f"PumpFunScanner registered {c['ticker']} | age={s['contract_age_hours']}h | "
                f"liq=${s['liquidity_depth_usd']:,.0f} | insider={s['insider_distribution_ratio']}"
            )
            written += 1
        return written


# ---------------------------------------------------------------------------
# Reddit — organic social mentions
# ---------------------------------------------------------------------------
class RedditScraper:
    BASE = "https://www.reddit.com/r/{sub}/new.json?limit={limit}"

    def __init__(self, subreddits: List[str] = None, limit_per_sub: int = 50):
        self.subreddits = subreddits or DEFAULT_SUBREDDITS
        self.limit = limit_per_sub

    def _fetch_sub(self, sub: str) -> List[dict]:
        url = self.BASE.format(sub=sub, limit=self.limit)
        try:
            data = _http_get(url)
        except urllib.error.URLError as e:
            logger.warning(f"Reddit /r/{sub} fetch failed: {e}")
            return []
        children = (data.get("data") or {}).get("children") or []
        return [c["data"] for c in children if c.get("kind") == "t3"]

    def scrape(self, watchlist_tickers: List[str]) -> List[Dict]:
        """Returns a list of post dicts ready for db.bulk_upsert_posts()."""
        watchset = {t.upper() for t in watchlist_tickers}
        out: List[Dict] = []
        for sub in self.subreddits:
            posts = self._fetch_sub(sub)
            logger.info(f"Reddit /r/{sub}: pulled {len(posts)} posts")
            for p in posts:
                combined = f"{p.get('title','')} {p.get('selftext','')}".strip()
                tickers_found = {"$" + m for m in TICKER_RE.findall(combined)}
                # Always prefix with $ for matching
                hits = sorted(tickers_found & watchset)
                if not hits:
                    continue
                ts = datetime.fromtimestamp(
                    float(p.get("created_utc") or 0), tz=timezone.utc
                ).replace(microsecond=0).isoformat()
                engagement = int(p.get("score") or 0) + int(p.get("num_comments") or 0)
                # One post can mention multiple tickers → emit one row per ticker
                for t in hits:
                    out.append({
                        "id": f"reddit:{p.get('id')}:{t}",
                        "platform": "reddit",
                        "ticker": t,
                        "author": "u/" + (p.get("author") or "deleted"),
                        "text": combined[:500],
                        "permalink": "https://reddit.com" + (p.get("permalink") or ""),
                        "timestamp": ts,
                        "engagement": engagement,
                        "bot_flag": 0,
                    })
            time.sleep(1)  # polite throttle
        return out


# ---------------------------------------------------------------------------
# Bot heuristics — runs AFTER posts are staged so we can compare across authors
# ---------------------------------------------------------------------------
class BotFilter:
    """Flags posts that look astroturfed. Conservative by design."""

    BOT_KEYWORDS = ("100x", "1000x", "moon mission", "join telegram", "dont miss",
                    "next gem", "pre-sale", "presale", "stealth launch")

    def score(self, posts: List[Dict]) -> List[Dict]:
        if not posts:
            return posts
        author_counts = Counter(p["author"] for p in posts)
        text_sigs = Counter(re.sub(r"\s+", " ", (p["text"] or "").lower())[:80] for p in posts)

        flagged = 0
        for p in posts:
            reasons = []
            text = (p["text"] or "").lower()
            if any(k in text for k in self.BOT_KEYWORDS):
                reasons.append("spam_keyword")
            if p["engagement"] <= 2 and len(text) < 80:
                reasons.append("low_engagement_short")
            if author_counts[p["author"]] >= 4:
                reasons.append("author_burst")
            if text_sigs[re.sub(r"\s+", " ", text)[:80]] >= 3:
                reasons.append("duplicate_text")

            if len(reasons) >= 2:
                p["bot_flag"] = 1
                flagged += 1
        logger.info(f"BotFilter: flagged {flagged}/{len(posts)} posts as likely bots.")
        return posts


# ---------------------------------------------------------------------------
# High-level runners
# ---------------------------------------------------------------------------
def scrape_chain_all() -> int:
    """Fetch a fresh chain snapshot for every enabled ticker. Returns # written."""
    dex = DexScreenerScraper()
    cg = CoinGeckoScraper()
    hl = HyperliquidScraper()
    written = 0
    for row in db.list_tickers():
        snap = None
        # Hyperliquid-native spot assets: coin name lives in token_address slot.
        if row["chain"] == "hyperliquid" and row["token_address"]:
            snap = hl.snapshot(row["ticker"], row["token_address"])
        elif row["token_address"]:
            snap = dex.snapshot(row["ticker"], row["token_address"], row["chain"])
        if snap is None and row["coingecko_id"]:
            snap = cg.snapshot(row["ticker"], row["coingecko_id"], row["chain"])
        if snap:
            db.insert_chain_snapshot(snap)
            written += 1
            logger.info(
                f"{row['ticker']:<10} | liq=${snap['liquidity_depth_usd']:,.0f} "
                f"| sells_1h={snap['txns_1h_sells']} | insider_proxy={snap['insider_distribution_ratio']}"
            )
        time.sleep(0.3)  # polite throttle for DexScreener
    return written


def scrape_social_all() -> int:
    """Sweep all configured subreddits for ticker mentions. Returns # posts written."""
    watchlist = [t["ticker"] for t in db.list_tickers()]
    reddit = RedditScraper()
    raw_posts = reddit.scrape(watchlist)
    filtered = BotFilter().score(raw_posts)
    return db.bulk_upsert_posts(filtered)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    db.init_db()
    logger.info("=== Scraping chain data ===")
    n_chain = scrape_chain_all()
    logger.info("=== Scraping social data ===")
    n_social = scrape_social_all()
    logger.info(f"Done. Wrote {n_chain} chain snapshots, {n_social} social posts.")
