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
import os
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

# Reddit's public API requires a descriptive User-Agent in the format
#   <platform>:<app ID>:<version> (by /u/<reddit-username>)
# Generic UAs and anything containing "bot" or ".local" are rate-limited
# aggressively — especially on cloud/shared IPs (Render, Fly, Heroku).
# Override at deploy time via env var FITPAC_USER_AGENT if you have a real
# Reddit handle you'd rather attribute.
USER_AGENT = os.environ.get(
    "FITPAC_USER_AGENT",
    "linux:fitpac-alert-engine:v1.0 (by /u/jamesnobleroots-oss)",
)
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
    """GET url, parse JSON. Re-raises URLError/HTTPError with status for log visibility."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # Surface the HTTP status so the Render log tab shows 403/429 clearly.
        logger.warning(f"HTTP {e.code} on {url}: {e.reason}")
        raise urllib.error.URLError(f"HTTP {e.code} {e.reason}") from e


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

    @staticmethod
    def pair_to_snapshot(ticker: str, chain: str, pair: dict) -> Dict:
        """Turn a DexScreener pair dict into a chain_snapshots row.

        Split out so both the direct address lookup path and the
        DexScreenerResolver (search-by-symbol) path can reuse the same
        pair → snapshot conversion logic.
        """
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

    def snapshot(self, ticker: str, token_address: str, chain: str) -> Optional[Dict]:
        """Build a chain_snapshots row from DexScreener data."""
        pairs = self.fetch(token_address)
        pair = self._best_pair(pairs)
        if not pair:
            logger.info(f"{ticker} | no DexScreener pairs returned.")
            return None
        return self.pair_to_snapshot(ticker, chain, pair)


# ---------------------------------------------------------------------------
# DexScreener Resolver — auto-discover token_address by ticker symbol
# ---------------------------------------------------------------------------
# Why this exists:
#   On Render/Fly/Heroku egress IPs, the /latest/dex/tokens/{addr} endpoint
#   sometimes returns empty pairs (likely intermittent rate limiting), which
#   causes the primary scrape to fail and the backend to mark the ticker as
#   CHAIN UNVERIFIED — capping confidence at ~0.40 and suppressing ignition.
#
# The /search?q= endpoint is a different code path that (empirically) behaves
# more forgivingly from cloud IPs. It also returns the full pair dict inline,
# so even if /tokens/{addr} keeps failing we can still emit a snapshot.
#
# Resolved addresses are persisted back to the tickers table so the search
# cost is paid at most once per ticker per container lifetime.
# ---------------------------------------------------------------------------
class DexScreenerResolver:
    SEARCH = "https://api.dexscreener.com/latest/dex/search?q={query}"

    # Pools below this liquidity floor are almost always scammer squats on a
    # popular symbol. We skip them to avoid polluting the snapshot with noise.
    MIN_LIQUIDITY_USD = 50_000

    # DexScreener chainId → our internal chain name. Most are identity mappings;
    # listed explicitly so the set of allowed chains is reviewable at a glance.
    CHAIN_MAP = {
        "ethereum":  "ethereum",
        "solana":    "solana",
        "base":      "base",
        "bsc":       "bsc",
        "polygon":   "polygon",
        "arbitrum":  "arbitrum",
        "optimism":  "optimism",
        "moonriver": "moonriver",
        "blast":     "blast",
        "avalanche": "avalanche",
        "fantom":    "fantom",
        "sui":       "sui",
        "ton":       "ton",
    }

    def resolve(self, ticker: str, hint_chain: Optional[str] = None) -> Optional[Dict]:
        """Search DexScreener for `ticker` and return the best matching pool.

        Returns: dict(chain, token_address, pair) or None.
        Best-effort — returns None on HTTP error, no matches, or low liquidity.
        """
        symbol = ticker.lstrip("$").upper()
        if not symbol:
            return None

        try:
            data = _http_get(self.SEARCH.format(query=urllib.parse.quote(symbol)))
        except urllib.error.URLError as e:
            logger.warning(f"DexScreener search failed for {ticker}: {e}")
            return None

        pairs = data.get("pairs") or []
        # Symbol must match the baseToken exactly (case-insensitive), AND the
        # pool must clear the minimum liquidity floor.
        candidates = [
            p for p in pairs
            if (p.get("baseToken") or {}).get("symbol", "").upper() == symbol
            and ((p.get("liquidity") or {}).get("usd") or 0) >= self.MIN_LIQUIDITY_USD
        ]
        if not candidates:
            logger.info(
                f"{ticker} | DexScreener search returned no pairs ≥ "
                f"${self.MIN_LIQUIDITY_USD:,.0f} liquidity."
            )
            return None

        # Prefer the hinted chain if we have one (from the seeded DB row)
        if hint_chain:
            preferred_id = self.CHAIN_MAP.get(hint_chain, hint_chain)
            on_hint = [p for p in candidates if p.get("chainId") == preferred_id]
            if on_hint:
                candidates = on_hint

        # Deepest liquidity wins
        best = max(candidates, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
        chain_id = best.get("chainId") or "unknown"
        chain_name = self.CHAIN_MAP.get(chain_id, chain_id)
        base = best.get("baseToken") or {}
        addr = base.get("address")
        if not addr:
            return None

        logger.info(
            f"{ticker} | resolved via /search → chain={chain_name} "
            f"addr={addr[:10]}… liq=${(best.get('liquidity') or {}).get('usd', 0):,.0f}"
        )
        return {"chain": chain_name, "token_address": addr, "pair": best}


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
# CoinGecko Trending — primary social signal source (no auth, no cloud blocks)
# ---------------------------------------------------------------------------
class CoinGeckoTrendingScraper:
    """Pulls the top 15 trending coins and writes one synthetic 'post' per
    watchlist hit, with count proportional to CoinGecko's internal trending
    score. Low score = hot → more synthetic posts. This drives
    SocialTransformerModule's recent-vs-prior window comparison, so a coin
    entering the trending list produces a real hype_velocity bump.

    Also auto-ingests new trending coins into the watchlist so FITPAC
    self-discovers emerging tickers without manual curation. See `_autoingest`.
    """

    BASE = "https://api.coingecko.com/api/v3/search/trending"
    AUTHOR = "u/coingecko_trending"

    # Hard cap on total watchlist size. Each ticker adds ~0.3s of DexScreener
    # throttle + a resolver HTTP call, so at 60 the full chain scrape stays
    # under ~30s — well within Render's request budget.
    MAX_WATCHLIST_SIZE = 60

    # Screen out scammer clones: CoinGecko assigns market_cap_rank only to
    # coins with a verified exchange listing. Anything beyond this rank (or
    # missing a rank entirely) is skipped.
    MAX_MARKET_CAP_RANK = 10_000

    def _autoingest(self, coins: List[dict]) -> int:
        """Add any trending coin not already on the watchlist, up to cap.

        Inserted rows have chain='unknown' and token_address=NULL — the
        DexScreenerResolver fills both in on the next chain scrape cycle.
        """
        try:
            current_count = db.count_tickers()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Auto-ingest: count_tickers failed — skipping: {e}")
            return 0

        added = 0
        for coin in coins:
            if current_count + added >= self.MAX_WATCHLIST_SIZE:
                logger.info(
                    f"Auto-ingest cap reached ({self.MAX_WATCHLIST_SIZE} tickers); "
                    "skipping remaining trending coins."
                )
                break
            item = coin.get("item") or {}
            symbol = (item.get("symbol") or "").upper()
            if not symbol:
                continue
            ticker = f"${symbol}"
            rank = item.get("market_cap_rank")
            if rank is None or rank > self.MAX_MARKET_CAP_RANK:
                continue
            try:
                inserted = db.insert_ticker_if_new(
                    ticker=ticker,
                    chain="unknown",  # resolver will overwrite on first chain scrape
                    coingecko_id=item.get("id"),
                    display_name=item.get("name") or symbol,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Auto-ingest: insert {ticker} failed: {e}")
                continue
            if inserted:
                logger.info(
                    f"Auto-ingested {ticker} (rank={rank}, cg_id={item.get('id')}) "
                    "— resolver will map chain address next cycle."
                )
                added += 1
        return added

    def scrape(self, watchlist_tickers: List[str]) -> List[Dict]:
        try:
            data = _http_get(self.BASE)
        except urllib.error.URLError as e:
            logger.warning(f"CoinGecko trending fetch failed: {e}")
            return []

        coins = data.get("coins") or []

        # Auto-grow the watchlist from trending before we build the hit set,
        # so newly-added tickers get synthetic posts emitted THIS cycle.
        if self._autoingest(coins):
            watchlist_tickers = [r["ticker"] for r in db.list_tickers()]

        watchset = {t.upper() for t in watchlist_tickers}
        out: List[Dict] = []
        now = datetime.now(timezone.utc).replace(microsecond=0)

        for coin in coins:
            item = coin.get("item") or {}
            symbol = (item.get("symbol") or "").upper()
            ticker = f"${symbol}"
            if ticker not in watchset:
                continue

            score = int(item.get("score") or 0)              # 0=hottest
            rank = item.get("market_cap_rank") or 9999
            name = item.get("name") or symbol
            slug = item.get("slug") or item.get("id") or ""
            data_blob = item.get("data") or {}
            pct_24h = 0.0
            price_change = data_blob.get("price_change_percentage_24h") or {}
            if isinstance(price_change, dict):
                pct_24h = float(price_change.get("usd") or 0.0)

            # Synthetic post count proportional to heat. Score 0 → 5 posts,
            # score ≥ 4 → 1 post. Drives hype_velocity ratio when a coin
            # moves up/down the trending list between cycles.
            heat = max(1, 5 - score)

            for i in range(heat):
                out.append({
                    "id": f"coingecko_trending:{item.get('id')}:{now.isoformat()}:{i}",
                    "platform": "coingecko_trending",
                    "ticker": ticker,
                    "author": self.AUTHOR,
                    "text": (
                        f"{name} ({ticker}) trending on CoinGecko. "
                        f"Score={score}, rank={rank}, 24h={pct_24h:+.2f}%."
                    )[:500],
                    "permalink": f"https://www.coingecko.com/en/coins/{slug}",
                    "timestamp": now.isoformat(),
                    "engagement": heat * 100 + max(0, int(pct_24h)),
                    "bot_flag": 0,
                })

            logger.info(
                f"CoinGecko trending: {ticker} score={score} rank={rank} "
                f"pct24h={pct_24h:+.2f} heat={heat}"
            )

        logger.info(f"CoinGecko trending: emitted {len(out)} synthetic posts")
        return out


# ---------------------------------------------------------------------------
# Reddit — organic social mentions (with OAuth upgrade path)
# ---------------------------------------------------------------------------
class RedditScraper:
    """Two modes:
      (1) OAuth (recommended for cloud deploys). Set REDDIT_CLIENT_ID,
          REDDIT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD env vars. Hits
          https://oauth.reddit.com with a bearer token. 60 req/min limit.
      (2) Unauthenticated JSON. Reddit hard-blocks this from most cloud IPs
          (Render, Fly, Heroku). Kept as a fallback for local dev.
    Toggle-off entirely with FITPAC_REDDIT_ENABLE=0 (default: auto)."""

    UNAUTH_BASE = "https://www.reddit.com/r/{sub}/new.json?limit={limit}"
    OAUTH_BASE = "https://oauth.reddit.com/r/{sub}/new?limit={limit}"
    TOKEN_URL = "https://www.reddit.com/api/v1/access_token"

    def __init__(self, subreddits: List[str] = None, limit_per_sub: int = 50):
        self.subreddits = subreddits or DEFAULT_SUBREDDITS
        self.limit = limit_per_sub
        self.client_id = os.environ.get("REDDIT_CLIENT_ID", "").strip()
        self.client_secret = os.environ.get("REDDIT_SECRET", "").strip()
        self.username = os.environ.get("REDDIT_USERNAME", "").strip()
        self.password = os.environ.get("REDDIT_PASSWORD", "").strip()
        self._token: Optional[str] = None
        self._token_expires: float = 0.0

    @property
    def has_oauth(self) -> bool:
        return all([self.client_id, self.client_secret, self.username, self.password])

    @property
    def enabled(self) -> bool:
        return os.environ.get("FITPAC_REDDIT_ENABLE", "auto") != "0"

    def _fetch_oauth_token(self) -> Optional[str]:
        """Grab a fresh bearer token via password grant (script-app flow)."""
        import base64
        auth_raw = f"{self.client_id}:{self.client_secret}".encode("utf-8")
        auth = base64.b64encode(auth_raw).decode("utf-8")
        body = urllib.parse.urlencode({
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }).encode("utf-8")
        req = urllib.request.Request(
            self.TOKEN_URL,
            data=body,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                payload = json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            logger.warning(f"Reddit OAuth token fetch failed: {e}")
            return None
        tok = payload.get("access_token")
        expires_in = int(payload.get("expires_in") or 3600)
        self._token = tok
        self._token_expires = time.time() + expires_in - 60
        if tok:
            logger.info("Reddit OAuth: token acquired (expires in %ds)", expires_in)
        return tok

    def _auth_headers(self) -> Optional[Dict[str, str]]:
        if not self.has_oauth:
            return None
        if self._token is None or time.time() >= self._token_expires:
            if not self._fetch_oauth_token():
                return None
        return {"Authorization": f"Bearer {self._token}"}

    def _fetch_sub(self, sub: str) -> List[dict]:
        headers = self._auth_headers()
        if headers is not None:
            url = self.OAUTH_BASE.format(sub=sub, limit=self.limit)
        else:
            url = self.UNAUTH_BASE.format(sub=sub, limit=self.limit)
        try:
            data = _http_get(url, extra_headers=headers)
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

    # Synthetic feeds we produce ourselves (CoinGecko trending, etc.) aren't
    # astroturf — they're structured ground-truth data. Exempt their authors
    # from heuristics that would otherwise flag them for same-author burst or
    # duplicate text.
    TRUSTED_AUTHORS = {"u/coingecko_trending"}

    def score(self, posts: List[Dict]) -> List[Dict]:
        if not posts:
            return posts
        author_counts = Counter(p["author"] for p in posts)
        text_sigs = Counter(re.sub(r"\s+", " ", (p["text"] or "").lower())[:80] for p in posts)

        flagged = 0
        for p in posts:
            if p.get("author") in self.TRUSTED_AUTHORS:
                continue
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
    """Fetch a fresh chain snapshot for every enabled ticker. Returns # written.

    Fallback order per ticker:
      1. Hyperliquid spot (for $HYPE and friends)
      2. DexScreener /tokens/{addr}    — fast path, uses seeded address
      3. DexScreener /search?q={sym}   — resolver fallback; also self-heals
                                         stale/wrong addresses and persists
                                         the fix back to the tickers table
      4. CoinGecko /coins/{id}         — for non-DEX assets ($DOGE, $TAO)
    """
    dex = DexScreenerScraper()
    cg = CoinGeckoScraper()
    hl = HyperliquidScraper()
    resolver = DexScreenerResolver()
    written = 0
    for row in db.list_tickers():
        snap = None
        ticker = row["ticker"]
        chain = row["chain"]

        # Hyperliquid-native spot assets: coin name lives in token_address slot.
        if chain == "hyperliquid" and row["token_address"]:
            snap = hl.snapshot(ticker, row["token_address"])
        else:
            # Fast path: use the seeded on-chain address
            if row["token_address"]:
                snap = dex.snapshot(ticker, row["token_address"], chain)

            # Resolver fallback: if the seeded address returned no pairs (or
            # there's no seeded address at all), search DexScreener by symbol.
            if snap is None and chain != "hyperliquid":
                resolved = resolver.resolve(ticker, hint_chain=chain)
                if resolved:
                    # The resolver already has the pair data — build a snapshot
                    # directly from it rather than paying for a second HTTP call.
                    snap = DexScreenerScraper.pair_to_snapshot(
                        ticker, resolved["chain"], resolved["pair"]
                    )
                    # Persist the resolved address so next cycle skips the search.
                    try:
                        db.update_ticker_address(
                            ticker, resolved["chain"], resolved["token_address"]
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            f"{ticker} | failed to persist resolved address: {exc}"
                        )

        # Last-resort: CoinGecko for non-DEX assets
        if snap is None and row["coingecko_id"]:
            snap = cg.snapshot(ticker, row["coingecko_id"], chain)

        if snap:
            db.insert_chain_snapshot(snap)
            written += 1
            logger.info(
                f"{ticker:<10} | liq=${snap['liquidity_depth_usd']:,.0f} "
                f"| sells_1h={snap['txns_1h_sells']} | insider_proxy={snap['insider_distribution_ratio']}"
            )
        time.sleep(0.3)  # polite throttle for DexScreener
    return written


def scrape_social_all() -> int:
    """Fuse social signal from all available sources. Returns # posts written.

    Order:
      1. CoinGecko trending — always runs (no auth, no cloud blocks).
      2. Reddit — runs only if OAuth creds set OR FITPAC_REDDIT_ENABLE=force.
         On cloud IPs the unauth JSON endpoint is basically guaranteed to
         fail, so we don't even try unless OAuth is configured.
    Both streams are merged, bot-filtered, and upserted together."""
    watchlist = [t["ticker"] for t in db.list_tickers()]
    combined: List[Dict] = []

    # --- CoinGecko trending (primary) ---
    cg_trending = CoinGeckoTrendingScraper()
    combined.extend(cg_trending.scrape(watchlist))

    # --- Reddit (conditional) ---
    reddit = RedditScraper()
    if not reddit.enabled:
        logger.info("Reddit scraping disabled via FITPAC_REDDIT_ENABLE=0")
    elif reddit.has_oauth:
        logger.info("Reddit OAuth credentials present — scraping via oauth.reddit.com")
        combined.extend(reddit.scrape(watchlist))
    elif os.environ.get("FITPAC_REDDIT_ENABLE") == "force":
        logger.info("Reddit scraping forced (unauth mode) — expect 403s on cloud IPs")
        combined.extend(reddit.scrape(watchlist))
    else:
        logger.info(
            "Reddit OAuth not configured; skipping Reddit. "
            "Set REDDIT_CLIENT_ID/SECRET/USERNAME/PASSWORD to enable."
        )

    filtered = BotFilter().score(combined)
    return db.bulk_upsert_posts(filtered)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    db.init_db()
    logger.info("=== Scraping chain data ===")
    n_chain = scrape_chain_all()
    logger.info("=== Scraping social data ===")
    n_social = scrape_social_all()
    logger.info(f"Done. Wrote {n_chain} chain snapshots, {n_social} social posts.")
