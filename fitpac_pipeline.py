"""
FITPAC Pipeline — one-command orchestrator
==========================================
Usage:
    python3 fitpac_pipeline.py init        # build DB + seed from CSVs if available
    python3 fitpac_pipeline.py scrape      # pull live data from DexScreener + Reddit
    python3 fitpac_pipeline.py scan        # auto-discover trending Solana launches via pump.fun scanner
    python3 fitpac_pipeline.py analyze     # run FITPAC engine, emit alerts.json/.js
    python3 fitpac_pipeline.py run         # scrape + analyze in one shot
    python3 fitpac_pipeline.py prune       # drop posts older than 24h

No pip installs required — pure Python 3.8+ stdlib.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fitpac_db as db
import fitpac_backend as backend
import fitpac_scrapers as scrapers

logger = logging.getLogger("FITPAC_Pipeline")


def cmd_init() -> None:
    db.init_db()
    csv_dir = Path(__file__).parent / "data"
    if csv_dir.exists():
        logger.info("Legacy CSVs found — seeding one-time fixture data.")
        db.seed_from_csv(csv_dir)
    logger.info(f"DB ready at {db.DB_PATH}")


def cmd_scrape() -> None:
    db.init_db()
    logger.info("=== Scraping chain data (DexScreener / CoinGecko) ===")
    n_chain = scrapers.scrape_chain_all()
    logger.info("=== Scraping social data (Reddit) ===")
    n_social = scrapers.scrape_social_all()
    logger.info(f"Scrape done. {n_chain} chain snapshots, {n_social} social posts.")


def cmd_analyze() -> None:
    alerts = backend.run_backend_cycle()
    logger.info(f"Analyze done. Emitted {len(alerts)} alerts.")


def cmd_scan() -> None:
    db.init_db()
    logger.info("=== Scanning pump.fun / DexScreener boosts for trending launches ===")
    scanner = scrapers.PumpFunScanner()
    added = scanner.register(limit=5)
    logger.info(f"Scan done. {added} new tickers added to watchlist.")


def cmd_run() -> None:
    cmd_scrape()
    cmd_analyze()


def cmd_prune() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    n = db.prune_old_posts(cutoff)
    logger.info(f"Pruned {n} posts older than 24h.")


COMMANDS = {
    "init":    cmd_init,
    "scrape":  cmd_scrape,
    "scan":    cmd_scan,
    "analyze": cmd_analyze,
    "run":     cmd_run,
    "prune":   cmd_prune,
}


def main():
    parser = argparse.ArgumentParser(description="FITPAC Alert Engine pipeline")
    parser.add_argument("command", choices=list(COMMANDS.keys()))
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    COMMANDS[args.command]()


if __name__ == "__main__":
    main()
