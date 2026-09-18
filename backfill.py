"""
Backfill script for Brookfield Private Equity Intelligence Pipeline.
Scrapes the last 24 months of corporate press releases from Brookfield,
extracts headline, date, and full body text, classifies them with Gemini,
deduplicates them across a 3-day rolling window, and logs results to brookfield_24mo_log.csv.
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd

from classifier import GeminiClassifier
from config import (
    BACKFILL_MONTHS,
    BROOKFIELD_BBU_NEWSROOM_URL,
    DEFAULT_LOG_CSV,
    GEMINI_API_KEY,
    RATE_LIMIT_SLEEP_SECONDS,
)
from fetcher import (
    fetch_article_body,
    fetch_brookfield_press_releases,
    fetch_brookfield_shareholder_letters,
)
from pipeline import Pipeline

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backfill")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill 24 months of Brookfield press releases and shareholder letters into PE intelligence datastore."
    )
    parser.add_argument(
        "--months",
        type=int,
        default=BACKFILL_MONTHS,
        help=f"Number of months to backfill (default: {BACKFILL_MONTHS})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_LOG_CSV,
        help=f"Output CSV path (default: {DEFAULT_LOG_CSV})",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=RATE_LIMIT_SLEEP_SECONDS,
        help=f"Seconds to sleep between AI calls for rate limit safety (default: {RATE_LIMIT_SLEEP_SECONDS}s)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of articles to process (useful for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (uses local rule engine instead of consuming Gemini API quota)",
    )
    parser.add_argument(
        "--include-letters",
        action="store_true",
        default=True,
        help="Also ingest quarterly Letters to Shareholders to capture portfolio acquisitions & bolt-ons",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=BROOKFIELD_BBU_NEWSROOM_URL,
        help="Brookfield newsroom URL to scrape",
    )
    return parser.parse_args()


def run_backfill():
    args = parse_args()
    logger.info("=" * 70)
    logger.info("STARTING BROOKFIELD 24-MONTH INTELLIGENCE BACKFILL")
    logger.info("=" * 70)

    # Check API key configuration
    is_dry_run = args.dry_run
    classifier = GeminiClassifier()
    if not classifier.is_configured:
        if not is_dry_run:
            logger.warning(
                "GEMINI_API_KEY is not configured in config.py or environment. "
                "Running in DRY-RUN mode to validate scraping, deduplication, and schema."
            )
            is_dry_run = True
    else:
        logger.info(f"Gemini API configured. Model: {classifier._model.model_name}")

    # Compute cutoff date
    now = datetime.now()
    cutoff_date = now - relativedelta(months=args.months)
    logger.info(f"Current Date: {now.strftime('%Y-%m-%d')}")
    logger.info(f"Cutoff Date ({args.months} months): {cutoff_date.strftime('%Y-%m-%d')}")
    logger.info(f"Target Source: {args.base_url}")
    logger.info(f"Datastore CSV: {args.output}")
    logger.info(f"Rate limit pacing: {args.sleep}s delay per item")

    pipeline = Pipeline(datastore_path=args.output, classifier=classifier)

    stats = {
        "pages_scraped": 0,
        "articles_found": 0,
        "processed": 0,
        "saved": 0,
        "duplicates": 0,
        "irrelevant": 0,
        "errors": 0,
    }

    page = 0
    stop_backfill = False

    while not stop_backfill:
        logger.info(f"\n--- Scraping Page {page} ---")
        cards = fetch_brookfield_press_releases(page=page, base_url=args.base_url)
        stats["pages_scraped"] += 1

        if not cards:
            logger.info("No more press release cards found. Reached end of archive.")
            break

        logger.info(f"Found {len(cards)} press releases on page {page}")

        for card in cards:
            stats["articles_found"] += 1
            headline = card["headline"]
            pub_date = card.get("published_date")
            date_str = card.get("date_str", "")
            url = card["url"]

            logger.info(f"\nChecking: [{date_str}] {headline}")

            # Check cutoff
            if pub_date and pub_date < cutoff_date:
                logger.info(
                    f"Reached article date {pub_date.strftime('%Y-%m-%d')} older than cutoff {cutoff_date.strftime('%Y-%m-%d')}. Stopping backfill."
                )
                stop_backfill = True
                break

            # Check pre-duplicate before fetching body to save network and tokens
            is_dup, dup_reason = pipeline.is_pre_duplicate(headline, pub_date, url)
            if is_dup:
                logger.info(f"-> Skipped duplicate: {dup_reason}")
                stats["duplicates"] += 1
                continue

            # Fetch full article body
            logger.info(f"Fetching full text from {url}")
            body_text = fetch_article_body(url)
            if not body_text:
                logger.warning(f"Could not retrieve body text for: {headline}")
                body_text = headline

            card["body_text"] = body_text

            # Process through pipeline
            try:
                res = pipeline.process_item(card, dry_run=is_dry_run)
                stats["processed"] += 1
                status = res.get("status")

                if status == "saved":
                    records = res.get("saved_records", [])
                    stats["saved"] += len(records)
                    for rec in records:
                        logger.info(
                            f"-> [SAVED EVENT] Region: {rec['primary_region']} | Signal: {rec['signal_type']} | "
                            f"Headline: {rec['headline']} | Structure: {rec['deal_structure']} | Size: {rec.get('size_disclosed', 'N/A')}"
                        )
                    # Pacing sleep
                    time.sleep(args.sleep)

                elif status == "dropped_irrelevant":
                    stats["irrelevant"] += 1
                    logger.info("-> [DROPPED] Non-strategic / boilerplate item silently dropped.")

                elif status == "duplicate":
                    stats["duplicates"] += 1
                    logger.info(f"-> [DUPLICATE] Dropped: {res.get('reason')}")

            except Exception as e:
                logger.error(f"Error processing item '{headline}': {e}")
                stats["errors"] += 1

            # Check optional processing limit
            if args.limit and stats["saved"] >= args.limit:
                logger.info(f"Reached specified limit of {args.limit} saved items.")
                stop_backfill = True
                break

        page += 1
        # Safety bound on max pages
        if page > 25:
            logger.info("Reached maximum safety page limit (25 pages). Stopping.")
            break

    # ==============================================================================
    # Shareholder Letters Ingestion (Portfolio company acquisitions & bolt-ons)
    # ==============================================================================
    if args.include_letters and not (args.limit and stats["saved"] >= args.limit):
        logger.info("\n" + "=" * 70)
        logger.info("INGESTING BROOKFIELD SHAREHOLDER LETTERS (DEALS & BOLT-ONS)")
        logger.info("=" * 70)
        letters = fetch_brookfield_shareholder_letters()
        for letter in letters:
            pub_date = letter.get("published_date")
            headline = letter["headline"]
            url = letter["url"]

            if pub_date and pub_date < cutoff_date:
                logger.info(f"Skipping letter {headline}: older than cutoff.")
                continue

            logger.info(f"\nProcessing Shareholder Letter: {headline}")
            body_text = fetch_article_body(url)
            if not body_text:
                continue
            letter["body_text"] = body_text

            try:
                res = pipeline.process_item(letter, dry_run=is_dry_run)
                stats["processed"] += 1
                status = res.get("status")
                if status == "saved":
                    records = res.get("saved_records", [])
                    stats["saved"] += len(records)
                    for rec in records:
                        logger.info(
                            f"-> [SAVED FROM LETTER] Region: {rec['primary_region']} | Signal: {rec['signal_type']} | "
                            f"Headline: {rec['headline']} | Size: {rec.get('size_disclosed', 'N/A')}"
                        )
                    time.sleep(args.sleep)
                elif status == "duplicate":
                    stats["duplicates"] += 1
            except Exception as e:
                logger.error(f"Error processing shareholder letter '{headline}': {e}")
                stats["errors"] += 1

            if args.limit and stats["saved"] >= args.limit:
                break

    # ==============================================================================
    # Backfill Completion Summary
    # ==============================================================================
    logger.info("\n" + "=" * 70)
    logger.info("BACKFILL EXECUTION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total Pages Scraped:     {stats['pages_scraped']}")
    logger.info(f"Total Articles Found:    {stats['articles_found']}")
    logger.info(f"Novel Articles Checked:  {stats['processed']}")
    logger.info(f"Novel Intel Saved (CSV): {stats['saved']}")
    logger.info(f"Duplicates Skipped:      {stats['duplicates']}")
    logger.info(f"Boilerplate Dropped:     {stats['irrelevant']}")
    logger.info(f"Errors Encountered:      {stats['errors']}")
    logger.info(f"Datastore Location:      {args.output}")

    if os.path.exists(args.output):
        try:
            df = pd.read_csv(args.output)
            logger.info(f"\nTotal Datastore Rows: {len(df)}")
            if not df.empty and "signal_type" in df.columns:
                logger.info("\nSignal Type Breakdown:")
                logger.info(df["signal_type"].value_counts().to_string())
                logger.info("\nPrimary Region Breakdown:")
                logger.info(df["primary_region"].value_counts().to_string())
        except Exception:
            pass

    logger.info("\nNext Step:")
    logger.info(f"Open '{args.output}' in Google Sheets or Excel to review the extracted intelligence.")
    logger.info("=" * 70)


if __name__ == "__main__":
    run_backfill()
