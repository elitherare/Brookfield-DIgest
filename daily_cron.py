"""
Daily Cron & Real-Time Monitoring Module for Brookfield Private Equity Intelligence.
Polls newsrooms and RSS feeds, deduplicates against master log, de-bundles multi-deal
releases using Gemini, and broadcasts executive deal alerts to Telegram (@BAMIntelligence_bot).
"""

import argparse
import html
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from api_manager import (
    get_telegram_credentials,
    is_benzinga_configured,
    is_telegram_configured,
)
from classifier import GeminiClassifier
from config import (
    BROOKFIELD_BBU_NEWSROOM_URL,
    BROOKFIELD_MAIN_NEWSROOM_URL,
    DEFAULT_LOG_CSV,
    RATE_LIMIT_SLEEP_SECONDS,
    RSS_FEEDS,
)
from fetcher import (
    fetch_article_body,
    fetch_benzinga_news,
    fetch_brookfield_press_releases,
    fetch_brookfield_shareholder_letters,
    fetch_rss_feed,
)
from pipeline import Pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("daily_cron")


import hashlib

class TelegramNotifier:
    """Handles formatting and dispatching PE deal alerts to Telegram."""

    def __init__(self):
        creds = get_telegram_credentials()
        self.bot_token = creds["bot_token"]
        self.chat_id = creds["chat_id"]
        self.enabled = bool(self.bot_token and self.chat_id)

    def create_deal_keyboard(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build an interactive inline keyboard for Telegram deal alerts."""
        headline = record.get("headline", "")
        deal_hash = hashlib.md5(headline.encode("utf-8", errors="ignore")).hexdigest()[:8]
        row1 = [
            {"text": "🔍 Deep Dive", "callback_data": f"dd:{deal_hash}"},
            {"text": "📊 Similar Deals", "callback_data": f"sim:{deal_hash}"},
        ]
        keyboard = [row1]
        url = record.get("source_url")
        if url and url.startswith("http"):
            keyboard.append([{"text": "🔗 View Source Release", "url": url}])
        return {"inline_keyboard": keyboard}

    def send_message(
        self,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Send an HTML/Markdown formatted message to the configured Telegram chat."""
        if not self.enabled:
            logger.warning("Telegram is not configured. Skipping notification.")
            return False

        api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": False,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            resp = requests.post(api_url, json=payload, timeout=15)
            data = resp.json()
            if data.get("ok"):
                logger.info("Telegram alert delivered successfully.")
                return True
            else:
                logger.error(f"Telegram API Error: {data.get('description')}")
                return False
        except Exception as e:
            logger.error(f"Failed to deliver Telegram notification: {e}")
            return False

    def format_deal_alert(self, record: Dict[str, Any]) -> str:
        """Format an extracted event into an executive PE intelligence alert."""
        headline = html.escape(record.get("headline", "Brookfield Transaction"))
        region = html.escape(record.get("primary_region", "Global / unassigned"))
        signal = html.escape(record.get("signal_type", "Deal"))
        structure = html.escape(record.get("deal_structure", "N/A"))
        summary = html.escape(record.get("one_line_summary", ""))
        size = html.escape(record.get("size_disclosed", "Undisclosed"))
        entities = html.escape(record.get("entities", ""))
        url = record.get("source_url", "")
        pub_date = html.escape(str(record.get("published_date", "")))

        # Emoji mapping
        emoji_map = {
            "Deal": "🎯",
            "Exit": "💰",
            "Fund": "🏦",
            "Results": "📊",
            "People": "👤",
            "Ops": "⚙️",
            "Regulatory": "⚖️",
            "Market": "📈",
        }
        signal_emoji = emoji_map.get(signal, "📢")
        ticker = record.get("ticker", "")
        source = record.get("source", "")
        wire_tag = f"⚡ <b>[BENZINGA WIRE • {ticker}]</b>\n" if (source == "benzinga" or ticker) else ""

        lines = [
            f"{wire_tag}<b>{signal_emoji} BROOKFIELD PE INTELLIGENCE ALERT</b>",
            f"<i>{pub_date}</i>",
            "",
            f"<b>📌 Action:</b> {headline}",
            f"<b>🌍 Region:</b> {region}",
            f"<b>🏷️ Signal:</b> {signal} (Structure: {structure})",
        ]

        if size and size not in ["Undisclosed", "N/A"]:
            lines.append(f"<b>💵 Size:</b> {size}")

        if entities:
            lines.append(f"<b>🏢 Entities:</b> {entities}")

        if summary:
            lines.append(f"\n<b>📝 Executive Summary:</b>\n{summary}")

        if url:
            lines.append(f'\n🔗 <a href="{url}">View Primary Release</a>')

        return "\n".join(lines)


def run_monitoring_cycle(
    pipeline: Pipeline,
    notifier: TelegramNotifier,
    dry_run: bool = False,
    sleep_interval: float = RATE_LIMIT_SLEEP_SECONDS,
) -> Dict[str, int]:
    """
    Execute one complete monitoring cycle across all newsrooms, shareholder letters, and RSS feeds.
    Returns stats on new deals discovered.
    """
    logger.info("=" * 70)
    logger.info(f"STARTING MONITORING CYCLE - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("=" * 70)

    candidates = []

    # 1. Fetch Latest Brookfield Business Partners Press Releases (Page 0)
    logger.info(f"Polling BBU Newsroom: {BROOKFIELD_BBU_NEWSROOM_URL}")
    bbu_cards = fetch_brookfield_press_releases(page=0, base_url=BROOKFIELD_BBU_NEWSROOM_URL)
    logger.info(f"Found {len(bbu_cards)} recent releases from BBU Newsroom")
    candidates.extend(bbu_cards)

    # 2. Fetch Latest Corporate Newsroom (Page 0)
    logger.info(f"Polling Main Corporate Newsroom: {BROOKFIELD_MAIN_NEWSROOM_URL}")
    corp_cards = fetch_brookfield_press_releases(page=0, base_url=BROOKFIELD_MAIN_NEWSROOM_URL)
    logger.info(f"Found {len(corp_cards)} recent releases from Main Newsroom")
    candidates.extend(corp_cards)

    # 3. Fetch Top 3 Latest Shareholder Letters
    logger.info("Checking latest Brookfield Shareholder Letters...")
    letters = fetch_brookfield_shareholder_letters()
    candidates.extend(letters[:3])

    # 4. Fetch Active RSS Feeds
    for feed_url in RSS_FEEDS:
        rss_items = fetch_rss_feed(feed_url)
        logger.info(f"Found {len(rss_items)} items from RSS: {feed_url[:60]}...")
        candidates.extend(rss_items[:10])

    # 5. Fetch Benzinga Institutional News Wire (Massive.com)
    if is_benzinga_configured():
        logger.info("Polling Benzinga Institutional News Wire for BAM, BN, BBU, BIP, BEP...")
        benzinga_items = fetch_benzinga_news(limit=5, days=7)
        candidates.extend(benzinga_items)

    logger.info(f"\nTotal candidate items collected for evaluation: {len(candidates)}")

    stats = {
        "evaluated": 0,
        "novel_events_saved": 0,
        "duplicates": 0,
        "dropped_irrelevant": 0,
    }

    new_saved_records = []

    for item in candidates:
        stats["evaluated"] += 1
        headline = item.get("headline", "")
        url = item.get("url", "")
        pub_date = item.get("published_date")

        # Fast pre-deduplication check by URL before fetching heavy body text
        if url and (pipeline.df["source_url"] == url).any() and not item.get("is_shareholder_letter"):
            stats["duplicates"] += 1
            continue

        # Fetch body if not already present
        if not item.get("body_text"):
            body = fetch_article_body(url)
            item["body_text"] = body or headline

        try:
            res = pipeline.process_item(item, dry_run=dry_run)
            status = res.get("status")

            if status == "saved":
                records = res.get("saved_records", [])
                stats["novel_events_saved"] += len(records)
                for rec in records:
                    new_saved_records.append(rec)
                    logger.info(
                        f"-> [NEW PE EVENT] Region: {rec['primary_region']} | Signal: {rec['signal_type']} | "
                        f"Headline: {rec['headline']}"
                    )
                time.sleep(sleep_interval)

            elif status == "duplicate":
                stats["duplicates"] += 1

            elif status == "dropped_irrelevant":
                stats["dropped_irrelevant"] += 1

        except Exception as e:
            logger.error(f"Error processing candidate '{headline}': {e}")

    # ==============================================================================
    # Telegram Alert Dispatch
    # ==============================================================================
    if new_saved_records and notifier.enabled:
        logger.info(f"Dispatching {len(new_saved_records)} novel deal alerts to Telegram...")
        for rec in new_saved_records:
            msg = notifier.format_deal_alert(rec)
            keyboard = notifier.create_deal_keyboard(rec)
            notifier.send_message(msg, reply_markup=keyboard)
            time.sleep(1.0)
    else:
        logger.info("No novel PE intelligence to broadcast on this cycle.")

    logger.info("\n" + "=" * 70)
    logger.info("CYCLE COMPLETE SUMMARY")
    logger.info(f"Items Evaluated:         {stats['evaluated']}")
    logger.info(f"Novel Intel Saved:       {stats['novel_events_saved']}")
    logger.info(f"Duplicates Skipped:      {stats['duplicates']}")
    logger.info(f"Boilerplate Dropped:     {stats['dropped_irrelevant']}")
    logger.info("=" * 70 + "\n")

    return stats


def parse_args():
    parser = argparse.ArgumentParser(
        description="Daily cron & live watcher for Brookfield Private Equity Intelligence."
    )
    parser.add_argument(
        "--datastore",
        type=str,
        default=DEFAULT_LOG_CSV,
        help=f"Path to master CSV datastore (default: {DEFAULT_LOG_CSV})",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously in background daemon loop",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Seconds between polling cycles in daemon mode (default: 3600s / 1 hour)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without consuming Gemini API quota",
    )
    parser.add_argument(
        "--test-telegram",
        action="store_true",
        help="Send a test deal intelligence alert to Telegram immediately and exit",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    notifier = TelegramNotifier()

    # Handle immediate Telegram test
    if args.test_telegram:
        logger.info("Sending test deal alert to Telegram...")
        sample_record = {
            "published_date": datetime.now().strftime("%b %d, %Y"),
            "headline": "Brookfield Acquires Leading European Logistics Infrastructure Platform",
            "source_url": "https://bbuc.brookfield.com/news-events/press-releases",
            "primary_region": "Europe",
            "signal_type": "Deal",
            "deal_structure": "Carve-out",
            "size_disclosed": "€750 Million",
            "entities": "Brookfield Business Partners, EuroLogistics Group",
            "one_line_summary": "Brookfield completed the acquisition of a premier pan-European temperature-controlled logistics network.",
        }
        msg = notifier.format_deal_alert(sample_record)
        keyboard = notifier.create_deal_keyboard(sample_record)
        success = notifier.send_message(msg, reply_markup=keyboard)
        if success:
            logger.info("Test alert delivered successfully! Check your Telegram app.")
        sys.exit(0 if success else 1)

    # Initialize Pipeline
    classifier = GeminiClassifier()
    pipeline = Pipeline(datastore_path=args.datastore, classifier=classifier)

    if args.loop:
        logger.info(f"Starting continuous daemon mode. Polling every {args.interval} seconds.")
        try:
            while True:
                run_monitoring_cycle(pipeline, notifier, dry_run=args.dry_run)
                logger.info(f"Sleeping for {args.interval} seconds until next cycle...")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user.")
    else:
        # Run single monitoring cycle
        run_monitoring_cycle(pipeline, notifier, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
