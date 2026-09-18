"""
Interactive Two-Way Telegram Bot for Brookfield Private Equity Intelligence.
Provides continuous 2-way conversational Q&A via Gemini, slash commands,
interactive inline buttons under deal alerts, and background monitoring sweeps.
"""

import argparse
import hashlib
import html
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from api_manager import (
    get_gemini_api_key,
    get_telegram_credentials,
    is_benzinga_configured,
    is_gemini_configured,
    is_telegram_configured,
)
from classifier import GeminiClassifier
from config import (
    DEFAULT_LOG_CSV,
    GEMINI_MODEL_NAME,
    RATE_LIMIT_SLEEP_SECONDS,
)
from daily_cron import TelegramNotifier, run_monitoring_cycle
from fetcher import fetch_benzinga_news
from pipeline import Pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("interactive_bot")


class DealDataManager:
    """Manages querying and filtering over the master CSV datastore."""

    def __init__(self, csv_path: str = DEFAULT_LOG_CSV):
        self.csv_path = csv_path

    def load_data(self) -> pd.DataFrame:
        """Load the master CSV log into a pandas DataFrame."""
        if not os.path.exists(self.csv_path):
            return pd.DataFrame()
        try:
            df = pd.read_csv(self.csv_path)
            # Add hash column for fast button lookups
            if "headline" in df.columns:
                df["deal_hash"] = df["headline"].apply(
                    lambda h: hashlib.md5(str(h).encode("utf-8", errors="ignore")).hexdigest()[:8]
                )
            return df
        except Exception as e:
            logger.error(f"Error loading datastore from {self.csv_path}: {e}")
            return pd.DataFrame()

    def get_latest_deals(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Return the N most recent events."""
        df = self.load_data()
        if df.empty:
            return []
        # Return tail in reverse (most recently appended first)
        records = df.tail(limit).to_dict(orient="records")
        records.reverse()
        return records

    def search_deals(self, query: str, limit: int = 6) -> List[Dict[str, Any]]:
        """Search across headline, entities, summary, and region with keyword relevance."""
        df = self.load_data()
        if df.empty:
            return []
        q = query.lower().strip()
        search_cols = [
            c for c in ["headline", "entities", "one_line_summary", "primary_region", "signal_type", "deal_structure"]
            if c in df.columns
        ]

        # First try exact substring match
        mask = False
        for col in search_cols:
            mask = mask | df[col].astype(str).str.lower().str.contains(re.escape(q))

        matches_df = df[mask]

        # If no exact match (e.g. natural language sentence), search for keyword tokens
        if matches_df.empty:
            stopwords = {
                "what", "when", "where", "which", "who", "whom", "this", "that", "these", "those",
                "brookfield", "acquire", "acquires", "acquisition", "deal", "deals", "have", "has",
                "about", "their", "they", "from", "with", "into", "tell", "show", "list", "more", "does"
            }
            tokens = [w for w in re.findall(r"[a-zA-Z0-9]+", q) if len(w) > 2 and w not in stopwords]
            if tokens:
                token_mask = False
                for token in tokens:
                    for col in search_cols:
                        token_mask = token_mask | df[col].astype(str).str.lower().str.contains(re.escape(token))
                matches_df = df[token_mask]

        if matches_df.empty:
            return []

        matches = matches_df.tail(limit).to_dict(orient="records")
        matches.reverse()
        return matches

    def filter_by_region(self, region: str, limit: int = 6) -> List[Dict[str, Any]]:
        """Filter deals by primary geographic region."""
        df = self.load_data()
        if df.empty:
            return []
        reg = region.lower().strip()
        mask = df["primary_region"].astype(str).str.lower().str.contains(reg)
        matches = df[mask].tail(limit).to_dict(orient="records")
        matches.reverse()
        return matches

    def get_exits(self, limit: int = 6) -> List[Dict[str, Any]]:
        """Return recent exit or divestiture events."""
        df = self.load_data()
        if df.empty:
            return []
        mask = (df["signal_type"].astype(str).str.lower() == "exit") | (
            df["deal_structure"].astype(str).str.lower() == "exit"
        )
        matches = df[mask].tail(limit).to_dict(orient="records")
        matches.reverse()
        return matches

    def get_letters(self, limit: int = 4) -> List[Dict[str, Any]]:
        """Return recent quarterly shareholder/unitholder letter events."""
        df = self.load_data()
        if df.empty:
            return []
        mask = df["headline"].astype(str).str.lower().str.contains("letter to")
        matches = df[mask].tail(limit).to_dict(orient="records")
        matches.reverse()
        return matches

    def get_deal_by_hash(self, deal_hash: str) -> Optional[Dict[str, Any]]:
        """Lookup a specific deal by its 8-char MD5 hash."""
        df = self.load_data()
        if df.empty or "deal_hash" not in df.columns:
            return None
        matches = df[df["deal_hash"] == deal_hash]
        if not matches.empty:
            return matches.iloc[0].to_dict()
        return None

    def get_similar_deals(self, record: Dict[str, Any], limit: int = 3) -> List[Dict[str, Any]]:
        """Find other deals matching the region or signal type of the given record."""
        df = self.load_data()
        if df.empty:
            return []
        current_hash = record.get("deal_hash", "")
        region = str(record.get("primary_region", "")).strip()
        signal = str(record.get("signal_type", "")).strip()

        mask = (df["deal_hash"] != current_hash) & (
            (df["primary_region"] == region) | (df["signal_type"] == signal)
        )
        matches = df[mask].tail(limit).to_dict(orient="records")
        matches.reverse()
        return matches

    def get_stats(self) -> Dict[str, Any]:
        """Compute portfolio intelligence summary statistics."""
        df = self.load_data()
        if df.empty:
            return {"total_events": 0}

        total = len(df)
        region_counts = df["primary_region"].value_counts().head(5).to_dict()
        signal_counts = df["signal_type"].value_counts().to_dict()
        top_structures = df["deal_structure"].value_counts().head(4).to_dict()

        return {
            "total_events": total,
            "regions": region_counts,
            "signals": signal_counts,
            "structures": top_structures,
            "last_entry": df.iloc[-1].get("headline", "") if total > 0 else "",
            "last_date": df.iloc[-1].get("published_date", "") if total > 0 else "",
        }


class ConversationalAssistant:
    """Gemini-powered Q&A and Deal Deep-Dive generator."""

    def __init__(self, data_mgr: DealDataManager):
        self.data_mgr = data_mgr
        self.api_key = get_gemini_api_key()
        self._gemini_model = None

        if is_gemini_configured():
            try:
                import google.generativeai as genai

                genai.configure(api_key=self.api_key)
                self._gemini_model = genai.GenerativeModel(GEMINI_MODEL_NAME)
                logger.info(f"ConversationalAssistant initialized with model: {GEMINI_MODEL_NAME}")
            except Exception as e:
                logger.warning(f"Could not initialize Gemini model for chat: {e}")

    def generate_deal_deep_dive(self, record: Dict[str, Any]) -> str:
        """Generate a 3-bullet executive private equity breakdown for a transaction."""
        headline = record.get("headline", "Transaction")
        region = record.get("primary_region", "N/A")
        signal = record.get("signal_type", "N/A")
        structure = record.get("deal_structure", "N/A")
        size = record.get("size_disclosed", "Undisclosed")
        entities = record.get("entities", "")
        summary = record.get("one_line_summary", "")

        if self._gemini_model:
            prompt = (
                "You are an elite Private Equity Managing Director specializing in Brookfield Asset Management.\n"
                f"Analyze the following deal announcement and provide an executive 3-bullet strategic memo:\n\n"
                f"Headline: {headline}\n"
                f"Target Region: {region}\n"
                f"Signal: {signal}\n"
                f"Structure: {structure}\n"
                f"Size Disclosed: {size}\n"
                f"Key Entities: {entities}\n"
                f"Summary: {summary}\n\n"
                "Format strictly with these 3 bullets:\n"
                "• <b>Strategic Rationale & Fit:</b> (bolt-on vs platform, market thesis)\n"
                "• <b>Deal Architecture & Valuation:</b> (carve-out/take-private mechanics, EBITDA/pricing context if available)\n"
                "• <b>Operational Angle & Value Creation:</b> (Brookfield operating model, synergies, potential risks)\n\n"
                "Keep each bullet under 2 crisp sentences. Do not include boilerplate."
            )
            try:
                resp = self._gemini_model.generate_content(prompt)
                if resp and resp.text:
                    return f"<b>🔎 STRATEGIC DEEP DIVE</b>\n<i>{html.escape(headline)}</i>\n\n{resp.text.strip()}"
            except Exception as e:
                logger.warning(f"Gemini deep dive fallback: {e}")

        # Rule-based fallback
        return (
            f"<b>🔎 STRATEGIC DEEP DIVE</b>\n<i>{html.escape(headline)}</i>\n\n"
            f"• <b>Strategic Rationale:</b> Acquisition aligned with Brookfield's global {region} investment strategy in {signal}.\n"
            f"• <b>Deal Architecture:</b> Executed as a {structure} transaction. Disclosed valuation: {size}.\n"
            f"• <b>Value Creation Angle:</b> Focuses on operational integration across {entities or 'target asset'}."
        )

    def answer_general_question(self, user_question: str) -> str:
        """Ground the user query against relevant records and answer via Gemini."""
        # Retrieve context: search relevant deals + top recent deals
        matched_deals = self.data_mgr.search_deals(user_question, limit=8)
        if not matched_deals:
            matched_deals = self.data_mgr.get_latest_deals(limit=8)

        context_lines = []
        for d in matched_deals:
            context_lines.append(
                f"- [{d.get('published_date', 'N/A')}] {d.get('headline')} | Region: {d.get('primary_region')} | "
                f"Signal: {d.get('signal_type')} | Size: {d.get('size_disclosed')} | Summary: {d.get('one_line_summary')}"
            )
        context_str = "\n".join(context_lines)

        if self._gemini_model:
            prompt = (
                "You are the Brookfield Private Equity Intelligence AI Assistant on Telegram.\n"
                "Answer the user's question using ONLY the provided verified deal log context below. "
                "Be executive, precise, and factual. Cite specific company names, dates, deal sizes, or regions.\n"
                "Use clean HTML formatting (<b>bold</b>, <i>italics</i>, • bullets) suitable for Telegram.\n\n"
                f"Verified Deals Context:\n{context_str}\n\n"
                f"User Question: {user_question}\n\n"
                "Answer:"
            )
            try:
                resp = self._gemini_model.generate_content(prompt)
                if resp and resp.text:
                    return resp.text.strip()
            except Exception as e:
                logger.warning(f"Gemini Q&A fallback: {e}")

        # Rule-based search response
        if matched_deals:
            lines = [f"<b>🔍 Matches for:</b> <i>{html.escape(user_question)}</i>\n"]
            for d in matched_deals[:4]:
                lines.append(f"• <b>{html.escape(str(d.get('headline')))}</b>")
                lines.append(f"  <i>{d.get('published_date')}</i> | {d.get('primary_region')} | {d.get('size_disclosed', 'Undisclosed')}")
                if d.get("source_url"):
                    lines.append(f'  🔗 <a href="{d.get("source_url")}">Read Release</a>')
                lines.append("")
            return "\n".join(lines)

        return "No specific deals found matching your query in the 24-month master log. Try `/latest` or `/stats`."


class InteractiveTelegramBot:
    """Two-way Telegram Bot handler managing long polling and callbacks."""

    def __init__(
        self,
        datastore_path: str = DEFAULT_LOG_CSV,
        poll_interval_seconds: int = 3600,
    ):
        creds = get_telegram_credentials()
        self.bot_token = creds["bot_token"]
        self.authorized_chat_id = str(creds["chat_id"]) if creds["chat_id"] else ""
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.enabled = bool(self.bot_token and self.authorized_chat_id)

        self.data_mgr = DealDataManager(csv_path=datastore_path)
        self.assistant = ConversationalAssistant(data_mgr=self.data_mgr)
        self.notifier = TelegramNotifier()
        self.pipeline = Pipeline(datastore_path=datastore_path, classifier=GeminiClassifier())

        self.poll_interval = poll_interval_seconds
        self.last_scan_time: Optional[datetime] = None
        self.is_scanning = False
        self.stop_event = threading.Event()
        self.digest_hour = 8  # 8:00 AM daily briefing
        self.last_digest_date = ""

    def is_authorized(self, chat_id: Any) -> bool:
        """Check if incoming message sender matches configured TELEGRAM_CHAT_ID."""
        if not self.authorized_chat_id:
            return True  # Open if not restricted
        return str(chat_id) == self.authorized_chat_id

    def send_message(
        self,
        chat_id: Any,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Send message directly to a specific chat ID, chunking if necessary."""
        if not text:
            return False

        # Telegram hard limit is 4096 characters; chunk by paragraph if large
        if len(text) > 4000:
            chunks = []
            curr = ""
            for part in text.split("\n\n"):
                if len(curr) + len(part) + 2 > 4000:
                    if curr:
                        chunks.append(curr.strip())
                    curr = part + "\n\n"
                else:
                    curr += part + "\n\n"
            if curr:
                chunks.append(curr.strip())

            all_ok = True
            for i, chunk in enumerate(chunks):
                rm = reply_markup if i == len(chunks) - 1 else None
                ok = self.send_message(chat_id, chunk, parse_mode=parse_mode, reply_markup=rm)
                all_ok = all_ok and ok
            return all_ok

        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            resp = requests.post(f"{self.api_url}/sendMessage", json=payload, timeout=15)
            data = resp.json()
            if not data.get("ok"):
                logger.error(f"Telegram sendMessage failed: {data.get('description')}")
                if parse_mode == "HTML":
                    logger.info("Retrying sendMessage without HTML parse mode...")
                    payload.pop("parse_mode", None)
                    retry_resp = requests.post(f"{self.api_url}/sendMessage", json=payload, timeout=15)
                    return retry_resp.json().get("ok", False)
                return False
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def send_document(
        self,
        chat_id: Any,
        file_path: str,
        caption: str = "",
    ) -> bool:
        """Upload and send a file/spreadsheet to the Telegram chat."""
        if not os.path.exists(file_path):
            logger.error(f"File not found for sendDocument: {file_path}")
            return False
        try:
            with open(file_path, "rb") as f:
                files = {"document": (os.path.basename(file_path), f, "text/csv")}
                data = {"chat_id": chat_id, "caption": caption}
                resp = requests.post(f"{self.api_url}/sendDocument", data=data, files=files, timeout=30)
                res_json = resp.json()
                if res_json.get("ok"):
                    logger.info("Document uploaded successfully to Telegram.")
                    return True
                else:
                    logger.error(f"Telegram sendDocument error: {res_json.get('description')}")
                    return False
        except Exception as e:
            logger.error(f"Error uploading document: {e}")
            return False

    def answer_callback_query(self, callback_id: str, text: Optional[str] = None) -> None:
        """Acknowledge a Telegram inline button click to remove loading spinner."""
        try:
            payload = {"callback_query_id": callback_id}
            if text:
                payload["text"] = text
            requests.post(f"{self.api_url}/answerCallbackQuery", json=payload, timeout=10)
        except Exception as e:
            logger.error(f"Error answering callback query: {e}")

    # ==============================================================================
    # Slash Command Handlers
    # ==============================================================================

    def handle_start_or_help(self, chat_id: Any) -> None:
        """Display welcoming command cheatsheet and status."""
        msg = (
            "🏛 <b>BROOKFIELD PE INTELLIGENCE ASSISTANT</b>\n\n"
            "Welcome! I track Brookfield dealflow, exits, and shareholder letters, "
            "powered by Gemini and real-time newsroom scrapers.\n\n"
            "<b>⚡ Fast Commands:</b>\n"
            "• <b>/latest [N]</b> — View last N deals (e.g. <code>/latest 5</code>)\n"
            "• <b>/search &lt;query&gt;</b> — Search deals (e.g. <code>/search Reliance</code>)\n"
            "• <b>/deals [region]</b> — Filter by region (e.g. <code>/deals Europe</code>)\n"
            "• <b>/exits</b> — View recent sales and divestitures\n"
            "• <b>/letters</b> — Key highlights from Shareholder Letters\n"
            "• <b>/digest</b> — Morning executive briefing of recent dealflow\n"
            "• <b>/wire [ticker]</b> — Real-time Benzinga wire (e.g. <code>/wire</code> or <code>/wire BAM</code>)\n"
            "• <b>/export</b> — Download master CSV datastore directly to phone\n"
            "• <b>/stats</b> — Portfolio summary & regional breakdown\n"
            "• <b>/scan</b> — Trigger an immediate live news sweep\n"
            "• <b>/status</b> — Check bot health and datastore stats\n\n"
            "<b>💬 Conversational Q&A:</b>\n"
            "You can also ask me anything in plain English! For example:\n"
            "<i>• \"What acquisitions did Brookfield make in data centers?\"\n"
            "• \"Tell me about the Reliance Worldwide transaction.\"\n"
            "• \"What carve-outs were executed in 2025?\"</i>"
        )
        self.send_message(chat_id, msg)

    def handle_latest(self, chat_id: Any, text: str) -> None:
        """Return the N most recent deals."""
        parts = text.strip().split()
        limit = 5
        if len(parts) > 1 and parts[1].isdigit():
            limit = min(int(parts[1]), 10)

        deals = self.data_mgr.get_latest_deals(limit=limit)
        if not deals:
            self.send_message(chat_id, "No deal records found in the master log.")
            return

        lines = [f"<b>🎯 LATEST {len(deals)} BROOKFIELD DEALS</b>\n"]
        for idx, d in enumerate(deals, 1):
            h = html.escape(str(d.get("headline", "Transaction")))
            reg = html.escape(str(d.get("primary_region", "Global")))
            sig = html.escape(str(d.get("signal_type", "Deal")))
            size = html.escape(str(d.get("size_disclosed", "Undisclosed")))
            date = str(d.get("published_date", ""))
            url = d.get("source_url", "")

            entry = f"<b>{idx}. {h}</b>\n"
            entry += f"   <i>{date}</i> | 🌍 {reg} | 🏷️ {sig}"
            if size and size != "Undisclosed":
                entry += f" | 💵 {size}"
            if url:
                entry += f' | <a href="{url}">Source</a>'
            lines.append(entry)

        self.send_message(chat_id, "\n\n".join(lines))

    def handle_search(self, chat_id: Any, text: str) -> None:
        """Search records for a specific term."""
        parts = text.strip().split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            self.send_message(chat_id, "Usage: <code>/search &lt;keyword&gt;</code> (e.g. <code>/search logistics</code>)")
            return

        query = parts[1].strip()
        matches = self.data_mgr.search_deals(query, limit=5)
        if not matches:
            self.send_message(chat_id, f"No transactions found matching '<b>{html.escape(query)}</b>'.")
            return

        lines = [f"<b>🔍 Search Results for:</b> <i>{html.escape(query)}</i> ({len(matches)} matches)\n"]
        for idx, d in enumerate(matches, 1):
            h = html.escape(str(d.get("headline", "")))
            reg = html.escape(str(d.get("primary_region", "N/A")))
            date = str(d.get("published_date", ""))
            size = d.get("size_disclosed", "Undisclosed")
            url = d.get("source_url", "")

            entry = f"<b>{idx}. {h}</b>\n   <i>{date}</i> | 🌍 {reg}"
            if size != "Undisclosed":
                entry += f" | 💵 {size}"
            if url:
                entry += f' | <a href="{url}">Release</a>'
            lines.append(entry)

        self.send_message(chat_id, "\n\n".join(lines))

    def handle_deals(self, chat_id: Any, text: str) -> None:
        """Filter deals by geographic region."""
        parts = text.strip().split(maxsplit=1)
        region = parts[1].strip() if len(parts) > 1 else ""
        if not region:
            self.send_message(
                chat_id,
                "Usage: <code>/deals &lt;region&gt;</code>\n"
                "Examples:\n"
                "• <code>/deals Europe</code>\n"
                "• <code>/deals North America</code>\n"
                "• <code>/deals Asia Pacific</code>",
            )
            return

        matches = self.data_mgr.filter_by_region(region, limit=5)
        if not matches:
            self.send_message(chat_id, f"No recent deals found for region '<b>{html.escape(region)}</b>'.")
            return

        lines = [f"<b>🌍 Recent Deals in {html.escape(region.title())}:</b>\n"]
        for idx, d in enumerate(matches, 1):
            h = html.escape(str(d.get("headline", "")))
            date = str(d.get("published_date", ""))
            struct = d.get("deal_structure", "N/A")
            lines.append(f"<b>{idx}. {h}</b>\n   <i>{date}</i> | Structure: {struct}")

        self.send_message(chat_id, "\n\n".join(lines))

    def handle_exits(self, chat_id: Any) -> None:
        """Display recent exit/divestment events."""
        exits = self.data_mgr.get_exits(limit=5)
        if not exits:
            self.send_message(chat_id, "No exit transactions logged in the current dataset.")
            return

        lines = ["<b>💰 BROOKFIELD RECENT EXITS & DIVESTITURES</b>\n"]
        for idx, d in enumerate(exits, 1):
            h = html.escape(str(d.get("headline", "")))
            date = str(d.get("published_date", ""))
            reg = d.get("primary_region", "")
            size = d.get("size_disclosed", "Undisclosed")
            lines.append(f"<b>{idx}. {h}</b>\n   <i>{date}</i> | 🌍 {reg} | 💵 {size}")

        self.send_message(chat_id, "\n\n".join(lines))

    def handle_letters(self, chat_id: Any) -> None:
        """Display recent letters to shareholders."""
        letters = self.data_mgr.get_letters(limit=4)
        if not letters:
            self.send_message(chat_id, "No shareholder letters logged in the dataset.")
            return

        lines = ["<b>📜 RECENT BROOKFIELD SHAREHOLDER LETTERS</b>\n"]
        for idx, d in enumerate(letters, 1):
            h = html.escape(str(d.get("headline", "")))
            summary = html.escape(str(d.get("one_line_summary", "")))
            url = d.get("source_url", "")
            entry = f"<b>{idx}. {h}</b>\n{summary}"
            if url:
                entry += f'\n🔗 <a href="{url}">Read Full Letter</a>'
            lines.append(entry)

        self.send_message(chat_id, "\n\n".join(lines))

    def handle_stats(self, chat_id: Any) -> None:
        """Display portfolio intelligence analytics."""
        stats = self.data_mgr.get_stats()
        total = stats.get("total_events", 0)
        if total == 0:
            self.send_message(chat_id, "Datastore is currently empty.")
            return

        reg_lines = [f"  • {k}: <b>{v}</b>" for k, v in stats.get("regions", {}).items()]
        sig_lines = [f"  • {k}: <b>{v}</b>" for k, v in stats.get("signals", {}).items()]
        struct_lines = [f"  • {k}: <b>{v}</b>" for k, v in stats.get("structures", {}).items()]

        msg = (
            "<b>📊 BROOKFIELD PE INTELLIGENCE SNAPSHOT</b>\n\n"
            f"<b>📁 Total Monitored Events:</b> <code>{total}</code>\n"
            f"<b>🕒 Last Recorded:</b> <i>{stats.get('last_date', 'N/A')}</i>\n"
            f"<b>📌 Latest Action:</b> {html.escape(stats.get('last_entry', 'N/A')[:80])}...\n\n"
            f"<b>🌍 By Region:</b>\n" + "\n".join(reg_lines) + "\n\n"
            f"<b>🏷️ By Signal:</b>\n" + "\n".join(sig_lines) + "\n\n"
            f"<b>⚙️ Top Deal Structures:</b>\n" + "\n".join(struct_lines)
        )
        self.send_message(chat_id, msg)

    def handle_status(self, chat_id: Any) -> None:
        """Report bot status and VM info."""
        stats = self.data_mgr.get_stats()
        total = stats.get("total_events", 0)
        last_scan_str = (
            self.last_scan_time.strftime("%Y-%m-%d %H:%M:%S UTC")
            if self.last_scan_time
            else "Pending first sweep"
        )
        msg = (
            "<b>🟢 SYSTEM STATUS: ONLINE</b>\n\n"
            f"• <b>Bot:</b> @BAMIntelligence_bot\n"
            f"• <b>Gemini Engine:</b> <code>{GEMINI_MODEL_NAME}</code>\n"
            f"• <b>Master Log:</b> <code>{total}</code> classified events\n"
            f"• <b>Monitoring Interval:</b> Every {self.poll_interval // 60} minutes\n"
            f"• <b>Last Scan:</b> <i>{last_scan_str}</i>\n"
            f"• <b>Active Scan Running:</b> {'Yes' if self.is_scanning else 'No'}"
        )
        self.send_message(chat_id, msg)

    def handle_scan(self, chat_id: Any) -> None:
        """Trigger an on-demand scraping and classification sweep."""
        if self.is_scanning:
            self.send_message(chat_id, "⚠️ A newsroom sweep is already actively running. Please wait.")
            return

        self.send_message(chat_id, "🔄 <b>Starting on-demand newsroom & RSS sweep...</b>\nThis takes ~30-45 seconds.")

        def scan_worker():
            self.is_scanning = True
            try:
                cycle_stats = run_monitoring_cycle(
                    pipeline=self.pipeline,
                    notifier=self.notifier,
                    sleep_interval=RATE_LIMIT_SLEEP_SECONDS,
                )
                self.last_scan_time = datetime.now(timezone.utc)
                report = (
                    "<b>✅ SWEEP COMPLETE</b>\n\n"
                    f"• <b>Items Evaluated:</b> {cycle_stats.get('evaluated', 0)}\n"
                    f"• <b>Novel Deals Discovered:</b> {cycle_stats.get('novel_events_saved', 0)}\n"
                    f"• <b>Duplicates Skipped:</b> {cycle_stats.get('duplicates', 0)}\n"
                    f"• <b>Boilerplate Filtered:</b> {cycle_stats.get('dropped_irrelevant', 0)}"
                )
                self.send_message(chat_id, report)
            except Exception as e:
                logger.error(f"Error during on-demand scan: {e}")
                self.send_message(chat_id, f"❌ Sweep encountered an error: {html.escape(str(e))}")
            finally:
                self.is_scanning = False

        threading.Thread(target=scan_worker, daemon=True).start()

    def generate_morning_digest(self) -> str:
        """Generate a structured executive morning PE intelligence briefing."""
        df = self.data_mgr.load_data()
        if df.empty:
            return "No deal intelligence records available for the morning digest."

        now = datetime.now()
        date_header = now.strftime("%A, %b %d, %Y")

        deals = self.data_mgr.get_latest_deals(limit=8)
        exits = self.data_mgr.get_exits(limit=3)
        letters = self.data_mgr.get_letters(limit=2)
        stats = self.data_mgr.get_stats()

        lines = [
            "🌅 <b>BROOKFIELD MORNING PE BRIEFING</b>",
            f"<i>{date_header} • Executive Daily Intelligence</i>\n",
            "<b>🎯 Recent M&amp;A &amp; Growth Deals:</b>",
        ]

        deal_count = 0
        for d in deals:
            sig = str(d.get("signal_type", ""))
            if sig.lower() in ["deal", "direct growth"]:
                deal_count += 1
                h = html.escape(str(d.get("headline", "")))
                reg = html.escape(str(d.get("primary_region", "")))
                sz = d.get("size_disclosed", "Undisclosed")
                line = f"• <b>{h}</b>\n  🌍 {reg}"
                if sz and sz != "Undisclosed":
                    line += f" | 💵 {sz}"
                lines.append(line)

        if deal_count == 0:
            lines.append("• <i>No major M&A announcements in immediate window.</i>")

        if exits:
            lines.append("\n<b>💰 Exits &amp; Monetizations:</b>")
            for ex in exits[:2]:
                h = html.escape(str(ex.get("headline", "")))
                sz = ex.get("size_disclosed", "Undisclosed")
                lines.append(f"• <b>{h}</b> (Size: {sz})")

        if letters:
            lines.append("\n<b>📜 Leadership &amp; Shareholder Insight:</b>")
            for let in letters[:1]:
                h = html.escape(str(let.get("headline", "")))
                summ = html.escape(str(let.get("one_line_summary", ""))[:130])
                lines.append(f"• <b>{h}</b>: {summ}...")

        lines.extend([
            "",
            f"<b>📊 Master Datastore:</b> <code>{stats.get('total_events', 0)}</code> verified events.",
            "<i>Tip: Type /export to download the full CSV log, or /latest for recent dealflow.</i>",
        ])

        return "\n".join(lines)

    def handle_digest(self, chat_id: Any) -> None:
        """Send the morning executive briefing on demand."""
        self.send_message(chat_id, "🌅 <i>Compiling Brookfield morning executive briefing...</i>")
        digest_text = self.generate_morning_digest()
        self.send_message(chat_id, digest_text)

    def handle_export(self, chat_id: Any) -> None:
        """Export master CSV datastore directly to the Telegram chat as a file."""
        csv_path = self.data_mgr.csv_path
        if not os.path.exists(csv_path):
            self.send_message(chat_id, "⚠️ Master datastore CSV not found.")
            return

        df = self.data_mgr.load_data()
        total = len(df)
        self.send_message(chat_id, f"📤 <i>Uploading master intelligence spreadsheet ({total} rows)...</i>")
        caption = f"📊 Brookfield Master PE Intelligence Datastore ({total} verified events)"
        success = self.send_document(chat_id, csv_path, caption=caption)
        if not success:
            self.send_message(chat_id, "❌ Failed to send CSV file to Telegram. Please check server logs.")

    def handle_wire(self, chat_id: Any, text: str) -> None:
        """Fetch and display real-time institutional news from the Benzinga Wire."""
        if not is_benzinga_configured():
            self.send_message(
                chat_id,
                "⚠️ <b>Benzinga News API is not configured.</b>\n"
                "Please add <code>MASSIVE_BENZINGA_API_KEY</code> to your <code>.env</code> file."
            )
            return

        parts = text.strip().split()
        tickers = None
        limit = 5
        days = 30
        valid_tickers = ["BAM", "BN", "BBU", "BIP", "BEP"]
        for p in parts[1:]:
            p_up = p.upper()
            if p_up in valid_tickers:
                tickers = [p_up]
            elif p_up.endswith("D") and p_up[:-1].isdigit():
                days = int(p_up[:-1])
            elif p_up == "ALL":
                days = None
            elif p.isdigit():
                limit = min(max(int(p), 1), 10)

        target_display = f"<code>{tickers[0]}</code>" if tickers else "<code>BAM, BN, BBU, BIP, BEP</code>"
        time_desc = f"past {days} days" if days else "all-time"
        self.send_message(chat_id, f"⚡ <i>Fetching live Benzinga wire for {target_display} ({time_desc})...</i>")

        articles = fetch_benzinga_news(tickers=tickers, limit=limit, days=days)
        if not articles:
            # If user queried a specific ticker with days cutoff and found nothing, check without cutoff
            if tickers and days:
                fallback = fetch_benzinga_news(tickers=tickers, limit=3, days=None)
                if fallback:
                    self.send_message(
                        chat_id,
                        f"ℹ️ <i>No wire items found for {target_display} in the {time_desc}. Showing latest available coverage:</i>"
                    )
                    articles = fallback

            if not articles:
                self.send_message(chat_id, f"No Benzinga wire items found for {target_display} ({time_desc}).")
                return

        articles = articles[:limit]

        lines = [f"⚡ <b>BENZINGA REAL-TIME WIRE ({len(articles)} items)</b>\n"]
        for idx, art in enumerate(articles, 1):
            raw_h = html.unescape(str(art.get("headline", "Headline")))
            h = html.escape(raw_h)
            date_str = str(art.get("date_str", "") or art.get("published_date", ""))
            if len(date_str) > 16:
                date_str = date_str[:16].replace("T", " ")
            url = art.get("url", "")
            ticker = art.get("ticker", "PE")

            entry = f"<b>{idx}. {h}</b>\n"
            entry += f"   <i>{date_str}</i> | 🏷️ <code>{ticker}</code>"
            if url:
                entry += f' | <a href="{html.escape(url)}">Read Wire</a>'
            lines.append(entry)

        self.send_message(chat_id, "\n\n".join(lines))

    # ==============================================================================
    # Inline Button Callback Query Handler
    # ==============================================================================

    def handle_callback_query(self, callback_query: Dict[str, Any]) -> None:
        """Handle inline button clicks under deal alert cards."""
        callback_id = callback_query.get("id", "")
        data = callback_query.get("data", "")
        chat_id = callback_query.get("message", {}).get("chat", {}).get("id")

        if not self.is_authorized(chat_id):
            self.answer_callback_query(callback_id, "Unauthorized")
            return

        self.answer_callback_query(callback_id, "Processing request...")

        if data.startswith("dd:"):
            # Deep dive request
            deal_hash = data[3:]
            record = self.data_mgr.get_deal_by_hash(deal_hash)
            if not record:
                self.send_message(chat_id, "⚠️ Deal record details not found in datastore.")
                return

            self.send_message(chat_id, "⏳ <i>Generating strategic PE deep dive...</i>")
            analysis = self.assistant.generate_deal_deep_dive(record)
            self.send_message(chat_id, analysis)

        elif data.startswith("sim:"):
            # Similar deals request
            deal_hash = data[4:]
            record = self.data_mgr.get_deal_by_hash(deal_hash)
            if not record:
                self.send_message(chat_id, "⚠️ Deal record not found.")
                return

            similar = self.data_mgr.get_similar_deals(record, limit=3)
            if not similar:
                self.send_message(chat_id, "No directly comparable deals found in recent log.")
                return

            lines = [f"<b>📊 SIMILAR TRANSACTIONS</b> (matching {record.get('primary_region')} / {record.get('signal_type')}):\n"]
            for idx, s in enumerate(similar, 1):
                h = html.escape(str(s.get("headline", "")))
                d = str(s.get("published_date", ""))
                sz = s.get("size_disclosed", "Undisclosed")
                lines.append(f"<b>{idx}. {h}</b>\n   <i>{d}</i> | 💵 {sz}")
            self.send_message(chat_id, "\n\n".join(lines))

    # ==============================================================================
    # Message Routing (Commands & Gemini Q&A)
    # ==============================================================================

    def handle_message(self, message: Dict[str, Any]) -> None:
        """Route incoming user messages to slash command handlers or Gemini Q&A."""
        chat_id = message.get("chat", {}).get("id")
        text = message.get("text", "").strip()

        if not text:
            return

        if not self.is_authorized(chat_id):
            logger.warning(f"Ignored message from unauthorized chat_id: {chat_id}")
            return

        cmd = text.split()[0].lower() if text else ""

        if cmd in ["/start", "/help"]:
            self.handle_start_or_help(chat_id)
        elif cmd == "/latest":
            self.handle_latest(chat_id, text)
        elif cmd == "/search":
            self.handle_search(chat_id, text)
        elif cmd == "/deals":
            self.handle_deals(chat_id, text)
        elif cmd == "/exits":
            self.handle_exits(chat_id)
        elif cmd == "/letters":
            self.handle_letters(chat_id)
        elif cmd == "/stats":
            self.handle_stats(chat_id)
        elif cmd == "/status":
            self.handle_status(chat_id)
        elif cmd == "/scan":
            self.handle_scan(chat_id)
        elif cmd == "/digest":
            self.handle_digest(chat_id)
        elif cmd == "/export":
            self.handle_export(chat_id)
        elif cmd in ["/wire", "/benzinga"]:
            self.handle_wire(chat_id, text)
        else:
            # Natural Language Q&A grounded in master log via Gemini
            self.send_message(chat_id, "🤔 <i>Analyzing Brookfield deal dataset...</i>")
            answer = self.assistant.answer_general_question(text)
            self.send_message(chat_id, answer)

    # ==============================================================================
    # Polling & Daemon Execution
    # ==============================================================================

    def start_background_watcher(self) -> None:
        """Periodically run the monitoring cycle in a background worker thread."""
        def watcher_loop():
            logger.info(f"Background monitoring watcher active. Sweep interval: {self.poll_interval}s.")
            # Initial sleep before first scheduled sweep
            time.sleep(10)
            while not self.stop_event.is_set():
                if not self.is_scanning:
                    logger.info("Executing scheduled periodic monitoring sweep...")
                    self.is_scanning = True
                    try:
                        run_monitoring_cycle(
                            pipeline=self.pipeline,
                            notifier=self.notifier,
                            sleep_interval=RATE_LIMIT_SLEEP_SECONDS,
                        )
                        self.last_scan_time = datetime.now(timezone.utc)
                    except Exception as e:
                        logger.error(f"Error in background watcher cycle: {e}")
                    finally:
                        self.is_scanning = False

                # Sleep in increments of 30 seconds and check scheduled morning digest (8:00 AM)
                elapsed = 0
                while elapsed < self.poll_interval and not self.stop_event.is_set():
                    time.sleep(30)
                    elapsed += 30
                    now = datetime.now()
                    today_str = now.strftime("%Y-%m-%d")
                    if now.hour == self.digest_hour and self.last_digest_date != today_str:
                        if self.authorized_chat_id:
                            logger.info(f"Triggering scheduled daily morning briefing for {today_str} at 08:00...")
                            digest_text = self.generate_morning_digest()
                            self.send_message(self.authorized_chat_id, digest_text)
                            self.last_digest_date = today_str

        t = threading.Thread(target=watcher_loop, daemon=True)
        t.start()

    def run_polling_loop(self) -> None:
        """Continuous long polling loop for Telegram updates."""
        if not self.enabled:
            logger.error("Telegram credentials missing or unconfigured. Exiting bot loop.")
            return

        logger.info("Starting Telegram Long Polling listener for @BAMIntelligence_bot...")
        offset = 0

        # Start the background sweep thread
        self.start_background_watcher()

        while not self.stop_event.is_set():
            try:
                params = {"offset": offset, "timeout": 25}
                resp = requests.get(f"{self.api_url}/getUpdates", params=params, timeout=30)
                data = resp.json()

                if not data.get("ok"):
                    logger.error(f"Telegram getUpdates error: {data.get('description')}")
                    time.sleep(5)
                    continue

                updates = data.get("result", [])
                for update in updates:
                    offset = update["update_id"] + 1

                    if "message" in update:
                        self.handle_message(update["message"])
                    elif "callback_query" in update:
                        self.handle_callback_query(update["callback_query"])

            except requests.exceptions.Timeout:
                # Normal timeout for long-polling
                continue
            except Exception as e:
                logger.error(f"Polling loop exception: {e}")
                time.sleep(3)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Interactive Two-Way Telegram Bot for Brookfield Private Equity Intelligence."
    )
    parser.add_argument(
        "--datastore",
        type=str,
        default=DEFAULT_LOG_CSV,
        help=f"Path to master CSV datastore (default: {DEFAULT_LOG_CSV})",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Background sweep interval in seconds (default: 3600s / 1 hour)",
    )
    parser.add_argument(
        "--poll-only",
        action="store_true",
        help="Run conversational Telegram listener only, without background scheduled sweeps",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Test query engine and Gemini Q&A locally without starting long polling",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.test_mode:
        logger.info("Running Interactive Bot in local test mode...")
        data_mgr = DealDataManager(csv_path=args.datastore)
        stats = data_mgr.get_stats()
        logger.info(f"Loaded datastore with {stats.get('total_events')} records.")
        latest = data_mgr.get_latest_deals(limit=2)
        logger.info(f"Latest 2 deals: {[d.get('headline') for d in latest]}")

        assistant = ConversationalAssistant(data_mgr=data_mgr)
        logger.info("Testing Gemini Q&A grounding...")
        ans = assistant.answer_general_question("What did Brookfield acquire in data centers?")
        logger.info(f"Q&A Answer snippet:\n{ans[:200]}...")
        logger.info("Test mode completed successfully.")
        return

    bot = InteractiveTelegramBot(
        datastore_path=args.datastore,
        poll_interval_seconds=args.interval,
    )

    if args.poll_only:
        logger.info("Starting in poll-only mode (background sweep disabled)...")
        bot.run_polling_loop()
    else:
        bot.run_polling_loop()


if __name__ == "__main__":
    main()
