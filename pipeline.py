"""
Pipeline orchestrator for Brookfield Private Equity Intelligence Pipeline.
Manages deduplication across a 3-day rolling window, passes novel items to the classifier,
and appends structured intelligence to the CSV datastore.
"""

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import dateutil.parser
import pandas as pd

from classifier import GeminiClassifier, classify_article
from config import (
    DEDUPLICATION_WINDOW_DAYS,
    DEFAULT_LOG_CSV,
)

logger = logging.getLogger(__name__)

# Canonical columns for the CSV datastore
DATASTORE_COLUMNS = [
    "published_date",
    "headline",
    "source_url",
    "primary_region",
    "secondary_regions",
    "signal_type",
    "deal_structure",
    "co_investors",
    "confidence",
    "entities",
    "one_line_summary",
    "size_disclosed",
    "created_at",
]

# Words to ignore when extracting entity/subject tokens from headlines for pre-filter
STOP_TOKENS = {
    "brookfield", "business", "partners", "corporation", "reports", "announces",
    "to", "and", "the", "a", "an", "of", "in", "on", "for", "with", "by", "from",
    "strong", "first", "second", "third", "fourth", "quarter", "results", "year",
    "end", "declares", "host", "conference", "call", "completes", "meeting",
    "press", "release", "news", "today", "inc", "ltd", "corp", "llc", "lp"
}


def _extract_headline_core_tokens(headline: str) -> Set[str]:
    """Extract informative non-stopword tokens from headline for duplicate matching."""
    words = re.findall(r"\b[A-Za-z0-9\-\']+\b", headline.lower())
    return {w for w in words if len(w) > 2 and w not in STOP_TOKENS}


def _parse_item_date(date_val: Any) -> Optional[datetime]:
    """Safely parse various date formats into a timezone-naive datetime for uniform comparison."""
    if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
        return None
    if isinstance(date_val, datetime):
        return date_val.replace(tzinfo=None)
    try:
        dt = dateutil.parser.parse(str(date_val))
        return dt.replace(tzinfo=None)
    except Exception:
        return None


class Pipeline:
    """Orchestrates ingestion, 3-day rolling window deduplication, and datastore updates."""

    def __init__(self, datastore_path: str = DEFAULT_LOG_CSV, classifier: Optional[GeminiClassifier] = None):
        self.datastore_path = datastore_path
        self.classifier = classifier or GeminiClassifier()
        self.df = self.load_datastore()

    def load_datastore(self) -> pd.DataFrame:
        """Load existing CSV datastore or initialize an empty DataFrame."""
        if os.path.exists(self.datastore_path):
            try:
                df = pd.read_csv(self.datastore_path)
                # Ensure required columns exist
                for col in DATASTORE_COLUMNS:
                    if col not in df.columns:
                        df[col] = ""
                # Parse dates for rolling window queries
                df["_parsed_date"] = df["published_date"].apply(_parse_item_date)
                logger.info(f"Loaded {len(df)} records from {self.datastore_path}")
                return df
            except Exception as e:
                logger.error(f"Error loading {self.datastore_path}: {e}. Creating fresh datastore.")

        df = pd.DataFrame(columns=DATASTORE_COLUMNS)
        df["_parsed_date"] = pd.Series(dtype="object")
        return df

    def is_pre_duplicate(self, headline: str, published_date: Optional[datetime], url: str) -> Tuple[bool, str]:
        """
        Check before calling Gemini:
        1. Exact URL match in datastore.
        2. 3-day rolling window check for identical or matching core subject tokens.
        Keep the earliest primary source.
        """
        if self.df.empty:
            return False, ""

        # 1. Exact URL match
        if url and (self.df["source_url"] == url).any():
            return True, f"Exact URL already logged: {url}"

        # If date is missing, can't check rolling window
        cand_date = _parse_item_date(published_date)
        if not cand_date:
            return False, ""

        cand_tokens = _extract_headline_core_tokens(headline)
        if not cand_tokens:
            return False, ""

        # 2. Filter datastore items within candidate_date +/- 3 days
        window_days = DEDUPLICATION_WINDOW_DAYS
        mask = self.df["_parsed_date"].apply(
            lambda d: bool(d and abs((d - cand_date).days) <= window_days)
        )
        window_df = self.df[mask]

        if window_df.empty:
            return False, ""

        for _, row in window_df.iterrows():
            row_tokens = _extract_headline_core_tokens(str(row.get("headline", "")))
            # Check overlap
            overlap = cand_tokens.intersection(row_tokens)
            # If significant entity/subject overlap (>= 2 core tokens or high Jaccard index)
            if len(overlap) >= 2 or (overlap and len(overlap) / max(len(cand_tokens), len(row_tokens)) >= 0.5):
                existing_date = row.get("_parsed_date")
                # If existing is earlier or equal, drop candidate (keep earliest source)
                if existing_date and existing_date <= cand_date:
                    return True, f"Matched existing story '{row.get('headline')}' within {window_days}-day window"

        return False, ""

    def is_post_duplicate(
        self,
        candidate_entities: List[str],
        candidate_signal: str,
        published_date: Optional[datetime],
    ) -> Tuple[bool, str]:
        """
        Check after Gemini extracts entities & signal_type:
        Check if an item with the same core entities and action/signal_type
        exists within a 3-day rolling window.
        """
        if self.df.empty:
            return False, ""

        cand_date = _parse_item_date(published_date)
        if not cand_date:
            return False, ""

        # Filter out generic terms from entities
        clean_cand_entities = {
            e.strip().lower() for e in candidate_entities
            if e.strip().lower() not in ["brookfield", "business partners", "corporation", "bn", "bbu"]
        }
        if not clean_cand_entities:
            return False, ""

        window_days = DEDUPLICATION_WINDOW_DAYS
        mask = self.df["_parsed_date"].apply(
            lambda d: bool(d and abs((d - cand_date).days) <= window_days)
        )
        window_df = self.df[mask]

        for _, row in window_df.iterrows():
            # Check matching signal type
            if str(row.get("signal_type", "")).strip().lower() == candidate_signal.strip().lower():
                # Check entity overlap
                raw_entities = str(row.get("entities", "")).split(";")
                existing_entities = {
                    e.strip().lower() for e in raw_entities
                    if e.strip().lower() not in ["brookfield", "business partners", "corporation", "bn", "bbu"]
                }
                if clean_cand_entities.intersection(existing_entities):
                    existing_date = row.get("_parsed_date")
                    if existing_date and existing_date <= cand_date:
                        return True, f"Duplicate entity {clean_cand_entities.intersection(existing_entities)} and signal {candidate_signal} within {window_days} days"

        return False, ""

    def save_item(self, item: Dict[str, Any], classified: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten JSON output and append to datastore."""
        pub_date = item.get("published_date")
        date_str = item.get("date_str")
        if not date_str and pub_date:
            date_str = pub_date.strftime("%Y-%m-%d")

        # Specific event headline if de-bundled, otherwise article headline
        event_headline = classified.get("event_headline") or item.get("headline", "")

        # Flatten array fields into clean semicolon-delimited strings
        secondary_regions = "; ".join(classified.get("secondary_regions") or [])
        co_investors = "; ".join(classified.get("co_investors") or [])
        entities = "; ".join(classified.get("entities") or [])

        record = {
            "published_date": date_str or "",
            "headline": event_headline,
            "source_url": item.get("url", ""),
            "primary_region": classified.get("primary_region", "Global / unassigned"),
            "secondary_regions": secondary_regions,
            "signal_type": classified.get("signal_type", "Ops"),
            "deal_structure": classified.get("deal_structure", "N/A"),
            "co_investors": co_investors,
            "confidence": classified.get("confidence", "confirmed"),
            "entities": entities,
            "one_line_summary": classified.get("one_line_summary", ""),
            "size_disclosed": classified.get("size_disclosed", "Undisclosed"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "_parsed_date": _parse_item_date(pub_date or date_str),
        }

        # Append to in-memory DataFrame
        record_df = pd.DataFrame([record])
        self.df = pd.concat([self.df, record_df], ignore_index=True)

        # Write to CSV (excluding internal _parsed_date)
        export_df = self.df[[c for c in DATASTORE_COLUMNS if c in self.df.columns]]
        export_df.to_csv(self.datastore_path, index=False)
        logger.info(f"Saved novel intelligence to datastore: {event_headline[:50]}")
        return record

    def process_item(
        self,
        item: Dict[str, Any],
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Process a single candidate document through the pipeline:
        1. Pre-Gemini deduplication on the document level.
        2. Gemini classification with multi-event de-bundling.
        3. For each extracted event:
           - Drop silently if is_relevant == False.
           - Check 3-day rolling window event-level deduplication.
           - Append novel event to datastore.
        """
        headline = item.get("headline", "")
        pub_date = item.get("published_date")
        url = item.get("url", "")
        body_text = item.get("body_text", "")

        # 1. Document-level exact URL duplicate check (if already fully processed)
        if url and not self.df.empty and (self.df["source_url"] == url).any() and not item.get("is_shareholder_letter"):
            # Check if at least one event from this URL already exists
            logger.info(f"[DEDUPLICATED URL] Already processed: {url}")
            return {"status": "duplicate", "reason": "URL already processed", "item": item}

        # 2. Extract events (Gemini or de-bundling rule engine)
        events = self.classifier.classify(
            headline=headline,
            body_text=body_text,
            published_date=item.get("date_str") or (pub_date.strftime("%Y-%m-%d") if pub_date else None),
            dry_run=dry_run,
        )

        saved_records = []
        dropped_count = 0
        duplicate_count = 0

        for ev in events:
            # 3. Drop silently if not relevant
            if not ev.get("is_relevant", False):
                dropped_count += 1
                continue

            ev_headline = ev.get("event_headline") or headline

            # 4. Check event-level deduplication within 3-day window
            is_dup, dup_reason = self.is_pre_duplicate(ev_headline, pub_date, "")
            if not is_dup:
                is_post_dup, post_reason = self.is_post_duplicate(
                    ev.get("entities", []),
                    ev.get("signal_type", ""),
                    pub_date,
                )
                if is_post_dup:
                    is_dup = True
                    dup_reason = post_reason

            if is_dup:
                duplicate_count += 1
                logger.info(f"[DEDUPLICATED EVENT] Skipping '{ev_headline[:40]}...': {dup_reason}")
                continue

            # 5. Save novel event
            rec = self.save_item(item, ev)
            saved_records.append(rec)

        if saved_records:
            return {"status": "saved", "saved_records": saved_records, "count": len(saved_records)}
        elif duplicate_count > 0:
            return {"status": "duplicate", "reason": "All events were duplicates"}
        else:
            return {"status": "dropped_irrelevant"}
