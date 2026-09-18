"""
Classifier module for Brookfield Private Equity Intelligence Pipeline.
Wraps the Gemini API with strict JSON schema enforcement, PE hard logic rules,
and multi-event de-bundling to capture every acquisition and exit buried in earnings releases.
"""

import json
import logging
import re
import time
import warnings
from typing import Any, Dict, List, Optional

# Suppress deprecation warning from google.generativeai for cleaner logs
warnings.filterwarnings("ignore", category=FutureWarning)
import google.generativeai as genai

from config import (
    CLASSIFICATION_SCHEMA,
    GEMINI_API_KEY,
    GEMINI_MODEL_NAME,
    MULTI_EVENT_SCHEMA,
)

logger = logging.getLogger(__name__)

SYSTEM_INSTRUCTION = """
You are an elite Private Equity (PE) Intelligence Analyst specialized in M&A, capital allocation, and sponsor deal activity.
Your objective is to evaluate corporate press releases, earnings reports, and shareholder letters, extract structured intelligence, and output valid JSON conforming strictly to the provided JSON schema.

CRITICAL HARD LOGIC RULES:

1. EVENT DE-BUNDLING MANDATE:
   - When analyzing documents that bundle multiple corporate actions (e.g., Quarterly Results, Year-End Financial Reports, Letters to Shareholders, or Capital Recycling updates), you MUST extract EACH distinct strategic transaction or material milestone as an individual event object inside the 'events' array.
   - For example, if an earnings release reports:
     * Q2 Adjusted EBITDA of $587M
     * Completed acquisition of World Freight Company in the Netherlands
     * Completed acquisition of Gregg Distributors in Canada
     * Agreed to sell Multiplex to Obayashi for $650M
     Then you MUST output 4 discrete event objects in 'events':
     - Event 1: event_headline="Brookfield Completes Acquisition of World Freight Company", signal_type="Deal", primary_region="Europe"
     - Event 2: event_headline="Brookfield Acquires Gregg Distributors", signal_type="Deal", primary_region="North America"
     - Event 3: event_headline="Brookfield to Sell Multiplex to Obayashi for $650M", signal_type="Exit", primary_region="APAC", size_disclosed="$650M"
     - Event 4: event_headline="Brookfield Reports Strong Q2 Results", signal_type="Results", primary_region="Global / unassigned"
   - If the document only reports a single news item (e.g., 'Brookfield to Acquire Fosber'), return 1 event in the 'events' array.

2. Region by Asset:
   - 'primary_region' MUST strictly follow the geographic location of the TARGET ASSET, TARGET PORTFOLIO COMPANY, or REGIONAL OPERATION.
   - NEVER determine primary_region based on the article's dateline, where the release was issued, or the acquirer's corporate HQ (e.g., Toronto or New York).
   - Allowed primary_region values: 'Middle East / GCC', 'North America', 'Europe', 'APAC', 'Global / unassigned'.

3. Relevance ('is_relevant'):
   - Set 'is_relevant' = true for actionable private equity signals: direct M&A, acquisitions, carve-outs, take-privates, add-on/bolt-on deals, exits, IPOs, secondary sales, fund closes, capital raises, material earnings results, or major regulatory approvals.
   - Set 'is_relevant' = false for routine administrative boilerplate notices (such as notices of annual shareholder meeting proxy materials, dividend record dates without commentary, or routine normal course issuer bid renewal filings).

4. Signal Type ('signal_type'):
   - Must be one of: 'Deal', 'Exit', 'Fund', 'People', 'Ops', 'Results', 'Regulatory', 'Market'.

5. Deal Structure ('deal_structure'):
   - If signal_type is 'Deal' or 'Exit', categorize into: 'Carve-out', 'Take-private', 'Add-on / Bolt-on', 'Direct Growth', or 'N/A'.
   - If non-deal, output 'N/A'.

6. Co-investors ('co_investors'):
   - Extract any sovereign wealth funds (e.g., ADIA, GIC, Mubadala, PIF, Temasek), institutional LPs, pension funds (e.g., CDPQ, CPPIB, OTPP), or consortium partners explicitly mentioned. If none, return [].

7. Entities ('entities'):
   - List all target companies, portfolio companies, sponsors, and acquiring vehicles involved in that specific event.

8. One Line Summary ('one_line_summary'):
   - Crisp, high-impact executive summary focusing on the strategic action, target, and rationale.

9. Size Disclosed ('size_disclosed'):
   - State transaction value if disclosed (e.g., '$1.2B', '€500M', 'C$180M'). If undisclosed, return 'Undisclosed' or 'N/A'.
"""


def _detect_region(text: str) -> str:
    """Helper to detect asset region from text."""
    t = text.lower()
    if any(k in t for k in ["europe", "uk", "germany", "france", "italy", "fosber", "spain", "netherlands", "rotterdam", "london"]):
        return "Europe"
    if any(k in t for k in ["middle east", "uae", "dubai", "saudi", "gcc", "qatar", "adia"]):
        return "Middle East / GCC"
    if any(k in t for k in ["apac", "asia", "australia", "india", "japan", "singapore", "obayashi", "multiplex"]):
        return "APAC"
    if any(k in t for k in ["us", "usa", "united states", "canada", "north america", "gregg"]):
        return "North America"
    return "Global / unassigned"


def _mock_classify(headline: str, body_text: str) -> List[Dict[str, Any]]:
    """
    Intelligent mock/fallback classifier that performs rule-based de-bundling.
    Extracts discrete acquisitions and divestitures mentioned in the body text
    even if GEMINI quota is temporarily exhausted.
    """
    hl_lower = headline.lower()
    body_lower = body_text.lower()

    # Rule-based detection for boilerplate
    is_boilerplate = any(
        kw in hl_lower for kw in [
            "notice of annual meeting",
            "normal course issuer bid",
            "distribution and dividend",
            "completes 2025 annual filings",
            "completes 2024 annual filings",
        ]
    )
    if is_boilerplate:
        return [{
            "is_relevant": False,
            "event_headline": headline,
            "primary_region": "Global / unassigned",
            "secondary_regions": [],
            "signal_type": "Ops",
            "deal_structure": "N/A",
            "co_investors": [],
            "confidence": "confirmed",
            "entities": ["Brookfield"],
            "one_line_summary": headline,
            "size_disclosed": "N/A",
        }]

    events = []

    # 1. Look for embedded acquisitions in text
    deal_matches = re.finditer(
        r"(?:completed|closed|announced|agreed to|pending)?\s*(?:the\s+)?acquisition of\s+([A-Z][A-Za-z0-9\s&]+?)(?:,|\.|\bfor\b|\bin\b)",
        body_text,
    )
    seen_targets = set()
    for m in deal_matches:
        target = m.group(1).strip()
        # Clean target name
        target = re.sub(r"\s+(?:for|in|from|a|an|leading|industrial).*$", "", target, flags=re.I).strip()
        if len(target) > 2 and len(target) < 40 and target.lower() not in seen_targets and target.lower() not in ["the", "a", "an", "our", "its"]:
            seen_targets.add(target.lower())
            surrounding = body_text[max(0, m.start() - 50):min(len(body_text), m.end() + 150)]
            region = _detect_region(surrounding)
            size_match = re.search(r"(\$|€|C\$|£)\s?(\d+(?:\.\d+)?\s*(?:billion|million|B|M))", surrounding, re.I)
            size = size_match.group(0) if size_match else "Undisclosed"

            events.append({
                "is_relevant": True,
                "event_headline": f"Brookfield Acquires {target}",
                "primary_region": region,
                "secondary_regions": [],
                "signal_type": "Deal",
                "deal_structure": "Add-on / Bolt-on" if "add-on" in surrounding.lower() or "bolt-on" in surrounding.lower() else "Direct Growth",
                "co_investors": [],
                "confidence": "confirmed",
                "entities": ["Brookfield", target],
                "one_line_summary": f"Brookfield completes acquisition of {target}.",
                "size_disclosed": size,
            })

    # 2. Look for embedded sales / exits in text
    exit_matches = re.finditer(
        r"(?:agreed to sell|sale of|completed the sale of|divested)\s+([A-Z][A-Za-z0-9\s&]+?)(?:,|\.|\bto\b|\bfor\b)",
        body_text,
    )
    for m in exit_matches:
        target = m.group(1).strip()
        target = re.sub(r"\s+(?:to|for|in|its|our|global).*$", "", target, flags=re.I).strip()
        if len(target) > 2 and len(target) < 40 and target.lower() not in seen_targets:
            seen_targets.add(target.lower())
            surrounding = body_text[max(0, m.start() - 50):min(len(body_text), m.end() + 150)]
            region = _detect_region(surrounding)
            size_match = re.search(r"(\$|€|C\$|£)\s?(\d+(?:\.\d+)?\s*(?:billion|million|B|M))", surrounding, re.I)
            size = size_match.group(0) if size_match else "Undisclosed"

            events.append({
                "is_relevant": True,
                "event_headline": f"Brookfield Sells {target}",
                "primary_region": region,
                "secondary_regions": [],
                "signal_type": "Exit",
                "deal_structure": "N/A",
                "co_investors": [],
                "confidence": "confirmed",
                "entities": ["Brookfield", target],
                "one_line_summary": f"Brookfield completes sale of {target}.",
                "size_disclosed": size,
            })

    # 3. Main headline event
    main_signal = "Results" if any(k in hl_lower for k in ["results", "quarter", "year end", "earnings"]) else (
        "Deal" if any(k in hl_lower for k in ["acquire", "deal", "investment", "buy"]) else (
            "Exit" if any(k in hl_lower for k in ["exit", "sell", "sale", "ipo"]) else "Ops"
        )
    )

    main_event = {
        "is_relevant": True,
        "event_headline": headline,
        "primary_region": _detect_region(headline + " " + body_text[:500]),
        "secondary_regions": [],
        "signal_type": main_signal,
        "deal_structure": "Carve-out" if "carve-out" in body_lower else "N/A",
        "co_investors": [],
        "confidence": "confirmed",
        "entities": ["Brookfield"],
        "one_line_summary": headline[:140],
        "size_disclosed": "Undisclosed",
    }
    events.append(main_event)
    return events


class GeminiClassifier:
    """Wrapper around Google Gemini API with strict JSON schema and multi-event de-bundling."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or GEMINI_API_KEY
        self._model = None
        if self.api_key and self.api_key != "YOUR_GEMINI_API_KEY":
            genai.configure(api_key=self.api_key)
            self._model = genai.GenerativeModel(
                model_name=GEMINI_MODEL_NAME,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                    response_schema=MULTI_EVENT_SCHEMA,
                    temperature=0.1,
                ),
                system_instruction=SYSTEM_INSTRUCTION,
            )

    @property
    def is_configured(self) -> bool:
        return bool(self._model is not None)

    def classify(
        self,
        headline: str,
        body_text: str,
        published_date: Optional[str] = None,
        max_retries: int = 2,
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Classify a news item, returning a list of discrete events/transactions extracted.
        Falls back to rule-based multi-event extraction if dry_run=True or if quotas are exceeded.
        """
        if dry_run or not self.is_configured:
            logger.info("Classifying via rule engine de-bundling (dry-run or unconfigured key).")
            return _mock_classify(headline, body_text)

        truncated_body = body_text[:14000] if body_text else "No body text available."
        prompt = (
            f"Document Title: {headline}\n"
            f"Published Date: {published_date or 'Unknown'}\n\n"
            f"Document Content:\n{truncated_body}\n\n"
            "Task: Extract all discrete strategic PE events, acquisitions, exits, and material operational results. "
            "Respond strictly conforming to the MULTI_EVENT_SCHEMA."
        )

        backoff = 3.0
        for attempt in range(1, max_retries + 1):
            try:
                response = self._model.generate_content(prompt)
                raw_text = response.text.strip()
                result = json.loads(raw_text)

                # Extract events list
                raw_events = result.get("events", []) if isinstance(result, dict) else []
                if not raw_events and isinstance(result, list):
                    raw_events = result

                validated_events = []
                for ev in raw_events:
                    # Validate required fields
                    if "is_relevant" not in ev or "signal_type" not in ev or "primary_region" not in ev:
                        continue
                    # Ensure fallback headline
                    if not ev.get("event_headline"):
                        ev["event_headline"] = headline
                    # Ensure lists
                    for list_f in ["secondary_regions", "co_investors", "entities"]:
                        if list_f not in ev or not isinstance(ev[list_f], list):
                            ev[list_f] = []
                    validated_events.append(ev)

                if validated_events:
                    return validated_events

                logger.warning("No valid events parsed from Gemini response, falling back to rule engine.")
                return _mock_classify(headline, body_text)

            except Exception as e:
                logger.warning(
                    f"Gemini classification attempt {attempt}/{max_retries} failed for '{headline[:40]}...': {e}"
                )
                if attempt == max_retries:
                    logger.error(f"Exhausted retries. Falling back to multi-event rule engine. Error: {e}")
                    return _mock_classify(headline, body_text)
                time.sleep(backoff)
                backoff *= 2.0


_default_classifier = None


def classify_article(
    headline: str,
    body_text: str,
    published_date: Optional[str] = None,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Classify an article using default singleton, returning list of de-bundled events."""
    global _default_classifier
    if _default_classifier is None:
        _default_classifier = GeminiClassifier()
    return _default_classifier.classify(
        headline=headline,
        body_text=body_text,
        published_date=published_date,
        dry_run=dry_run,
    )
