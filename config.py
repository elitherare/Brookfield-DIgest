"""
Configuration settings for the Brookfield Private Equity Intelligence Pipeline.
"""

from api_manager import (
    get_benzinga_api_key,
    get_gemini_api_key,
    get_telegram_credentials,
    is_benzinga_configured,
    is_gemini_configured,
    is_telegram_configured,
)

# ==============================================================================
# API Keys and Credentials (Loaded via api_manager from .env or environment)
# ==============================================================================
GEMINI_API_KEY = get_gemini_api_key()
_telegram_creds = get_telegram_credentials()
TELEGRAM_BOT_TOKEN = _telegram_creds["bot_token"]
TELEGRAM_CHAT_ID = _telegram_creds["chat_id"]
BENZINGA_API_KEY = get_benzinga_api_key()

# ==============================================================================
# Benzinga News API Configuration (Massive.com)
# ==============================================================================
BENZINGA_BASE_URL = "https://api.massive.com/benzinga/v2/news"
BENZINGA_TICKERS = ["BAM", "BN", "BBU", "BIP", "BEP"]
BENZINGA_FETCH_LIMIT = 10

# ==============================================================================
# Gemini Model Configuration
# ==============================================================================
GEMINI_MODEL_NAME = "gemini-flash-latest"

# Sleep time in seconds between Gemini API calls to respect rate limits
# Free tier allows 15 RPM (4.0s interval is a safe baseline)
RATE_LIMIT_SLEEP_SECONDS = 4.0

# ==============================================================================
# Target URLs & Data Sources
# ==============================================================================
# Primary Brookfield Business Partners (BBU/BBUC) newsroom
BROOKFIELD_BBU_NEWSROOM_URL = "https://bbuc.brookfield.com/news-events/press-releases"
# Legacy URL that redirects to bbuc:
BROOKFIELD_BBU_LEGACY_URL = "https://bbu.brookfield.com/press-releases"

# Brookfield Letters to Shareholders (CEO quarterly letters detailing bolt-ons and portfolio operations)
BROOKFIELD_LETTERS_URL = "https://bbuc.brookfield.com/reports-filings/letters-shareholders"

# Corporate Brookfield newsroom (aggregates BAM, BBU, BEP, BIP, BN)
BROOKFIELD_MAIN_NEWSROOM_URL = "https://www.brookfield.com/views-news/press-releases"

# Standard RSS Feeds for monitoring
RSS_FEEDS = [
    # Google News RSS for Brookfield Business Partners
    "https://news.google.com/rss/search?q=Brookfield+Business+Partners+when:7d&hl=en-US&gl=US&ceid=US:en",
    # Google News RSS for Brookfield Private Equity
    "https://news.google.com/rss/search?q=Brookfield+%22private+equity%22+when:7d&hl=en-US&gl=US&ceid=US:en",
]

# Standard request headers to avoid Cloudflare/Drupal 403 blocks
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# ==============================================================================
# Backfill & Pipeline Defaults
# ==============================================================================
BACKFILL_MONTHS = 24
DEDUPLICATION_WINDOW_DAYS = 3
DEFAULT_LOG_CSV = "brookfield_24mo_log.csv"

# ==============================================================================
# Single-Event Strict Gemini Classification Schema
# ==============================================================================
CLASSIFICATION_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "is_relevant": {"type": "BOOLEAN"},
        "primary_region": {
            "type": "STRING",
            "enum": [
                "Middle East / GCC",
                "North America",
                "Europe",
                "APAC",
                "Global / unassigned",
            ],
        },
        "secondary_regions": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "signal_type": {
            "type": "STRING",
            "enum": [
                "Deal",
                "Exit",
                "Fund",
                "People",
                "Ops",
                "Results",
                "Regulatory",
                "Market",
            ],
        },
        "deal_structure": {
            "type": "STRING",
            "enum": [
                "Carve-out",
                "Take-private",
                "Add-on / Bolt-on",
                "Direct Growth",
                "N/A",
            ],
        },
        "co_investors": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
            "description": "Extract any sovereign wealth funds, LPs, or consortium partners mentioned.",
        },
        "confidence": {
            "type": "STRING",
            "enum": ["confirmed", "reported"],
        },
        "entities": {
            "type": "ARRAY",
            "items": {"type": "STRING"},
        },
        "one_line_summary": {"type": "STRING"},
        "size_disclosed": {"type": "STRING"},
    },
    "required": [
        "is_relevant",
        "primary_region",
        "signal_type",
        "confidence",
        "one_line_summary",
    ],
}

# ==============================================================================
# Multi-Event Gemini Classification Schema (For De-bundling Multi-Deal Releases)
# ==============================================================================
MULTI_EVENT_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "events": {
            "type": "ARRAY",
            "description": (
                "List of discrete strategic transactions, acquisitions, add-ons, divestitures, exits, "
                "fund closes, or material corporate results mentioned in the document. "
                "If multiple distinct acquisitions or sales are mentioned in an earnings release or letter, "
                "extract EACH transaction as its own individual event object."
            ),
            "items": {
                "type": "OBJECT",
                "properties": {
                    "is_relevant": {"type": "BOOLEAN"},
                    "event_headline": {
                        "type": "STRING",
                        "description": "Specific headline for this discrete deal or event (e.g. 'Brookfield Acquires World Freight Company')",
                    },
                    "primary_region": {
                        "type": "STRING",
                        "enum": [
                            "Middle East / GCC",
                            "North America",
                            "Europe",
                            "APAC",
                            "Global / unassigned",
                        ],
                    },
                    "secondary_regions": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                    },
                    "signal_type": {
                        "type": "STRING",
                        "enum": [
                            "Deal",
                            "Exit",
                            "Fund",
                            "People",
                            "Ops",
                            "Results",
                            "Regulatory",
                            "Market",
                        ],
                    },
                    "deal_structure": {
                        "type": "STRING",
                        "enum": [
                            "Carve-out",
                            "Take-private",
                            "Add-on / Bolt-on",
                            "Direct Growth",
                            "N/A",
                        ],
                    },
                    "co_investors": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "Extract any sovereign wealth funds, LPs, or consortium partners mentioned.",
                    },
                    "confidence": {
                        "type": "STRING",
                        "enum": ["confirmed", "reported"],
                    },
                    "entities": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                    },
                    "one_line_summary": {"type": "STRING"},
                    "size_disclosed": {"type": "STRING"},
                },
                "required": [
                    "is_relevant",
                    "primary_region",
                    "signal_type",
                    "confidence",
                    "one_line_summary",
                ],
            },
        }
    },
    "required": ["events"],
}
