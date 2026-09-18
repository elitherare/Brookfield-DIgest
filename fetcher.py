"""
Fetcher module for Brookfield Private Equity Intelligence Pipeline.
Handles pulling paginated HTML from Brookfield's newsroom and standard RSS feeds.
"""

import logging
import re
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

import dateutil.parser
import feedparser
import requests
from bs4 import BeautifulSoup

from config import (
    BROOKFIELD_BBU_NEWSROOM_URL,
    BROOKFIELD_LETTERS_URL,
    BROOKFIELD_MAIN_NEWSROOM_URL,
    REQUEST_HEADERS,
)

logger = logging.getLogger(__name__)


def get_http_session() -> requests.Session:
    """Create and configure a requests session with default headers."""
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    return session


def fetch_html(url: str, timeout: int = 20) -> Optional[str]:
    """
    Fetch raw HTML from a URL with browser-like headers and redirect handling.
    """
    session = get_http_session()
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching URL {url}: {e}")
        return None


def fetch_brookfield_press_releases(
    page: int = 0,
    base_url: str = BROOKFIELD_BBU_NEWSROOM_URL,
    timeout: int = 20,
) -> List[Dict]:
    """
    Fetch a single page of press release listings from Brookfield's newsroom.
    Returns a list of dictionaries with headline, date, and detail URL.
    """
    separator = "&" if "?" in base_url else "?"
    page_url = f"{base_url}{separator}page={page}"
    logger.info(f"Fetching press release page {page}: {page_url}")

    html = fetch_html(page_url, timeout=timeout)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # 1. Check for standard BBU / BBUC Drupal view cards (.press-data-wrapper)
    cards = soup.select(".press-data-wrapper")
    if cards:
        for card in cards:
            # Extract date
            date_elem = card.select_one(".press-data-date span") or card.select_one(".press-data-date")
            date_str = date_elem.get_text(strip=True) if date_elem else ""

            # Extract title
            title_elem = card.select_one(".press-data-title")
            headline = title_elem.get_text(strip=True) if title_elem else ""

            # Extract link
            link_elem = card.select_one(".press-data-links a[href]")
            raw_url = link_elem["href"].strip() if link_elem else ""
            if not raw_url:
                continue

            # Ignore PDF direct links if another link exists
            if raw_url.endswith(".pdf") and card.select(".press-data-links a"):
                links = [a["href"].strip() for a in card.select(".press-data-links a") if not a["href"].endswith(".pdf")]
                if links:
                    raw_url = links[0]

            full_url = urljoin(page_url, raw_url)

            # Parse date to datetime object
            parsed_date = None
            if date_str:
                try:
                    parsed_date = dateutil.parser.parse(date_str)
                except Exception as e:
                    logger.warning(f"Could not parse date '{date_str}': {e}")

            if headline and full_url:
                items.append({
                    "headline": headline,
                    "date_str": date_str,
                    "published_date": parsed_date,
                    "url": full_url,
                })
        return items

    # 2. Check for Corporate Newsroom cards (.featured-card, .secondary-card, .tertiary-card)
    corp_cards = soup.select("a.featured-card, a.secondary-card, a.tertiary-card")
    for card in corp_cards:
        raw_url = card.get("href", "").strip()
        full_url = urljoin(page_url, raw_url)

        # Title is usually in an h3, h4, or bold span
        title_elem = card.select_one("h3, h4, .card-title, .title")
        headline = title_elem.get_text(strip=True) if title_elem else card.get_text(strip=True)

        # Date element
        date_elem = card.select_one(".date, time, .publish-date")
        date_str = date_elem.get_text(strip=True) if date_elem else ""

        parsed_date = None
        if date_str:
            try:
                parsed_date = dateutil.parser.parse(date_str)
            except Exception:
                pass

        if headline and full_url:
            items.append({
                "headline": headline,
                "date_str": date_str,
                "published_date": parsed_date,
                "url": full_url,
            })

    return items


def fetch_article_body(url: str, timeout: int = 20) -> str:
    """
    Fetch the article detail page and extract clean body text.
    Handles Brookfield's template shadow-root embedding, as well as standard article tags.
    """
    html = fetch_html(url, timeout=timeout)
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    # 1. Check if Brookfield embedded the content inside a <template> (shadow DOM)
    shadow_div = soup.select_one("#press-release-shadow")
    template = (shadow_div.find("template") if shadow_div else None) or soup.find("template")
    if template:
        # Strip internal style and script tags inside template
        for s in template.find_all(["style", "script"]):
            s.decompose()
        text = template.get_text(separator="\n", strip=True)
        # Clean up whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        if len(text) > 100:
            return text.strip()

    # 2. Otherwise remove script, style, navigation, header, footer elements
    for element in soup(["script", "style", "noscript", "nav", "header", "footer", "aside"]):
        element.decompose()

    # Find the primary content container
    article_container = (
        soup.find("article")
        or soup.select_one(".press-release-inner")
        or soup.select_one(".news-wraper__body")
        or soup.select_one(".field--name-body")
        or soup.select_one(".node__content")
        or soup.select_one(".content-wrapper")
        or soup.select_one("main")
    )

    target_soup = article_container if article_container else soup

    # Extract paragraphs or plain text
    paragraphs = []
    for p in target_soup.find_all(["p", "h1", "h2", "h3", "h4", "li"]):
        text = p.get_text(" ", strip=True)
        if text and len(text) > 20:
            paragraphs.append(text)

    if paragraphs:
        body = "\n\n".join(paragraphs)
    else:
        body = target_soup.get_text(separator="\n", strip=True)

    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def fetch_rss_feed(feed_url: str, timeout: int = 20) -> List[Dict]:
    """
    Fetch and parse a standard RSS / Atom feed using feedparser.
    Returns a standardized list of news items.
    """
    logger.info(f"Fetching RSS feed: {feed_url}")
    session = get_http_session()
    try:
        resp = session.get(feed_url, timeout=timeout)
        resp.raise_for_status()
        raw_xml = resp.text
    except Exception as e:
        logger.error(f"Failed to fetch RSS feed {feed_url}: {e}")
        return []

    feed = feedparser.parse(raw_xml)
    items = []

    for entry in feed.entries:
        headline = entry.get("title", "").strip()
        url = entry.get("link", "").strip()

        # Date parsing
        parsed_date = None
        date_str = entry.get("published", entry.get("updated", ""))
        if date_str:
            try:
                parsed_date = dateutil.parser.parse(date_str)
            except Exception:
                pass
        elif hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                parsed_date = datetime(*entry.published_parsed[:6])
                date_str = parsed_date.strftime("%b %d, %Y")
            except Exception:
                pass

        # Summary / Body
        body_text = entry.get("summary", entry.get("description", ""))
        if body_text:
            # Strip basic HTML tags from RSS summary
            body_soup = BeautifulSoup(body_text, "html.parser")
            body_text = body_soup.get_text(" ", strip=True)

        if headline and url:
            items.append({
                "headline": headline,
                "date_str": date_str,
                "published_date": parsed_date,
                "url": url,
                "body_text": body_text,
            })

    return items


def fetch_brookfield_shareholder_letters(
    base_url: str = BROOKFIELD_LETTERS_URL,
    timeout: int = 20,
) -> List[Dict]:
    """
    Fetch quarterly Letters to Shareholders / Unitholders.
    These long-form documents contain critical portfolio company bolt-ons and operational updates.
    """
    logger.info(f"Fetching Brookfield shareholder letters index: {base_url}")
    html = fetch_html(base_url, timeout=timeout)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    letters = []
    seen_urls = set()

    for a in soup.find_all("a"):
        href = a.get("href", "").strip()
        if "/bbu/reports-filings/letters-unitholders/" in href or "/bbu/reports-filings/letters-to-unitholders/" in href:
            full_url = urljoin(base_url, href)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Extract date from slug (e.g. bbu-q2-2025-letter-to-unitholders)
            date_match = re.search(r"(q[1-4])-(\d{4})", href.lower())
            parsed_date = None
            date_str = ""
            if date_match:
                q, yr = date_match.groups()
                q_months = {"q1": (3, 31), "q2": (6, 30), "q3": (9, 30), "q4": (12, 31)}
                m, d = q_months.get(q, (1, 1))
                parsed_date = datetime(int(yr), m, d)
                date_str = f"{q.upper()} {yr}"

            slug = href.split("/")[-1].replace("-", " ").title()
            headline = f"Brookfield Business Partners - {date_str} Letter to Unitholders" if date_str else slug
            letters.append({
                "headline": headline,
                "date_str": date_str,
                "published_date": parsed_date,
                "url": full_url,
                "is_shareholder_letter": True,
            })

    logger.info(f"Found {len(letters)} shareholder web letters")
    return letters

