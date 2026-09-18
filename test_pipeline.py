"""
Unit and integration tests for the Brookfield Private Equity Intelligence Pipeline.
"""

import os
import unittest
from datetime import datetime, timedelta

import pandas as pd

from classifier import GeminiClassifier, _mock_classify
from config import CLASSIFICATION_SCHEMA
from fetcher import (
    fetch_article_body,
    fetch_brookfield_press_releases,
    fetch_brookfield_shareholder_letters,
)
from pipeline import Pipeline


class TestBrookfieldPipeline(unittest.TestCase):
    """Test suite for pipeline components."""

    def setUp(self):
        self.test_csv = "test_pipeline_temp.csv"
        if os.path.exists(self.test_csv):
            os.remove(self.test_csv)
        self.pipeline = Pipeline(datastore_path=self.test_csv)

    def tearDown(self):
        if os.path.exists(self.test_csv):
            os.remove(self.test_csv)

    def test_classification_schema_structure(self):
        """Verify the JSON schema matches all required fields and enums."""
        self.assertEqual(CLASSIFICATION_SCHEMA["type"], "OBJECT")
        props = CLASSIFICATION_SCHEMA["properties"]
        self.assertIn("is_relevant", props)
        self.assertIn("primary_region", props)
        self.assertIn("signal_type", props)
        self.assertIn("deal_structure", props)
        self.assertIn("co_investors", props)
        self.assertIn("confidence", props)
        self.assertIn("entities", props)
        self.assertIn("one_line_summary", props)
        self.assertIn("size_disclosed", props)

        # Check required fields
        required = CLASSIFICATION_SCHEMA["required"]
        self.assertIn("is_relevant", required)
        self.assertIn("primary_region", required)
        self.assertIn("signal_type", required)
        self.assertIn("confidence", required)
        self.assertIn("one_line_summary", required)

        # Check enums
        regions = props["primary_region"]["enum"]
        self.assertIn("Middle East / GCC", regions)
        self.assertIn("North America", regions)
        self.assertIn("Europe", regions)
        self.assertIn("APAC", regions)
        self.assertIn("Global / unassigned", regions)

        signals = props["signal_type"]["enum"]
        for s in ["Deal", "Exit", "Fund", "People", "Ops", "Results", "Regulatory", "Market"]:
            self.assertIn(s, signals)

    def test_mock_classification(self):
        """Test mock classifier fallback for dry-run verification."""
        events = _mock_classify(
            headline="Brookfield to Acquire German Logistics Network",
            body_text="Brookfield agreed to acquire a leading logistics provider in Frankfurt, Germany.",
        )
        self.assertGreater(len(events), 0)
        res = events[0]
        self.assertTrue(res["is_relevant"])
        self.assertEqual(res["primary_region"], "Europe")
        self.assertEqual(res["signal_type"], "Deal")
        self.assertIn("Brookfield", res["entities"])

    def test_de_bundling_multi_event(self):
        """Test that releases mentioning multiple deals are de-bundled into separate records."""
        body = """
        Brookfield Business Corporation Reports Second Quarter 2026 Results.
        Adjusted EBITDA was $587 million.
        During the quarter, we completed the acquisition of World Freight Company, a European logistics company.
        In addition, we completed the acquisition of Gregg Distributors, an industrial distributor in Canada.
        We also agreed to sell Multiplex to Obayashi Corporation for $650 million.
        """
        item = {
            "headline": "Brookfield Business Corporation Reports Second Quarter 2026 Results",
            "published_date": datetime(2026, 7, 31),
            "date_str": "Jul 31, 2026",
            "url": "https://bbuc.brookfield.com/bbu/press-releases/q2-results-multi",
            "body_text": body,
        }
        res = self.pipeline.process_item(item, dry_run=True)
        self.assertEqual(res["status"], "saved")
        self.assertGreaterEqual(res["count"], 3)

        # Check datastore CSV
        df = pd.read_csv(self.test_csv)
        headlines = df["headline"].tolist()
        self.assertTrue(any("World Freight Company" in h for h in headlines))
        self.assertTrue(any("Gregg Distributors" in h for h in headlines))
        self.assertTrue(any("Multiplex" in h for h in headlines))

    def test_3_day_rolling_window_deduplication(self):
        """Test that stories covering the same deal within 3 days are deduplicated."""
        base_date = datetime(2025, 6, 10)
        item1 = {
            "headline": "Brookfield to Acquire Global Tech Services Co",
            "published_date": base_date,
            "date_str": "Jun 10, 2025",
            "url": "https://bbu.brookfield.com/press-releases/deal-announcement",
            "body_text": "Brookfield agreed to acquire Global Tech Services for $1.5B.",
        }
        res1 = self.pipeline.process_item(item1, dry_run=True)
        self.assertEqual(res1["status"], "saved")

        # Second source 2 days later (e.g., wire syndicated summary)
        item2 = {
            "headline": "Brookfield Closes Acquisition of Global Tech Services",
            "published_date": base_date + timedelta(days=2),
            "date_str": "Jun 12, 2025",
            "url": "https://wire.com/brookfield-closes-global-tech",
            "body_text": "Follow-up coverage on the Global Tech transaction.",
        }
        res2 = self.pipeline.process_item(item2, dry_run=True)
        self.assertEqual(res2["status"], "duplicate")

        # Third source 10 days later (beyond 3-day window, e.g. distinct update)
        item3 = {
            "headline": "Brookfield Business Partners Announces Quarterly Results",
            "published_date": base_date + timedelta(days=10),
            "date_str": "Jun 20, 2025",
            "url": "https://bbu.brookfield.com/press-releases/q2-results",
            "body_text": "Strong financial performance in Q2.",
        }
        res3 = self.pipeline.process_item(item3, dry_run=True)
        self.assertEqual(res3["status"], "saved")

        # Verify CSV has exactly 2 rows
        df = pd.read_csv(self.test_csv)
        self.assertEqual(len(df), 2)

    def test_silent_drop_of_irrelevant_items(self):
        """Verify that items with is_relevant=False are dropped silently without saving."""
        item = {
            "headline": "Notice of Annual Meeting of Shareholders",
            "published_date": datetime(2025, 5, 1),
            "date_str": "May 01, 2025",
            "url": "https://bbu.brookfield.com/press-releases/annual-meeting-notice",
            "body_text": "Notice of routine procedural meeting.",
        }
        res = self.pipeline.process_item(item, dry_run=True)
        self.assertEqual(res["status"], "dropped_irrelevant")
        self.assertFalse(os.path.exists(self.test_csv) and len(pd.read_csv(self.test_csv)) > 0)

    def test_live_brookfield_scraping(self):
        """Verify that live newsroom scraping returns real articles and bodies."""
        cards = fetch_brookfield_press_releases(page=0)
        self.assertGreater(len(cards), 0)
        first_card = cards[0]
        self.assertTrue(first_card["headline"])
        self.assertTrue(first_card["url"])
        self.assertIsNotNone(first_card["published_date"])

        # Fetch detail page
        body = fetch_article_body(first_card["url"])
        self.assertGreater(len(body), 50)

    def test_live_shareholder_letters_scraping(self):
        """Verify that live shareholder letters are discovered and parsed."""
        letters = fetch_brookfield_shareholder_letters()
        self.assertGreater(len(letters), 0)
        first_letter = letters[0]
        self.assertTrue(first_letter["headline"])
        self.assertTrue(first_letter["url"])
        self.assertIsNotNone(first_letter["published_date"])


if __name__ == "__main__":
    unittest.main()
