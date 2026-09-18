"""
Unit tests for the Interactive Two-Way Telegram Bot and Query Engine.
"""

import os
import unittest
from interactive_bot import DealDataManager, ConversationalAssistant, InteractiveTelegramBot
from config import DEFAULT_LOG_CSV


class TestInteractiveBot(unittest.TestCase):

    def setUp(self):
        self.data_mgr = DealDataManager(csv_path=DEFAULT_LOG_CSV)

    def test_datastore_loading(self):
        df = self.data_mgr.load_data()
        self.assertFalse(df.empty, "Master datastore should contain records")
        self.assertIn("headline", df.columns)
        self.assertIn("deal_hash", df.columns)

    def test_latest_deals_retrieval(self):
        latest = self.data_mgr.get_latest_deals(limit=3)
        self.assertGreater(len(latest), 0)
        self.assertLessEqual(len(latest), 3)
        self.assertIn("headline", latest[0])

    def test_keyword_search(self):
        # Search for Reliance or Brookfield or Europe
        matches = self.data_mgr.search_deals("Reliance", limit=5)
        self.assertIsInstance(matches, list)
        if matches:
            found = any("reliance" in str(m.get("headline", "")).lower() for m in matches)
            self.assertTrue(found)

    def test_region_filtering(self):
        matches = self.data_mgr.filter_by_region("North America", limit=5)
        self.assertIsInstance(matches, list)
        for m in matches:
            self.assertIn("north america", str(m.get("primary_region", "")).lower())

    def test_stats_aggregation(self):
        stats = self.data_mgr.get_stats()
        self.assertIn("total_events", stats)
        self.assertGreater(stats["total_events"], 0)
        self.assertIn("regions", stats)
        self.assertIn("signals", stats)

    def test_hash_lookup_and_deep_dive(self):
        deals = self.data_mgr.get_latest_deals(limit=1)
        self.assertTrue(len(deals) > 0)
        rec = deals[0]
        deal_hash = rec.get("deal_hash")
        self.assertIsNotNone(deal_hash)

        looked_up = self.data_mgr.get_deal_by_hash(deal_hash)
        self.assertIsNotNone(looked_up)
        self.assertEqual(looked_up["headline"], rec["headline"])

        assistant = ConversationalAssistant(data_mgr=self.data_mgr)
        deep_dive = assistant.generate_deal_deep_dive(rec)
        self.assertIn("STRATEGIC DEEP DIVE", deep_dive)

    def test_authorization_logic(self):
        bot = InteractiveTelegramBot(datastore_path=DEFAULT_LOG_CSV)
        # Verify unauthorized chat ID is rejected
        if bot.authorized_chat_id:
            self.assertTrue(bot.is_authorized(bot.authorized_chat_id))
            self.assertFalse(bot.is_authorized("999999999_fake_chat"))

    def test_morning_digest_generation(self):
        bot = InteractiveTelegramBot(datastore_path=DEFAULT_LOG_CSV)
        digest = bot.generate_morning_digest()
        self.assertIn("BROOKFIELD MORNING PE BRIEFING", digest)
        self.assertIn("Recent M&amp;A &amp; Growth Deals", digest)
        self.assertIn("Master Datastore", digest)

    def test_export_csv_validation(self):
        self.assertTrue(os.path.exists(DEFAULT_LOG_CSV))
        df = self.data_mgr.load_data()
        self.assertGreater(len(df), 50)
        # Check all published_date fields are populated
        missing_dates = df["published_date"].isna().sum()
        self.assertEqual(missing_dates, 0, "No records should have missing published_date")

    def test_benzinga_wire_fetching(self):
        from api_manager import is_benzinga_configured
        from fetcher import fetch_benzinga_news
        if not is_benzinga_configured():
            self.skipTest("Benzinga API key not configured in .env")

        articles = fetch_benzinga_news(tickers=["BAM"], limit=2)
        self.assertIsInstance(articles, list)
        if articles:
            first = articles[0]
            self.assertIn("headline", first)
            self.assertIn("url", first)
            self.assertEqual(first.get("ticker"), "BAM")
            self.assertEqual(first.get("source"), "benzinga")

    def test_wire_command_handler(self):
        bot = InteractiveTelegramBot(datastore_path=DEFAULT_LOG_CSV)
        sent_messages = []
        bot.send_message = lambda chat_id, text, reply_markup=None: sent_messages.append((chat_id, text))

        bot.handle_wire(chat_id="12345", text="/wire BAM 2")
        self.assertGreaterEqual(len(sent_messages), 1)
        combined_text = " ".join(msg[1] for msg in sent_messages)
        self.assertTrue("Benzinga" in combined_text or "wire" in combined_text.lower())


if __name__ == "__main__":
    unittest.main()
