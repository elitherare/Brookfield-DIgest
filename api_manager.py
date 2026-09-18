"""
API and Secret Management Module.
Loads API credentials from a local .env file or system environment variables,
preventing hardcoded secrets in source code.
"""

import os
from pathlib import Path
from typing import Dict, Optional
from dotenv import load_dotenv

# Automatically locate and load the .env file from the project root directory
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=True)
else:
    load_dotenv(override=True)


class APIManager:
    """Centralized manager for API keys, tokens, and credentials."""

    @staticmethod
    def get_gemini_api_key() -> str:
        """
        Retrieve Gemini API Key from .env or environment variable GEMINI_API_KEY.
        Returns empty string if not configured.
        """
        key = os.getenv("GEMINI_API_KEY", "").strip()
        # Clean quotes if user entered them in .env
        if key.startswith(('"', "'")) and key.endswith(('"', "'")):
            key = key[1:-1].strip()
        return key

    @staticmethod
    def get_telegram_credentials() -> Dict[str, str]:
        """
        Retrieve Telegram Bot Token and Chat ID for alert notifications.
        """
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        return {
            "bot_token": token,
            "chat_id": chat_id,
        }

    @staticmethod
    def get_benzinga_api_key() -> str:
        """
        Retrieve Benzinga (Massive.com) API Key from .env or environment variable.
        """
        key = os.getenv("MASSIVE_BENZINGA_API_KEY") or os.getenv("BENZINGA_API_KEY", "")
        key = key.strip()
        if key.startswith(('"', "'")) and key.endswith(('"', "'")):
            key = key[1:-1].strip()
        return key

    @classmethod
    def is_gemini_configured(cls) -> bool:
        """Check if a non-placeholder Gemini API key is provided."""
        key = cls.get_gemini_api_key()
        return bool(key and key != "YOUR_GEMINI_API_KEY")

    @classmethod
    def is_telegram_configured(cls) -> bool:
        """Check if Telegram bot credentials are configured."""
        creds = cls.get_telegram_credentials()
        return bool(creds["bot_token"] and creds["chat_id"])

    @classmethod
    def is_benzinga_configured(cls) -> bool:
        """Check if Benzinga API key is configured."""
        key = cls.get_benzinga_api_key()
        return bool(key and key != "YOUR_BENZINGA_API_KEY")


# Module-level convenience accessors
get_gemini_api_key = APIManager.get_gemini_api_key
get_telegram_credentials = APIManager.get_telegram_credentials
get_benzinga_api_key = APIManager.get_benzinga_api_key
is_gemini_configured = APIManager.is_gemini_configured
is_telegram_configured = APIManager.is_telegram_configured
is_benzinga_configured = APIManager.is_benzinga_configured
