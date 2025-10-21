"""Utilities for sending Telegram notifications."""

from __future__ import annotations

from typing import Mapping, Optional, Any, Union

import requests
from loguru import logger


class TelegramNotifier:
    """Thin wrapper around Telegram Bot API for simple text notifications."""

    def __init__(self, token: Union[str, int], chat_id: Union[str, int], *, timeout: float = 10.0) -> None:
        # Accept ints from YAML (e.g., CHAT_ID: 123456) and coerce to str
        self._token = str(token).strip()
        self._chat_id = str(chat_id).strip()
        self._timeout = timeout

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, timeout: float = 10.0) -> Optional["TelegramNotifier"]:
        """Build a notifier when bot credentials are present in the config."""
        token = (config or {}).get("TELEGRAM_TOKEN")
        chat_id = (config or {}).get("CHAT_ID")
        if not token or not chat_id:
            logger.debug("Telegram notifier not configured (missing token or chat id).")
            return None
        return cls(token, chat_id, timeout=timeout)

    def send(self, message: str) -> bool:
        """Send a text message to the configured Telegram chat."""
        if not message:
            logger.debug("Telegram notifier skipped empty message.")
            return False

        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {"chat_id": self._chat_id, "text": message}

        try:
            response = requests.post(url, data=payload, timeout=self._timeout)
            if response.status_code == 200:
                logger.debug("Telegram message sent successfully.")
                return True

            logger.error(
                f"Telegram message failed with status {response.status_code}: {response.text}"
            )
        except requests.exceptions.RequestException as exc:
            logger.error(f"Telegram message failed due to network error: {exc}")

        return False

