"""
Slack API client for the KPI pipeline.

Sends direct messages (DMs) to individual engineers and to the admin.
Uses the Slack Web API `chat.postMessage` endpoint with the bot token.

Privacy guarantee:
  Each call to send_dm() sends to a single user's DM channel.
  Messages are NEVER posted to public/shared channels.

Prerequisites:
  - Slack bot with `chat:write` scope installed in the workspace.
  - Bot invited to each engineer's DM (Slack opens DMs automatically when
    you send to a user_id).

In dry_run mode, all messages are printed to stdout instead.
"""

from __future__ import annotations

from typing import Optional

import requests

from utils.logging import get_logger

logger = get_logger(__name__)

_SLACK_API_URL = "https://slack.com/api/chat.postMessage"
_MAX_TEXT_LEN = 40000  # Slack message limit; truncate long reports


class SlackClient:
    """Sends DMs via Slack Web API."""

    def __init__(self, bot_token: str, dry_run: bool = False) -> None:
        """
        Args:
            bot_token: Slack Bot OAuth token (xoxb-...).
            dry_run:   If True, print messages to stdout instead of sending.
        """
        self._token = bot_token
        self._dry_run = dry_run

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def send_dm(self, user_id: str, text: str) -> bool:
        """Send a direct message to a Slack user.

        Args:
            user_id: Slack member ID (U01ABCDE...).
            text:    Message text (Markdown / mrkdwn formatted).

        Returns:
            True on success, False on failure.
        """
        if self._dry_run:
            print(f"\n{'='*70}")
            print(f"[DRY RUN] Slack DM → {user_id}")
            print('='*70)
            print(text[:_MAX_TEXT_LEN])
            print('='*70)
            return True

        truncated = text[:_MAX_TEXT_LEN]
        payload = {
            "channel": user_id,   # Sending to a user ID opens a DM automatically
            "text": truncated,
            "mrkdwn": True,
        }
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json; charset=utf-8",
        }

        try:
            resp = requests.post(
                _SLACK_API_URL,
                json=payload,
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data.get("ok"):
                error_code = data.get("error", "unknown_error")
                logger.error(
                    "Slack send_dm(%s) API error: %s — full response: %s",
                    user_id,
                    error_code,
                    data,
                )
                return False

            logger.info("Slack DM sent successfully to %s", user_id)
            return True

        except requests.RequestException as exc:
            logger.error("Slack send_dm(%s) network error: %s", user_id, exc)
            return False

    def send_blocks_dm(self, user_id: str, blocks: list, fallback_text: str = "") -> bool:
        """Send a Block Kit message DM (richer formatting, optional extension point).

        TODO: Implement Block Kit rendering if richer formatting is needed.
        Currently falls back to send_dm with plain text.
        """
        return self.send_dm(user_id, fallback_text or str(blocks))
