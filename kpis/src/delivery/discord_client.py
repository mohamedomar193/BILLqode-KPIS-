"""
Discord delivery client for the KPI pipeline.

Uses the Discord REST API v10 directly (no third-party Discord library).
Requires only the `requests` package, which is already in requirements.txt.

Capabilities:
  - send_engineer_report()  — opens a DM channel with an engineer and posts their report
  - send_manager_summary()  — posts the team summary to a Discord channel

Authentication:
  DISCORD_BOT_TOKEN env var — a Bot token from the Discord Developer Portal.
  The bot must be in the server (or have DM access) for engineer DMs to work.
  For the manager channel, the bot needs "Send Messages" permission.

Discord message limit: 2 000 characters per message.
Long reports are automatically chunked at newline boundaries.

Rate limiting:
  Discord allows ~50 requests/second globally for bots.
  We add a small sleep between messages to avoid hitting secondary limits.
"""

from __future__ import annotations

import time
from typing import List, Optional

import requests

from utils.logging import get_logger

logger = get_logger(__name__)

_DISCORD_API = "https://discord.com/api/v10"
_MAX_MESSAGE_LEN = 2000
_REQUEST_SLEEP_SECS = 0.5   # polite delay between messages


class DiscordClient:
    """Thin wrapper around the Discord REST API v10."""

    def __init__(self, bot_token: str, dry_run: bool = False) -> None:
        """
        Args:
            bot_token: Discord bot token (starts with "Bot ..." prefix added internally).
            dry_run:   If True, log what would be sent but make no real API calls.
        """
        self._dry_run = dry_run
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bot {bot_token}",
            "Content-Type": "application/json",
        })
        logger.info("DiscordClient initialised (dry_run=%s)", dry_run)

    # ------------------------------------------------------------------
    # Public delivery methods
    # ------------------------------------------------------------------

    def send_engineer_report(
        self,
        discord_user_id: str,
        report_markdown: str,
        engineer_name: str,
    ) -> bool:
        """Send an engineer's personal KPI report as a Discord DM.

        Args:
            discord_user_id:  Numeric Discord user ID snowflake (string).
            report_markdown:  Full report text (will be chunked if > 2000 chars).
            engineer_name:    Used only for log messages.

        Returns:
            True on success, False on any failure.
        """
        if self._dry_run:
            logger.info(
                "[DRY RUN] Would send Discord DM to %s (user_id=%s) — %d chars",
                engineer_name,
                discord_user_id,
                len(report_markdown),
            )
            return True

        if not discord_user_id or discord_user_id.startswith("YOUR_"):
            logger.warning(
                "Skipping Discord DM for %s — discord_user_id not configured",
                engineer_name,
            )
            return False

        channel_id = self._open_dm_channel(discord_user_id, engineer_name)
        if not channel_id:
            return False

        return self._send_chunked(channel_id, report_markdown, label=f"DM:{engineer_name}")

    def send_manager_summary(
        self,
        channel_id: str,
        summary_markdown: str,
        manager_name: str = "Manager",
    ) -> bool:
        """Post the team summary to the manager's Discord channel.

        Args:
            channel_id:       Numeric Discord channel ID (string).
            summary_markdown: Full summary text (chunked if needed).
            manager_name:     Used only for log messages.

        Returns:
            True on success, False on any failure.
        """
        if self._dry_run:
            logger.info(
                "[DRY RUN] Would send Discord manager summary to channel %s (%s) — %d chars",
                channel_id,
                manager_name,
                len(summary_markdown),
            )
            return True

        if not channel_id or channel_id.startswith("YOUR_"):
            logger.warning(
                "Skipping Discord manager summary — channel_id not configured"
            )
            return False

        return self._send_chunked(channel_id, summary_markdown, label=f"channel:{channel_id}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open_dm_channel(self, user_id: str, engineer_name: str) -> Optional[str]:
        """Create (or retrieve existing) DM channel with a user.

        Returns:
            Channel ID string on success, None on failure.
        """
        try:
            resp = self._session.post(
                f"{_DISCORD_API}/users/@me/channels",
                json={"recipient_id": user_id},
                timeout=30,
            )
            resp.raise_for_status()
            channel_id = resp.json().get("id")
            logger.debug("Opened DM channel %s for user %s (%s)", channel_id, user_id, engineer_name)
            return channel_id
        except Exception as exc:
            logger.error(
                "Failed to open DM channel for %s (user_id=%s): %s",
                engineer_name,
                user_id,
                exc,
            )
            return None

    def _send_to_channel(self, channel_id: str, content: str) -> bool:
        """Send a single message (must be ≤ 2000 chars) to a channel."""
        try:
            resp = self._session.post(
                f"{_DISCORD_API}/channels/{channel_id}/messages",
                json={"content": content},
                timeout=30,
            )
            resp.raise_for_status()
            return True
        except Exception as exc:
            logger.error("Failed to send message to channel %s: %s", channel_id, exc)
            return False

    def _send_chunked(self, channel_id: str, text: str, label: str = "") -> bool:
        """Send text to a channel, splitting into ≤ 2000-char chunks if needed."""
        chunks = _chunk_message(text)
        logger.debug("Sending %d chunk(s) to %s", len(chunks), label or channel_id)
        for i, chunk in enumerate(chunks, 1):
            if not self._send_to_channel(channel_id, chunk):
                logger.error(
                    "Failed to send chunk %d/%d to %s", i, len(chunks), label or channel_id
                )
                return False
            if len(chunks) > 1:
                time.sleep(_REQUEST_SLEEP_SECS)
        return True


# ---------------------------------------------------------------------------
# Message chunking
# ---------------------------------------------------------------------------

def _chunk_message(text: str, max_len: int = _MAX_MESSAGE_LEN) -> List[str]:
    """Split text into chunks that each fit within Discord's message length limit.

    Splits preferentially at newline boundaries to preserve formatting.
    """
    if len(text) <= max_len:
        return [text]

    chunks: List[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break

        # Try to find a newline to split at cleanly
        split_at = remaining.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len   # No newline found — hard-cut

        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip("\n")

    return chunks
