"""
Discord connectivity test for the KPI pipeline bot.

Verifies that:
  - DISCORD_BOT_TOKEN and DISCORD_TEST_USER_ID are set
  - The Discord API is reachable
  - The bot can open a DM channel with the target user
  - The bot can send a message to that channel

Usage:
    export DISCORD_BOT_TOKEN=your_token_here
    export DISCORD_TEST_USER_ID=your_user_id_here
    python kpis/scripts/test_discord_connection.py

Does NOT touch the KPI pipeline or any pipeline logic.
"""

import os
import sys

import requests

DISCORD_API = "https://discord.com/api/v10"


def main() -> None:
    # ----------------------------------------------------------------
    # 1. Read and validate environment variables
    # ----------------------------------------------------------------
    token = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    user_id = os.environ.get("DISCORD_TEST_USER_ID", "").strip()

    missing = []
    if not token:
        missing.append("DISCORD_BOT_TOKEN")
    if not user_id:
        missing.append("DISCORD_TEST_USER_ID")

    if missing:
        print("Error: the following environment variables are not set:")
        for var in missing:
            print(f"  • {var}")
        print("\nSet them in your shell and re-run:")
        print("  export DISCORD_BOT_TOKEN=your_token_here")
        print("  export DISCORD_TEST_USER_ID=your_user_id_here")
        sys.exit(1)

    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }

    # ----------------------------------------------------------------
    # 2. Open a DM channel with the target user
    # ----------------------------------------------------------------
    dm_resp = requests.post(
        f"{DISCORD_API}/users/@me/channels",
        headers=headers,
        json={"recipient_id": user_id},
        timeout=30,
    )

    if not dm_resp.ok:
        print(f"Failed to open DM channel.")
        print(f"Status code: {dm_resp.status_code}")
        print(f"Response: {dm_resp.text}")
        sys.exit(1)

    channel_id = dm_resp.json().get("id")
    if not channel_id:
        print("Failed to open DM channel: no channel ID in response.")
        print(f"Response: {dm_resp.text}")
        sys.exit(1)

    # ----------------------------------------------------------------
    # 3. Send the test message
    # ----------------------------------------------------------------
    msg_resp = requests.post(
        f"{DISCORD_API}/channels/{channel_id}/messages",
        headers=headers,
        json={"content": "✅ KPI bot test message successful. Discord integration works."},
        timeout=30,
    )

    if not msg_resp.ok:
        print(f"Failed to send message.")
        print(f"Status code: {msg_resp.status_code}")
        print(f"Response: {msg_resp.text}")
        sys.exit(1)

    # ----------------------------------------------------------------
    # 4. Success
    # ----------------------------------------------------------------
    print("Discord connection test successful.")
    print("Message sent to user.")


if __name__ == "__main__":
    main()
