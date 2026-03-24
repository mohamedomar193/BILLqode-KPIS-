#!/usr/bin/env python3
"""
Standalone Jira connectivity test.

Verifies that JIRA_BASE_URL, JIRA_EMAIL, and JIRA_API_TOKEN are correct
by hitting GET /rest/api/3/serverInfo with Basic Auth.

Usage:
    export JIRA_BASE_URL=https://yourorg.atlassian.net
    export JIRA_EMAIL=you@example.com
    export JIRA_API_TOKEN=your_api_token
    python kpis/scripts/test_jira_connection.py
"""

import json
import os
import sys
from base64 import b64encode

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is not installed. Run: pip install requests")
    sys.exit(1)


def main() -> None:
    base_url = os.environ.get("JIRA_BASE_URL", "").strip().rstrip("/")
    email = os.environ.get("JIRA_EMAIL", "").strip()
    token = os.environ.get("JIRA_API_TOKEN", "").strip()

    missing = []
    if not base_url:
        missing.append("JIRA_BASE_URL")
    if not email:
        missing.append("JIRA_EMAIL")
    if not token:
        missing.append("JIRA_API_TOKEN")

    if missing:
        print("ERROR: The following environment variables are not set:")
        for v in missing:
            print(f"  • {v}")
        sys.exit(1)

    url = f"{base_url}/rest/api/3/serverInfo"
    credentials = b64encode(f"{email}:{token}".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "Accept": "application/json",
    }

    print(f"Connecting to: {url}")
    print(f"Email:         {email}")
    print(f"Token:         {'*' * 8} (hidden)")
    print()

    try:
        resp = requests.get(url, headers=headers, timeout=15)
    except requests.exceptions.ConnectionError as exc:
        print(f"CONNECTION ERROR: Could not reach {base_url}")
        print(f"  {exc}")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print(f"TIMEOUT: No response from {base_url} within 15 seconds.")
        sys.exit(1)

    print(f"Status code: {resp.status_code}")

    if resp.status_code == 200:
        try:
            data = resp.json()
        except ValueError:
            print("WARNING: Response was 200 but body is not valid JSON.")
            print(resp.text[:500])
            sys.exit(0)

        print("Jira connection test SUCCESSFUL.")
        print()
        print(f"  Server title:   {data.get('serverTitle', 'N/A')}")
        print(f"  Version:        {data.get('version', 'N/A')}")
        print(f"  Deployment type:{data.get('deploymentType', 'N/A')}")
        print(f"  Base URL:       {data.get('baseUrl', 'N/A')}")
        sys.exit(0)

    elif resp.status_code == 401:
        print("FAILED: 401 Unauthorized — credentials are incorrect.")
        print()
        print("Check:")
        print("  1. JIRA_EMAIL must be your Atlassian account email (e.g. you@example.com)")
        print("  2. JIRA_API_TOKEN must be a token from https://id.atlassian.com/manage-profile/security/api-tokens")
        print("     (NOT your Atlassian password)")
        print()
        try:
            body = resp.json()
            print(f"API response: {json.dumps(body, indent=2)}")
        except ValueError:
            pass
        sys.exit(1)

    elif resp.status_code == 403:
        print("FAILED: 403 Forbidden — token is valid but lacks permission to read server info.")
        print("Ensure the account has Browse Projects permission in at least one project.")
        sys.exit(1)

    elif resp.status_code == 404:
        print(f"FAILED: 404 Not Found — check that JIRA_BASE_URL is correct: {base_url}")
        print("Expected format: https://yourorg.atlassian.net")
        sys.exit(1)

    else:
        print(f"FAILED: Unexpected status {resp.status_code}")
        print(f"Response: {resp.text[:500]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
