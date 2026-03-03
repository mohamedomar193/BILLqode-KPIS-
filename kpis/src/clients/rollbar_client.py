"""
Rollbar API client for the KPI pipeline.

Uses the Rollbar REST API v1 directly via `requests`.
Docs: https://docs.rollbar.com/reference/items

Assumptions / TODOs:
- The Rollbar project has GitHub integration enabled so that "last_occurrence"
  entries include framework/code context with commit author info.
- Blame attribution is based on commit author email matching `rollbar_identity`.
  If your Rollbar uses a different blame field, adjust _extract_blame_email().
- Rollbar timestamps are Unix epoch integers.

Authentication:
  Read-only project access token (ROLLBAR_TOKEN env var).
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from utils.dates import from_epoch, in_period
from utils.logging import get_logger

logger = get_logger(__name__)

# Rollbar API base URL
_BASE_URL = "https://api.rollbar.com/api/1"

# Items per page (Rollbar max is 100)
_PAGE_SIZE = 100

# Polite sleep between pages
_PAGE_SLEEP_SECS = 0.3


class RollbarClient:
    """Thin wrapper around the Rollbar Items REST API."""

    def __init__(self, token: str, project_id: str, environment: str = "production") -> None:
        """
        Args:
            token:       Rollbar read access token (project-level).
            project_id:  Rollbar project ID (numeric string).
            environment: Environment filter, e.g. "production".
        """
        self._token = token
        self._project_id = project_id
        self._environment = environment
        self._session = requests.Session()
        self._session.headers.update({"X-Rollbar-Access-Token": token})
        logger.info(
            "RollbarClient initialised for project %s env=%s",
            project_id,
            environment,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """GET a Rollbar API endpoint; raise on non-200 or Rollbar err field."""
        url = f"{_BASE_URL}{path}"
        resp = self._session.get(url, params=params or {}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("err") != 0:
            raise RuntimeError(f"Rollbar API error: {data.get('message', data)}")
        return data.get("result", {})

    # ------------------------------------------------------------------
    # Items (error occurrences)
    # ------------------------------------------------------------------

    def get_items(
        self,
        since: datetime,
        until: datetime,
    ) -> List[Dict[str, Any]]:
        """Return Rollbar items whose first occurrence falls within [since, until).

        Fetches all pages of items from the project, then filters by
        first_occurrence_timestamp client-side (Rollbar's date filter is
        approximate and based on last-occurrence; client-side filtering is
        more precise for first_seen semantics).

        Returns a list of item dicts. Each dict is the raw Rollbar item object.
        Key fields used downstream:
            item["id"]
            item["first_occurrence_timestamp"]  — Unix epoch int
            item["last_occurrence"]             — dict with blame info
            item["resolved_timestamp"]          — Unix epoch int or None
            item["status"]                      — "active" | "resolved" | "muted"
        """
        since_ts = int(since.timestamp())
        until_ts = int(until.timestamp())

        items: List[Dict[str, Any]] = []
        page = 1

        while True:
            params = {
                "environment": self._environment,
                "status": "active",       # fetch active; we'll also fetch resolved below
                "page": page,
                "per_page": _PAGE_SIZE,
            }
            result = self._get("/items/", params=params)
            batch = result.get("items", [])

            # Filter by first_occurrence_timestamp
            for item in batch:
                first_seen_ts = item.get("first_occurrence_timestamp")
                if first_seen_ts and since_ts <= first_seen_ts < until_ts:
                    items.append(item)

            if len(batch) < _PAGE_SIZE:
                break

            # If the oldest item in batch is before our window, we can stop
            oldest_ts = batch[-1].get("first_occurrence_timestamp", until_ts)
            if oldest_ts < since_ts:
                break

            page += 1
            time.sleep(_PAGE_SLEEP_SECS)

        # Also fetch resolved items in the window (for MTTR)
        resolved_items = self._get_resolved_items(since_ts, until_ts)
        # Merge — deduplicate by ID
        existing_ids = {item["id"] for item in items}
        for item in resolved_items:
            if item["id"] not in existing_ids:
                items.append(item)

        logger.debug("get_items: %d total items in period", len(items))
        return items

    def _get_resolved_items(self, since_ts: int, until_ts: int) -> List[Dict[str, Any]]:
        """Fetch resolved items for MTTR computation."""
        items: List[Dict[str, Any]] = []
        page = 1

        while True:
            params = {
                "environment": self._environment,
                "status": "resolved",
                "page": page,
                "per_page": _PAGE_SIZE,
            }
            try:
                result = self._get("/items/", params=params)
            except Exception as exc:
                logger.warning("Could not fetch resolved items page %d: %s", page, exc)
                break

            batch = result.get("items", [])
            for item in batch:
                first_seen_ts = item.get("first_occurrence_timestamp")
                if first_seen_ts and since_ts <= first_seen_ts < until_ts:
                    items.append(item)

            if len(batch) < _PAGE_SIZE:
                break
            page += 1
            time.sleep(_PAGE_SLEEP_SECS)

        return items

    # ------------------------------------------------------------------
    # Item detail (for linked Jira issues)
    # ------------------------------------------------------------------

    def get_item_detail(self, item_id: int) -> Dict[str, Any]:
        """Fetch full detail for a single Rollbar item (includes custom data)."""
        try:
            return self._get(f"/item/{item_id}")
        except Exception as exc:
            logger.warning("get_item_detail(%d) failed: %s", item_id, exc)
            return {}

    # ------------------------------------------------------------------
    # Blame extraction
    # ------------------------------------------------------------------

    @staticmethod
    def extract_blame_identity(item: Dict[str, Any]) -> Optional[str]:
        """Extract the commit-author identity from a Rollbar item.

        Rollbar GitHub integration populates the last_occurrence.body with
        commit author info.  We look in several places:

        1. item["last_occurrence"]["person"]["email"]   — if user tracking enabled
        2. item["assigned_user"]["email"]               — if manually assigned
        3. (fallback) None — unattributed

        Callers match this against Engineer.rollbar_identity (case-insensitive).

        TODO: If your Rollbar uses a different blame field, adjust this method.
        For example, some setups store blame in custom fields:
            item["last_occurrence"]["body"]["extra"]["commit_author_email"]
        """
        # Check assigned user
        assigned = item.get("assigned_user") or {}
        if assigned.get("email"):
            return assigned["email"].lower()

        # Check person tracking in last occurrence
        last_occ = item.get("last_occurrence") or {}
        person = last_occ.get("person") or {}
        if person.get("email"):
            return person["email"].lower()

        # Check custom data in last occurrence body
        body = last_occ.get("body") or {}
        extra = body.get("extra") or {}
        if extra.get("commit_author_email"):
            return extra["commit_author_email"].lower()

        return None
