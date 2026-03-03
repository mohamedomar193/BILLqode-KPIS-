"""
Metric 7: My MTTR — Mean Time to Resolve (Rollbar)

Formula:
    Median hours from item first_seen to resolved_at for Rollbar items
    attributed to the engineer within the period.

    Resolution time source priority:
      1. Rollbar resolved_at timestamp (if available and item is resolved)
      2. Jira Done time, if a linked Jira issue is resolved
         (requires Rollbar–Jira integration; TODO if not available)

    Items still open (no resolved_at and no Jira Done) are IGNORED.

Unit: hours (float)
Lower is better: True (faster resolution = better)

Data source: Rollbar timestamps + optional Jira link.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from clients.rollbar_client import RollbarClient
from utils.dates import from_epoch, hours_between
from utils.logging import get_logger

logger = get_logger(__name__)


def compute(
    rollbar_items: List[Dict[str, Any]],
    rollbar_identity: str,
    jira_client: Optional[Any] = None,
) -> Optional[float]:
    """Compute median MTTR in hours for items attributed to the engineer.

    Args:
        rollbar_items:      Items returned by RollbarClient.get_items() for
                            the period (includes both active and resolved).
        rollbar_identity:   Engineer's Rollbar identity (email / username).
        jira_client:        Optional JiraClient instance.  If provided, used
                            to look up Jira Done time for items that have a
                            linked Jira issue but no Rollbar resolved_at.
                            Pass None to skip Jira fallback.

    Returns:
        Median MTTR in hours, or None if no resolved items found.
    """
    identity_lower = rollbar_identity.lower()
    mttr_hours: List[float] = []

    for item in rollbar_items:
        blame = RollbarClient.extract_blame_identity(item)
        if blame is None or blame.lower() != identity_lower:
            continue

        first_seen = from_epoch(item.get("first_occurrence_timestamp"))
        resolved_at = _get_resolved_time(item, jira_client)

        if first_seen is None or resolved_at is None:
            # Still open — skip
            continue

        h = hours_between(first_seen, resolved_at)
        if h is not None and h >= 0:
            mttr_hours.append(h)

    if not mttr_hours:
        logger.debug("mttr(%s): no resolved items", rollbar_identity)
        return None

    median_h = float(pd.Series(mttr_hours).median())
    logger.debug(
        "mttr(%s): %d items, median=%.2fh",
        rollbar_identity,
        len(mttr_hours),
        median_h,
    )
    return round(median_h, 2)


def _get_resolved_time(
    item: Dict[str, Any],
    jira_client: Optional[Any],
) -> Optional[Any]:
    """Get the resolved timestamp for a Rollbar item.

    Priority:
    1. Rollbar resolved_timestamp field (most reliable)
    2. Jira Done transition time via linked issue (if jira_client available)
    """
    # 1. Rollbar native resolved time
    resolved_ts = item.get("resolved_timestamp")
    if resolved_ts:
        return from_epoch(resolved_ts)

    # Only check "resolved" status items
    if item.get("status") != "resolved":
        return None

    # 2. Jira fallback
    # TODO: Extract Jira issue key from item custom_data if Rollbar-Jira integration
    # is configured.  Example: item["last_occurrence"]["body"]["extra"]["jira_issue"] = "PROJ-123"
    # Then call: jira_client.get_resolved_issues(account_id=...) or fetch by key.
    # For now, return None to gracefully skip the fallback.
    if jira_client:
        jira_key = _extract_jira_key(item)
        if jira_key:
            try:
                from utils.dates import parse_iso
                jira_issue = jira_client._jira.issue(jira_key, expand="changelog")
                # Find Done transition time from changelog
                for history in jira_issue.changelog.histories:
                    for change in history.items:
                        if change.field == "status" and (change.toString or "").lower() == "done":
                            return parse_iso(history.created)
            except Exception as exc:
                logger.debug("MTTR Jira fallback failed for key %s: %s", jira_key, exc)

    return None


def _extract_jira_key(item: Dict[str, Any]) -> Optional[str]:
    """Try to extract a linked Jira issue key from Rollbar item custom data.

    TODO: Adjust the path below to match your Rollbar–Jira integration output.
    Common patterns:
      item["last_occurrence"]["body"]["extra"]["jira_issue"]  → "PROJ-123"
      item["custom_data"]["jira_key"]                         → "PROJ-123"
    """
    last_occ = item.get("last_occurrence") or {}
    body = last_occ.get("body") or {}
    extra = body.get("extra") or {}
    return extra.get("jira_issue") or extra.get("jira_key") or None
