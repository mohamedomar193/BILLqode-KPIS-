"""
Metric 1: My Cycle Time (Jira)

Formula:
    Median days from the first transition to "In Progress" to the first
    transition to "Done" after that, for issues where:
      - assignee = engineer
      - Done transition occurred within the reporting period

Unit: days (float, rounded to 1 dp)
Lower is better: True

Data source: Jira issue changelog (status transitions)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from utils.dates import days_between
from utils.logging import get_logger

logger = get_logger(__name__)

# Match your Jira workflow status names (case-insensitive comparison)
_STATUS_IN_PROGRESS = "in progress"
_STATUS_DONE = "done"


def compute(issues_with_changelog: List[Any]) -> Optional[float]:
    """Compute median cycle time in days for the given resolved issues.

    Args:
        issues_with_changelog:
            Jira Issue objects (with changelog expanded) that were resolved
            in the target period.  Pass the list returned by
            JiraClient.get_resolved_issues().

    Returns:
        Median cycle time in days, or None if no valid data points.
    """
    durations: List[float] = []

    for issue in issues_with_changelog:
        duration = _cycle_time_for_issue(issue)
        if duration is not None and duration >= 0:
            durations.append(duration)

    if not durations:
        logger.debug("cycle_time: no valid data points")
        return None

    series = pd.Series(durations)
    median_days = float(series.median())
    logger.debug("cycle_time: %d issues, median=%.2f days", len(durations), median_days)
    return round(median_days, 2)


def _cycle_time_for_issue(issue: Any) -> Optional[float]:
    """Return cycle time in days for a single issue, or None if not computable.

    Algorithm:
        1. Scan changelog chronologically.
        2. Find the FIRST transition to STATUS_IN_PROGRESS.
        3. After that point, find the FIRST transition to STATUS_DONE.
        4. Cycle time = days between (2) and (3).
    """
    # issue must have a .changelog attribute (populated by JiraClient)
    if not hasattr(issue, "changelog") or not issue.changelog:
        return None

    events = []
    for history in issue.changelog.histories:
        for item in history.items:
            if item.field == "status":
                from utils.dates import parse_iso
                events.append(
                    {
                        "to": (item.toString or "").lower(),
                        "created": parse_iso(history.created),
                    }
                )

    if not events:
        return None

    # Sort chronologically
    events.sort(key=lambda e: e["created"] or pd.Timestamp.min)

    in_progress_time = None
    for event in events:
        if event["to"] == _STATUS_IN_PROGRESS and in_progress_time is None:
            in_progress_time = event["created"]
        elif event["to"] == _STATUS_DONE and in_progress_time is not None:
            done_time = event["created"]
            return days_between(in_progress_time, done_time)

    # Issue moved to In Progress but never Done (shouldn't happen for resolved issues)
    return None
