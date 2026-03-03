"""
Metric 2: My Resolved Contribution (Jira)

Formula:
    Sum of story points on issues marked Done within the period
    where assignee = engineer.

Unit: story points (float)
Lower is better: False (higher is better)

Data source: Jira issue fields (story points custom field) + status transition time.
"""

from __future__ import annotations

from typing import Any, List

from utils.logging import get_logger

logger = get_logger(__name__)


def compute(issues: List[Any], story_points_field: str) -> float:
    """Sum story points for all resolved issues in the given list.

    Args:
        issues:              Jira Issue objects resolved in the period
                             (returned by JiraClient.get_resolved_issues).
        story_points_field:  Jira custom field name for story points,
                             e.g. "customfield_10016"
                             (from JIRA_STORY_POINTS_FIELD env var).

    Returns:
        Total story points as a float.  Issues without a story points value
        contribute 0 (they were unestimated; this is the correct treatment —
        we do NOT exclude them, because exclusion would inflate the metric).
    """
    total = 0.0
    unestimated_count = 0

    for issue in issues:
        value = getattr(issue.fields, story_points_field, None)
        if value is None:
            unestimated_count += 1
            continue
        try:
            total += float(value)
        except (TypeError, ValueError):
            unestimated_count += 1

    if unestimated_count:
        logger.debug(
            "resolved_contribution: %d issue(s) had no story points (treated as 0)",
            unestimated_count,
        )

    logger.debug("resolved_contribution: total=%.1f pts from %d issues", total, len(issues))
    return round(total, 1)
