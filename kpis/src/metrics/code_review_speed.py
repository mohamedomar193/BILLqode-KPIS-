"""
Metric 5: My Code Review Speed (GitHub)

Formula:
    Median hours from "Ready for Review" to the engineer's first comment
    or approval on each PR they reviewed.

    Ready for Review time:
      - If PR was a draft, use the "ready_for_review" timeline event time.
      - Otherwise, use PR created_at.

    First interaction time:
      - Earliest of:
          a) First review submission by engineer on that PR
          b) First PR comment (issue comment) by engineer on that PR

Unit: hours (float)
Lower is better: True (faster = more responsive)

Data source: GitHub PR timeline events + reviews + issue comments.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

import pandas as pd

from utils.dates import hours_between, parse_iso
from utils.logging import get_logger

logger = get_logger(__name__)


def compute(
    reviewed_prs: List[Any],
    engineer_login: str,
    gh_client: Any,
) -> Optional[float]:
    """Compute median hours from ready-for-review to first engineer interaction.

    Args:
        reviewed_prs:    PRs the engineer reviewed (from GitHubClient.get_prs_reviewed_by).
        engineer_login:  GitHub login of the engineer.
        gh_client:       GitHubClient instance.

    Returns:
        Median hours as a float, or None if no data points.
    """
    login_lower = engineer_login.lower()
    speed_hours: List[float] = []

    for pr in reviewed_prs:
        # Step 1: When did the PR become ready for review?
        ready_time = gh_client.get_ready_for_review_time(pr)

        # Step 2: What was the engineer's first interaction?
        first_interaction = _get_first_interaction(pr, login_lower, gh_client)

        if first_interaction is None:
            continue

        h = hours_between(ready_time, first_interaction)
        if h is not None and h >= 0:
            speed_hours.append(h)

    if not speed_hours:
        logger.debug("code_review_speed(%s): no data points", engineer_login)
        return None

    median_h = float(pd.Series(speed_hours).median())
    logger.debug(
        "code_review_speed(%s): %d PRs, median=%.2fh",
        engineer_login,
        len(speed_hours),
        median_h,
    )
    return round(median_h, 2)


def _get_first_interaction(
    pr: Any,
    login_lower: str,
    gh_client: Any,
) -> Optional[datetime]:
    """Return the earliest review or comment time by the engineer on this PR."""
    timestamps: List[datetime] = []

    # --- Reviews ---
    reviews = gh_client.get_pr_reviews(pr)
    for review in reviews:
        if review.user and review.user.login.lower() == login_lower:
            if review.submitted_at:
                timestamps.append(review.submitted_at.replace(tzinfo=timezone.utc))

    # --- Issue comments ---
    comments = gh_client.get_pr_comments(pr)
    for comment in comments:
        if comment.user and comment.user.login.lower() == login_lower:
            if comment.created_at:
                timestamps.append(comment.created_at.replace(tzinfo=timezone.utc))

    return min(timestamps) if timestamps else None
