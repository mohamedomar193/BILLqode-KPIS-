"""
Metric 4: My Review Contribution (GitHub)

Formula:
    A) Count of PR reviews performed by the engineer within the period
       (on PRs NOT authored by the engineer).
    B) Average hours from PR created_at to the engineer's first review
       submission on those PRs.

Returns both as a tuple: (review_count: int, avg_hours_to_first_review: float | None)

Unit: count + hours
Lower is better: False for count (higher = more helpful reviews)
                 True for avg hours (faster = better)

Data source: GitHub reviews API per PR.
"""

from __future__ import annotations

from datetime import timezone
from typing import Any, List, Optional, Tuple

import pandas as pd

from utils.dates import hours_between, parse_iso
from utils.logging import get_logger

logger = get_logger(__name__)


def compute(
    reviewed_prs: List[Any],
    engineer_login: str,
    gh_client: Any,
) -> Tuple[int, Optional[float]]:
    """Compute review count and average time-to-first-review.

    Args:
        reviewed_prs:    PRs the engineer reviewed (from GitHubClient.get_prs_reviewed_by).
                         These are NOT authored by the engineer.
        engineer_login:  GitHub login of the engineer (case-insensitive match).
        gh_client:       GitHubClient instance (used to fetch per-PR reviews).

    Returns:
        (review_count, avg_hours_to_first_review)
        review_count is the number of reviews submitted.
        avg_hours_to_first_review is None if no reviewable PRs found.
    """
    login_lower = engineer_login.lower()
    review_count = 0
    hours_list: List[float] = []

    for pr in reviewed_prs:
        reviews = gh_client.get_pr_reviews(pr)
        pr_created = pr.created_at.replace(tzinfo=timezone.utc)

        # Find reviews submitted by this engineer
        engineer_reviews = [
            r for r in reviews
            if r.user and r.user.login.lower() == login_lower
        ]

        if not engineer_reviews:
            continue

        # Count all reviews submitted (not just first)
        review_count += len(engineer_reviews)

        # Time-to-first-review: use earliest submitted_at
        submitted_times = [
            r.submitted_at.replace(tzinfo=timezone.utc)
            for r in engineer_reviews
            if r.submitted_at
        ]
        if submitted_times:
            first_review_time = min(submitted_times)
            h = hours_between(pr_created, first_review_time)
            if h is not None and h >= 0:
                hours_list.append(h)

    avg_hours = None
    if hours_list:
        avg_hours = round(float(pd.Series(hours_list).mean()), 2)

    logger.debug(
        "review_contribution(%s): %d reviews, avg_hours=%.2f",
        engineer_login,
        review_count,
        avg_hours or 0,
    )
    return review_count, avg_hours
