"""
Metric 3: My PR Merge Throughput (GitHub)

Formula:
    Count of merged pull requests authored by the engineer within the period.

Unit: count (int)
Lower is better: False (higher is better)

Data source: GitHub Search API or repo PR listing with merged_at filter.
The GitHubClient.get_merged_prs() already filters by author and period.
"""

from __future__ import annotations

from typing import Any, List

from utils.logging import get_logger

logger = get_logger(__name__)


def compute(merged_prs: List[Any]) -> int:
    """Return the count of merged PRs.

    Args:
        merged_prs: List of GitHub PullRequest objects returned by
                    GitHubClient.get_merged_prs().  These are already
                    filtered to the correct author and date range.

    Returns:
        Integer count.
    """
    count = len(merged_prs)
    logger.debug("pr_merge_throughput: %d merged PRs", count)
    return count
