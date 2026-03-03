"""
Metric 8: My CI Reliability (GitHub Actions)

Formula:
    For PRs authored by the engineer in the period, compute the percentage
    of associated workflow runs that succeeded.

    Denominator: all runs with a definitive conclusion
                 (conclusion NOT IN {None, "cancelled", "skipped"})
    Numerator:   runs with conclusion == "success"

    Cancelled / skipped runs are excluded from both numerator and denominator
    because they don't reflect code quality — they reflect external factors
    like manual cancellations or branch skip rules.

    If the denominator is 0 (all runs were cancelled/skipped or no runs exist),
    returns None.

Unit: % (float, 0-100)
Lower is better: False (higher = more reliable CI)

Data source: GitHub Actions workflow runs associated with PR head SHAs.
"""

from __future__ import annotations

from typing import Any, List, Optional

from utils.logging import get_logger

logger = get_logger(__name__)

# Conclusions excluded from both numerator and denominator.
# - "cancelled": user manually stopped the run
# - "skipped":   branch/path filter prevented the run
# - None:        run is still in progress (shouldn't appear, but guard anyway)
_EXCLUDED_CONCLUSIONS = {None, "cancelled", "skipped"}


def compute(workflow_runs: List[Any]) -> Optional[float]:
    """Compute CI success rate as a percentage.

    Args:
        workflow_runs:  WorkflowRun objects returned by
                        GitHubClient.get_workflow_runs_for_prs().
                        These are already scoped to the engineer's PRs
                        in the target period.

    Returns:
        Percentage of successful runs (0.0–100.0), or None if no applicable runs.
    """
    applicable = [r for r in workflow_runs if r.conclusion not in _EXCLUDED_CONCLUSIONS]
    successful = [r for r in applicable if r.conclusion == "success"]

    if not applicable:
        logger.debug("ci_reliability: no applicable runs (all cancelled/skipped or empty)")
        return None

    rate = (len(successful) / len(applicable)) * 100.0
    logger.debug(
        "ci_reliability: %d/%d runs succeeded = %.1f%%",
        len(successful),
        len(applicable),
        rate,
    )
    return round(rate, 1)
