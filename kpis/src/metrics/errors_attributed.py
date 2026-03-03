"""
Metric 6: Errors Attributed to My Changes (Rollbar)

Formula:
    Count of new Rollbar production items whose first_seen falls within
    the period AND blame points to the engineer (commit author / last
    editor matching rollbar_identity).  Unattributed items are excluded.

Unit: count (int)
Lower is better: True (fewer errors = better quality)

Data source: Rollbar items/occurrences with blame.
Assumes GitHub integration is enabled so Rollbar captures commit author info.

NOTE: Rollbar blame attribution depends on your integration setup.
If items don't have author info, this metric will return 0 (not an error —
it means no items were attributed to anyone matching your identity).
"""

from __future__ import annotations

from typing import Any, Dict, List

from clients.rollbar_client import RollbarClient
from utils.logging import get_logger

logger = get_logger(__name__)


def compute(
    rollbar_items: List[Dict[str, Any]],
    rollbar_identity: str,
) -> int:
    """Count Rollbar items attributed to the engineer.

    Args:
        rollbar_items:      Items returned by RollbarClient.get_items() for
                            the target period.  Already first-occurrence filtered.
        rollbar_identity:   Engineer's email or username as it appears in
                            Rollbar blame / commit author (from engineers.yml).

    Returns:
        Integer count of attributed items.  Returns 0 if none match.
    """
    identity_lower = rollbar_identity.lower()
    count = 0

    for item in rollbar_items:
        blame_identity = RollbarClient.extract_blame_identity(item)
        if blame_identity is None:
            # Unattributed — skip as specified
            continue
        if blame_identity.lower() == identity_lower:
            count += 1

    logger.debug(
        "errors_attributed(%s): %d attributed items out of %d total",
        rollbar_identity,
        count,
        len(rollbar_items),
    )
    return count
