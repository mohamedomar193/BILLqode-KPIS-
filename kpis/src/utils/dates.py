"""
Date/time helpers for the KPI pipeline.

All datetime objects produced here are UTC-aware (tzinfo=timezone.utc).
GitHub returns ISO-8601 with a trailing 'Z'; Jira uses ISO-8601 offsets;
Rollbar uses Unix epoch integers. Converters for each are provided here.

Period terminology:
    current  period → [now - period_days,  now)
    previous period → [now - 2*period_days, now - period_days)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Tuple

from dateutil import parser as dateutil_parser


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------
Period = Tuple[datetime, datetime]


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------

def utcnow() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(tz=timezone.utc)


def get_periods(period_days: int) -> Tuple[Period, Period]:
    """Return (current_period, previous_period) as (start, end) UTC datetime tuples.

    Args:
        period_days: Number of days in each rolling window (typically 30).

    Returns:
        current  = (now - period_days,  now)
        previous = (now - 2*period_days, now - period_days)
    """
    now = utcnow()
    current_start = now - timedelta(days=period_days)
    previous_start = now - timedelta(days=2 * period_days)
    return (current_start, now), (previous_start, current_start)


# ---------------------------------------------------------------------------
# Timestamp parsing helpers
# ---------------------------------------------------------------------------

def parse_iso(ts: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp string (GitHub / Jira format) to UTC datetime.

    Returns None if ts is None or empty.
    """
    if not ts:
        return None
    dt = dateutil_parser.isoparse(ts)
    # Ensure timezone-aware; if naive, assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def from_epoch(ts: int | float | None) -> datetime | None:
    """Convert a Unix epoch (integer seconds) to a UTC-aware datetime.

    Used for Rollbar timestamps.
    """
    if ts is None:
        return None
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


# ---------------------------------------------------------------------------
# Duration helpers
# ---------------------------------------------------------------------------

def hours_between(start: datetime | None, end: datetime | None) -> float | None:
    """Return fractional hours between two UTC-aware datetimes, or None if either is None."""
    if start is None or end is None:
        return None
    return (end - start).total_seconds() / 3600.0


def days_between(start: datetime | None, end: datetime | None) -> float | None:
    """Return fractional days between two UTC-aware datetimes, or None if either is None."""
    if start is None or end is None:
        return None
    return (end - start).total_seconds() / 86400.0


# ---------------------------------------------------------------------------
# Period boundary helpers
# ---------------------------------------------------------------------------

def in_period(dt: datetime | None, start: datetime, end: datetime) -> bool:
    """Return True if dt falls within [start, end)."""
    if dt is None:
        return False
    return start <= dt < end


def format_period(start: datetime, end: datetime) -> str:
    """Human-readable period string, e.g. '2024-06-01 → 2024-07-01'."""
    return f"{start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}"
