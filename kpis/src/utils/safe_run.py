"""
Safe execution wrappers for the KPI pipeline.

Two levels of safety:
  1. safe_metric()   — wraps a single metric computation; on failure returns a
                       MetricResult with None values so the report can continue.
  2. EngineerResult  — container that accumulates per-source errors for one engineer.

Usage example:
    result = safe_metric(compute_cycle_time, issues, metric_name="My Cycle Time")
    if result.error:
        logger.warning("Cycle time failed: %s", result.error)
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from utils.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# MetricResult — uniform return type from every metric module
# ---------------------------------------------------------------------------

@dataclass
class MetricResult:
    """Holds current/previous values for one KPI metric."""

    name: str
    """Human-readable metric name used in reports."""

    current_value: Optional[float]
    """Value for the current rolling period. None means unavailable."""

    previous_value: Optional[float]
    """Value for the previous rolling period. None means unavailable."""

    pct_change: Optional[float]
    """Percentage change: ((current - previous) / previous) * 100.
    None if either period is unavailable or previous == 0."""

    unit: str = ""
    """Display unit, e.g. 'days', 'pts', 'count', 'hours', '%'."""

    lower_is_better: bool = False
    """If True, a decrease is positive (green arrow). Used by renderer."""

    error: Optional[str] = None
    """Non-None if this metric could not be computed (error message)."""

    @staticmethod
    def compute_pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
        """Helper: compute % change, returning None on division-by-zero or missing data."""
        if current is None or previous is None:
            return None
        if previous == 0:
            return None
        return round(((current - previous) / previous) * 100, 1)

    @classmethod
    def error_result(cls, name: str, unit: str, lower_is_better: bool, error_msg: str) -> "MetricResult":
        """Factory for a MetricResult that represents a computation failure."""
        return cls(
            name=name,
            current_value=None,
            previous_value=None,
            pct_change=None,
            unit=unit,
            lower_is_better=lower_is_better,
            error=error_msg,
        )


# ---------------------------------------------------------------------------
# safe_metric — per-metric try/except wrapper
# ---------------------------------------------------------------------------

def safe_metric(
    func: Callable[..., Any],
    *args: Any,
    metric_name: str,
    unit: str = "",
    lower_is_better: bool = False,
    **kwargs: Any,
) -> Any:
    """Call func(*args, **kwargs) and return its result.

    On any exception, logs the error and returns ``MetricResult.error_result()``.
    The caller is expected to handle the case where a MetricResult (or raw data
    fetch result) is None/error.

    Args:
        func:            The function to call.
        *args:           Positional arguments forwarded to func.
        metric_name:     Used in the error MetricResult and log messages.
        unit:            Forwarded to MetricResult.error_result on failure.
        lower_is_better: Forwarded to MetricResult.error_result on failure.
        **kwargs:        Keyword arguments forwarded to func.

    Returns:
        The return value of func, or a MetricResult with error set.
    """
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.error("safe_metric [%s] failed — %s", metric_name, error_msg)
        logger.debug("Traceback:\n%s", traceback.format_exc())
        return MetricResult.error_result(
            name=metric_name,
            unit=unit,
            lower_is_better=lower_is_better,
            error_msg=error_msg,
        )


# ---------------------------------------------------------------------------
# EngineerError — tracks per-engineer failure details
# ---------------------------------------------------------------------------

@dataclass
class EngineerError:
    """Accumulates errors for a single engineer across data sources."""

    engineer_name: str
    source_errors: dict[str, str] = field(default_factory=dict)
    """Maps source name (e.g. 'jira', 'github') to error message."""

    fatal: bool = False
    """True if the entire engineer processing failed (not just one source)."""

    fatal_error: Optional[str] = None

    def add_source_error(self, source: str, error: str) -> None:
        self.source_errors[source] = error

    def has_errors(self) -> bool:
        return bool(self.source_errors) or self.fatal

    def summary_lines(self) -> list[str]:
        lines = []
        if self.fatal:
            lines.append(f"  FATAL: {self.fatal_error}")
        for src, err in self.source_errors.items():
            lines.append(f"  [{src}] {err}")
        return lines
