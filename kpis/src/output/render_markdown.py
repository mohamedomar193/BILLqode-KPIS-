"""
Markdown report renderer for the KPI pipeline.

Produces a clean, readable Markdown report for each engineer with:
  - A header showing engineer name and reporting period
  - A table of 8 metrics: current value, previous value, and % change
  - Trend arrows: ↑/↓ coloured by "lower is better" semantics
  - N/A displayed for missing values (API failure)
  - A footer with the generation timestamp

The output is suitable for Slack mrkdwn (backtick code blocks are rendered)
and also for rendering as GitHub Markdown.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from config import Engineer
from utils.dates import format_period
from utils.logging import get_logger
from utils.safe_run import MetricResult

logger = get_logger(__name__)

# Column widths for Markdown table formatting
_COL_METRIC = 38
_COL_VALUE = 14
_COL_PREV = 14
_COL_CHANGE = 12


def render(
    engineer: Engineer,
    metrics: List[MetricResult],
    current_period: tuple,
    previous_period: tuple,
) -> str:
    """Render a full Markdown report for one engineer.

    Args:
        engineer:        Engineer dataclass instance.
        metrics:         List of MetricResult objects (one per KPI).
        current_period:  (start: datetime, end: datetime) for current window.
        previous_period: (start: datetime, end: datetime) for previous window.

    Returns:
        Multi-line Markdown string ready to send via Slack DM.
    """
    curr_start, curr_end = current_period
    prev_start, prev_end = previous_period
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines: List[str] = []

    # ---- Header ----
    lines.append(f"# 📊 Personal KPI Report — {engineer.name}")
    lines.append("")
    lines.append(f"**Current period:** {format_period(curr_start, curr_end)}")
    lines.append(f"**Previous period:** {format_period(prev_start, prev_end)}")
    lines.append(f"*Generated: {now_str}*")
    lines.append("")

    # ---- Metrics table ----
    # Header row
    lines.append("| Metric | Current | Previous | Change |")
    lines.append("|--------|---------|----------|--------|")

    for metric in metrics:
        row = _format_row(metric)
        lines.append(row)

    lines.append("")

    # ---- Error summary (if any metric failed) ----
    failed_metrics = [m for m in metrics if m.error]
    if failed_metrics:
        lines.append("---")
        lines.append("### ⚠️ Metrics with data errors")
        for m in failed_metrics:
            lines.append(f"- **{m.name}**: `{m.error}`")
        lines.append("")
        lines.append(
            "_These metrics are marked N/A due to an API error. "
            "Your admin has been notified._"
        )
        lines.append("")

    # ---- Footer ----
    lines.append("---")
    lines.append("_This report is private and sent only to you. "
                 "Metrics use a 30-day rolling window._")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _format_row(metric: MetricResult) -> str:
    """Format one table row for a MetricResult."""
    name_cell = metric.name

    if metric.error:
        return f"| {name_cell} | N/A | N/A | — |"

    current_str = _format_value(metric.current_value, metric.unit)
    previous_str = _format_value(metric.previous_value, metric.unit)
    change_str = _format_change(metric.pct_change, metric.lower_is_better)

    return f"| {name_cell} | {current_str} | {previous_str} | {change_str} |"


def _format_value(value: Optional[float], unit: str) -> str:
    """Format a numeric value with its unit, or 'N/A'."""
    if value is None:
        return "N/A"
    if unit == "%":
        return f"{value:.1f}%"
    if unit in ("days", "hours"):
        return f"{value:.1f} {unit}"
    if unit == "pts":
        return f"{value:.1f} {unit}"
    if unit == "count":
        return str(int(value))
    # Generic fallback
    return f"{value} {unit}".strip()


def _format_change(pct_change: Optional[float], lower_is_better: bool) -> str:
    """Format % change with an arrow indicating good/bad direction.

    For "lower is better" metrics (cycle time, errors, MTTR):
      - Negative change  → improvement → ✅ arrow
      - Positive change  → regression  → ⚠️ arrow

    For "higher is better" metrics:
      - Positive change  → improvement → ✅ arrow
      - Negative change  → regression  → ⚠️ arrow
    """
    if pct_change is None:
        return "—"

    arrow = _trend_arrow(pct_change, lower_is_better)
    sign = "+" if pct_change >= 0 else ""
    return f"{arrow} {sign}{pct_change:.1f}%"


def _trend_arrow(pct_change: float, lower_is_better: bool) -> str:
    """Return an emoji arrow based on change direction and metric polarity."""
    if pct_change == 0:
        return "➡️"
    improvement = (pct_change < 0 and lower_is_better) or (pct_change > 0 and not lower_is_better)
    if improvement:
        return "✅"
    return "⚠️"


# ---------------------------------------------------------------------------
# Admin summary renderer
# ---------------------------------------------------------------------------

def render_admin_summary(
    engineer_results: dict,
    run_timestamp: Optional[str] = None,
) -> str:
    """Render an admin summary DM listing all engineer outcomes.

    Args:
        engineer_results: Dict mapping engineer name → {"success": bool, "error": str | None}
        run_timestamp:    Optional ISO timestamp string for the run.

    Returns:
        Markdown string for admin DM.
    """
    if run_timestamp is None:
        run_timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    successes = [n for n, r in engineer_results.items() if r.get("success")]
    failures = [n for n, r in engineer_results.items() if not r.get("success")]

    lines: List[str] = []
    lines.append("# 🤖 KPI Pipeline Run Summary")
    lines.append(f"*Run at: {run_timestamp}*")
    lines.append("")
    lines.append(
        f"**Results:** {len(successes)} ✅ succeeded, {len(failures)} ❌ failed"
        f" (total: {len(engineer_results)})"
    )
    lines.append("")

    if successes:
        lines.append("### ✅ Delivered Reports")
        for name in successes:
            lines.append(f"- {name}")
        lines.append("")

    if failures:
        lines.append("### ❌ Failed Engineers")
        for name in failures:
            error = engineer_results[name].get("error", "unknown error")
            lines.append(f"- **{name}**: {error}")
        lines.append("")

    return "\n".join(lines)
