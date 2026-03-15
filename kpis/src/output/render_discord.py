"""
Discord report renderer for the KPI pipeline.

Produces Discord-optimised message text for:
  - render_engineer_report()  — per-engineer DM with KPI values + validation evidence
  - render_manager_summary()  — consolidated team summary for a manager channel

Key differences from render_markdown.py:
  - Discord does NOT render Markdown table syntax (pipes become literal text).
    The manager summary uses a monospaced code block for the team table.
  - Headers (#) are not rendered in Discord chat — bold (**) is used instead.
  - Evidence lists are included inline beneath each relevant metric.
  - Long evidence lists are truncated to 10 items followed by "... +N more".
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import Engineer, Manager
from utils.dates import format_period
from utils.safe_run import MetricResult

_MAX_EVIDENCE_ITEMS = 10


# ---------------------------------------------------------------------------
# Engineer DM report
# ---------------------------------------------------------------------------

def render_engineer_report(
    engineer: Engineer,
    metrics: List[MetricResult],
    current_period: tuple,
    previous_period: tuple,
    evidence: Dict[str, List],
) -> str:
    """Render a Discord DM report for one engineer.

    Args:
        engineer:        Engineer dataclass instance.
        metrics:         List of MetricResult objects (one per KPI).
        current_period:  (start: datetime, end: datetime).
        previous_period: (start: datetime, end: datetime).
        evidence:        Dict produced by build_evidence() in main.py.

    Returns:
        Discord-ready string (may be chunked by DiscordClient before sending).
    """
    curr_start, curr_end = current_period
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    period_days = (curr_end - curr_start).days
    curr_label = f"{curr_start.strftime('%b %d')} → {curr_end.strftime('%b %d, %Y')}"

    # Index metrics by name for easy lookup
    by_name = {m.name: m for m in metrics}

    lines: List[str] = []

    lines.append(f"📊 **KPI Report — {engineer.name}**")
    lines.append(f"📅 Period: {curr_label} ({period_days} days)")
    lines.append("")

    # ---- GitHub ----
    lines.append("**GitHub**")
    lines.append(_metric_line(
        "🔀 PR Merge Throughput",
        by_name.get("My PR Merge Throughput"),
        evidence.get("merged_pr_numbers", []),
        fmt="#{}",
    ))
    lines.append(_metric_line(
        "👀 Review Count",
        by_name.get("My Review Count"),
        evidence.get("reviewed_pr_numbers", []),
        fmt="#{}",
    ))
    lines.append(_metric_line(
        "⚡ Code Review Speed",
        by_name.get("My Code Review Speed"),
        [],   # no per-item evidence for speed
    ))
    lines.append(_metric_line(
        "🤖 CI Reliability",
        by_name.get("My CI Reliability"),
        [],
    ))
    lines.append("")

    # ---- Jira ----
    lines.append("**Jira**")
    lines.append(_metric_line(
        "⏱️ Cycle Time",
        by_name.get("My Cycle Time"),
        evidence.get("jira_ticket_keys", []),
        fmt="{}",
    ))
    lines.append(_metric_line(
        "📦 Story Points",
        by_name.get("My Resolved Contribution"),
        evidence.get("jira_ticket_keys", []),
        fmt="{}",
    ))
    lines.append("")

    # ---- Rollbar ----
    lines.append("**Rollbar**")
    lines.append(_metric_line(
        "🐛 Errors Attributed",
        by_name.get("Errors Attributed to My Changes"),
        evidence.get("rollbar_item_ids", []),
        fmt="Item #{}",
    ))
    lines.append(_metric_line(
        "🔧 MTTR",
        by_name.get("My MTTR"),
        [],
    ))
    lines.append("")

    lines.append(f"*Generated: {now_str}*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Manager summary report
# ---------------------------------------------------------------------------

def render_manager_summary(
    engineer_reports: List[Dict[str, Any]],
    manager: Optional[Manager],
    run_timestamp: Optional[str] = None,
) -> str:
    """Render a team summary for the manager's Discord channel.

    Args:
        engineer_reports: List of dicts, each containing:
                          {eng, metrics, evidence, current_period, success}
        manager:          Manager dataclass (name used in header).
        run_timestamp:    Optional override timestamp string.

    Returns:
        Discord-ready string.
    """
    if run_timestamp is None:
        run_timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    manager_name = manager.name if manager else "Manager"
    total = len(engineer_reports)
    succeeded = sum(1 for r in engineer_reports if r.get("success"))
    failed = total - succeeded

    lines: List[str] = []

    lines.append(f"🤖 **Team KPI Summary** — {manager_name}")
    lines.append(f"*Run: {run_timestamp} | Engineers: {total} | ✅ {succeeded} succeeded, ❌ {failed} failed*")
    lines.append("")

    # ---- Team averages table (code block for monospaced alignment) ----
    successful_reports = [r for r in engineer_reports if r.get("success") and r.get("metrics")]
    if successful_reports:
        lines.append("**Team Overview (averages)**")
        lines.append(_build_team_table(successful_reports))
        lines.append("")

    # ---- Per-engineer detail ----
    lines.append("**Individual Reports**")
    for report in engineer_reports:
        eng = report.get("eng")
        if not eng:
            continue
        if not report.get("success"):
            lines.append(f"❌ **{eng.name}** — failed: {report.get('error', 'unknown error')}")
            continue

        metrics: List[MetricResult] = report.get("metrics", [])
        by_name = {m.name: m for m in metrics}

        pr_val = _val_str(by_name.get("My PR Merge Throughput"))
        rev_val = _val_str(by_name.get("My Review Count"))
        ct_val = _val_str(by_name.get("My Cycle Time"))
        pts_val = _val_str(by_name.get("My Resolved Contribution"))
        err_val = _val_str(by_name.get("Errors Attributed to My Changes"))
        mttr_val = _val_str(by_name.get("My MTTR"))

        lines.append(
            f"✅ **{eng.name}** — "
            f"PRs: {pr_val} | Reviews: {rev_val} | Cycle: {ct_val} | "
            f"Points: {pts_val} | Errors: {err_val} | MTTR: {mttr_val}"
        )

    lines.append("")
    lines.append("*Full individual reports were sent as direct messages to each engineer.*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _metric_line(
    label: str,
    metric: Optional[MetricResult],
    evidence: List,
    fmt: str = "{}",
) -> str:
    """Format one metric line with optional evidence list below it."""
    if metric is None or metric.error:
        error_msg = metric.error if metric else "not available"
        return f"  {label}: N/A _{error_msg}_"

    curr_str = _fmt_value(metric.current_value, metric.unit)
    prev_str = _fmt_value(metric.previous_value, metric.unit)
    change_str = _fmt_change(metric.pct_change, metric.lower_is_better)

    if metric.previous_value is not None:
        main = f"  {label}: **{curr_str}**  (prev: {prev_str}, {change_str})"
    else:
        main = f"  {label}: **{curr_str}**"

    if not evidence:
        return main

    ev_items = [fmt.format(item) for item in evidence[:_MAX_EVIDENCE_ITEMS]]
    ev_str = ", ".join(ev_items)
    if len(evidence) > _MAX_EVIDENCE_ITEMS:
        ev_str += f" ... +{len(evidence) - _MAX_EVIDENCE_ITEMS} more"

    return f"{main}\n    Evidence: {ev_str}"


def _fmt_value(value: Optional[float], unit: str) -> str:
    if value is None:
        return "N/A"
    if unit == "%":
        return f"{value:.1f}%"
    if unit in ("days", "hours"):
        return f"{value:.1f}{unit[0]}"   # "3.5d", "12.5h"
    if unit == "pts":
        return f"{value:.0f} pts"
    if unit == "count":
        return str(int(value))
    return f"{value} {unit}".strip()


def _fmt_change(pct_change: Optional[float], lower_is_better: bool) -> str:
    if pct_change is None:
        return "—"
    sign = "+" if pct_change >= 0 else ""
    improvement = (pct_change < 0 and lower_is_better) or (pct_change > 0 and not lower_is_better)
    arrow = "✅" if improvement else "⚠️"
    return f"{arrow} {sign}{pct_change:.1f}%"


def _val_str(metric: Optional[MetricResult]) -> str:
    """One-word value string for manager table cells."""
    if metric is None or metric.error or metric.current_value is None:
        return "N/A"
    return _fmt_value(metric.current_value, metric.unit)


def _build_team_table(reports: List[Dict[str, Any]]) -> str:
    """Build a monospaced code-block table for Discord.

    Columns: Engineer | PRs | Reviews | Cycle | Points | Errors | MTTR
    """
    header = f"{'Engineer':<18} {'PRs':>4} {'Reviews':>7} {'Cycle':>6} {'Points':>7} {'Errors':>7} {'MTTR':>7}"
    sep = "-" * len(header)

    rows = [header, sep]

    avg_accum: Dict[str, List[float]] = {
        "prs": [], "reviews": [], "cycle": [], "points": [], "errors": [], "mttr": []
    }

    for report in reports:
        eng = report["eng"]
        metrics: List[MetricResult] = report.get("metrics", [])
        by_name = {m.name: m for m in metrics}

        def get_val(name: str) -> Optional[float]:
            m = by_name.get(name)
            return m.current_value if m and not m.error else None

        pr_v = get_val("My PR Merge Throughput")
        rev_v = get_val("My Review Count")
        ct_v = get_val("My Cycle Time")
        pts_v = get_val("My Resolved Contribution")
        err_v = get_val("Errors Attributed to My Changes")
        mttr_v = get_val("My MTTR")

        def cell(v, unit="count"):
            if v is None:
                return "N/A"
            m = MetricResult(name="", current_value=v, previous_value=None,
                             pct_change=None, unit=unit)
            return _fmt_value(v, unit)

        name_trunc = eng.name[:17]
        row = (
            f"{name_trunc:<18}"
            f" {cell(pr_v):>4}"
            f" {cell(rev_v):>7}"
            f" {cell(ct_v, 'days'):>6}"
            f" {cell(pts_v, 'pts'):>7}"
            f" {cell(err_v):>7}"
            f" {cell(mttr_v, 'hours'):>7}"
        )
        rows.append(row)

        for key, val in [
            ("prs", pr_v), ("reviews", rev_v), ("cycle", ct_v),
            ("points", pts_v), ("errors", err_v), ("mttr", mttr_v),
        ]:
            if val is not None:
                avg_accum[key].append(val)

    # Averages row (only if > 1 engineer)
    if len(reports) > 1:
        rows.append(sep)

        def avg_cell(key, unit="count"):
            vals = avg_accum[key]
            if not vals:
                return "N/A"
            return _fmt_value(sum(vals) / len(vals), unit)

        rows.append(
            f"{'TEAM AVG':<18}"
            f" {avg_cell('prs'):>4}"
            f" {avg_cell('reviews'):>7}"
            f" {avg_cell('cycle', 'days'):>6}"
            f" {avg_cell('points', 'pts'):>7}"
            f" {avg_cell('errors'):>7}"
            f" {avg_cell('mttr', 'hours'):>7}"
        )

    return "```\n" + "\n".join(rows) + "\n```"
