"""
KPI Pipeline Orchestrator — main entry point.

Run:
    python kpis/src/main.py --period_days 90 --trend_days 90
    python kpis/src/main.py --dry_run   # prints reports; skips Sheets + Discord

Flow per engineer:
    1. Fetch data from GitHub (always), Jira (optional), Rollbar (optional)
    2. Compute 8 MetricResult objects (current + previous period each)
    3. Collect validation evidence (PR numbers, Jira keys, Rollbar IDs)
    4. Render Markdown report + write CSV
    5. Write metrics to engineer's Google Sheet worksheet tab (live runs only)

After all engineers:
    6. Send each engineer their Discord DM report (if DISCORD_BOT_TOKEN set)
    7. Send manager the team summary via Discord channel
    8. Print admin summary to stdout (visible in GitHub Actions run log).
    Exit 0 if at least one report delivered; exit 1 if all failed.
"""

from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ---- Local imports (all paths relative to kpis/src/) ----
from config import AppConfig, Engineer, Manager, load_app_config, missing_sheets_vars
from utils.logging import configure_logging, get_logger
from utils.dates import get_periods, format_period
from utils.safe_run import MetricResult, EngineerError

from clients.github_client import GitHubClient
from clients.jira_client import JiraClient
from clients.rollbar_client import RollbarClient
from clients.google_sheets_client import GoogleSheetsClient
from delivery.discord_client import DiscordClient

from metrics import cycle_time
from metrics import resolved_contribution
from metrics import pr_merge_throughput
from metrics import review_contribution
from metrics import code_review_speed
from metrics import errors_attributed
from metrics import mttr
from metrics import ci_reliability

from output.render_markdown import render, render_admin_summary
from output.render_discord import render_engineer_report as render_discord_engineer
from output.render_discord import render_manager_summary as render_discord_manager
from output.write_csv import write as write_csv

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Personal KPI Dashboard — per-engineer growth metrics."
    )
    parser.add_argument(
        "--period_days",
        type=int,
        default=90,
        help="Rolling window size in days (default: 90).",
    )
    parser.add_argument(
        "--trend_days",
        type=int,
        default=90,
        help="Comparison window size in days (default: 90, same as period_days).",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        default=False,
        help="Print reports to stdout instead of writing to Google Sheets.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="/tmp",
        help="Directory to write CSV reports (default: /tmp).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Safe data-fetch helpers
# ---------------------------------------------------------------------------

def _safe_fetch(func, *args, source_name: str, eng_error: EngineerError, **kwargs) -> Any:
    """Call a data-fetch function; on failure log and record the error.

    Returns the function's result, or None on failure.  The caller should
    treat None as "data unavailable for this source."
    """
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        logger.error("[%s] %s data fetch failed: %s", eng_error.engineer_name, source_name, msg)
        eng_error.add_source_error(source_name, msg)
        return None


def _build_metric(
    name: str,
    compute_fn,
    *args,
    unit: str,
    lower_is_better: bool,
    **kwargs,
) -> MetricResult:
    """Call a metric compute function; wrap in MetricResult on failure."""
    try:
        value = compute_fn(*args, **kwargs)
        return MetricResult(
            name=name,
            current_value=value,
            previous_value=None,   # filled in by caller after both periods computed
            pct_change=None,
            unit=unit,
            lower_is_better=lower_is_better,
        )
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        logger.error("Metric [%s] computation failed: %s", name, msg)
        return MetricResult.error_result(name, unit, lower_is_better, msg)


def _merge_periods(
    current_result: MetricResult,
    previous_result: MetricResult,
) -> MetricResult:
    """Combine current and previous period MetricResults into one object."""
    # Prefer current result's metadata; overlay previous value
    curr_val = current_result.current_value
    prev_val = previous_result.current_value if previous_result and not previous_result.error else None
    pct = MetricResult.compute_pct_change(curr_val, prev_val)
    error = current_result.error or (previous_result.error if previous_result else None)
    return MetricResult(
        name=current_result.name,
        current_value=curr_val,
        previous_value=prev_val,
        pct_change=pct,
        unit=current_result.unit,
        lower_is_better=current_result.lower_is_better,
        error=error,
    )


# ---------------------------------------------------------------------------
# Per-engineer pipeline
# ---------------------------------------------------------------------------

def process_engineer(
    eng: Engineer,
    cfg: AppConfig,
    current_period: Tuple,
    previous_period: Tuple,
    dry_run: bool,
    output_dir: str,
    sheets: Optional[GoogleSheetsClient],
) -> Optional[Dict]:
    """Run the full pipeline for one engineer.

    Returns a result dict on success:
        {
            "success": True,
            "metrics": List[MetricResult],
            "evidence": {
                "merged_pr_numbers": [int],
                "reviewed_pr_numbers": [int],
                "jira_ticket_keys": [str],
                "rollbar_item_ids": [int],
            },
            "md_report": str,
        }
    Returns {"success": False, "error": str} on fatal failure.
    In dry_run mode ``sheets`` is None and no Sheets write is attempted.
    """
    eng_error = EngineerError(engineer_name=eng.name)
    curr_start, curr_end = current_period
    prev_start, prev_end = previous_period

    logger.info("─── Processing engineer: %s ───", eng.name)

    # ----------------------------------------------------------------
    # 1. Initialise clients (failure here is per-source, not fatal)
    # ----------------------------------------------------------------
    gh_client: Optional[GitHubClient] = None
    jira_client_inst: Optional[JiraClient] = None
    rb_client: Optional[RollbarClient] = None

    try:
        gh_client = GitHubClient(token=cfg.gh_token, repo_full_name=cfg.github_repo)
    except Exception as exc:
        eng_error.add_source_error("github_init", str(exc))
        logger.error("Failed to init GitHubClient for %s: %s", eng.name, exc)

    try:
        jira_client_inst = JiraClient(
            base_url=cfg.jira_base_url,
            email=cfg.jira_email,
            api_token=cfg.jira_api_token,
        )
    except Exception as exc:
        eng_error.add_source_error("jira_init", str(exc))
        logger.warning("Failed to init JiraClient for %s: %s", eng.name, exc)

    if cfg.rollbar_token:
        try:
            rb_client = RollbarClient(
                token=cfg.rollbar_token,
                project_id=cfg.rollbar_project,
                environment=cfg.rollbar_env,
            )
        except Exception as exc:
            eng_error.add_source_error("rollbar_init", str(exc))
            logger.warning("Failed to init RollbarClient for %s: %s", eng.name, exc)

    # ----------------------------------------------------------------
    # 2. Fetch data for both periods
    # ----------------------------------------------------------------

    # --- GitHub ---
    merged_prs_curr: List = []
    merged_prs_prev: List = []
    reviewed_prs_curr: List = []
    reviewed_prs_prev: List = []
    workflow_runs_curr: List = []
    workflow_runs_prev: List = []

    if gh_client and eng.github_login:
        merged_prs_curr = _safe_fetch(
            gh_client.get_merged_prs, eng.github_login, curr_start, curr_end,
            source_name="github_merged_prs_curr", eng_error=eng_error,
        ) or []
        merged_prs_prev = _safe_fetch(
            gh_client.get_merged_prs, eng.github_login, prev_start, prev_end,
            source_name="github_merged_prs_prev", eng_error=eng_error,
        ) or []
        reviewed_prs_curr = _safe_fetch(
            gh_client.get_prs_reviewed_by, eng.github_login, curr_start, curr_end,
            source_name="github_reviewed_prs_curr", eng_error=eng_error,
        ) or []
        reviewed_prs_prev = _safe_fetch(
            gh_client.get_prs_reviewed_by, eng.github_login, prev_start, prev_end,
            source_name="github_reviewed_prs_prev", eng_error=eng_error,
        ) or []
        workflow_runs_curr = _safe_fetch(
            gh_client.get_workflow_runs_for_prs, merged_prs_curr,
            source_name="github_workflow_runs_curr", eng_error=eng_error,
        ) or []
        workflow_runs_prev = _safe_fetch(
            gh_client.get_workflow_runs_for_prs, merged_prs_prev,
            source_name="github_workflow_runs_prev", eng_error=eng_error,
        ) or []

    # --- Jira ---
    jira_issues_curr: List = []
    jira_issues_prev: List = []

    if jira_client_inst and eng.jira_account_id:
        jira_issues_curr = _safe_fetch(
            jira_client_inst.get_resolved_issues, eng.jira_account_id, curr_start, curr_end,
            source_name="jira_issues_curr", eng_error=eng_error,
        ) or []
        jira_issues_prev = _safe_fetch(
            jira_client_inst.get_resolved_issues, eng.jira_account_id, prev_start, prev_end,
            source_name="jira_issues_prev", eng_error=eng_error,
        ) or []

    # --- Rollbar ---
    rb_items_curr: List = []
    rb_items_prev: List = []

    if rb_client and eng.rollbar_identity:
        rb_items_curr = _safe_fetch(
            rb_client.get_items, curr_start, curr_end,
            source_name="rollbar_items_curr", eng_error=eng_error,
        ) or []
        rb_items_prev = _safe_fetch(
            rb_client.get_items, prev_start, prev_end,
            source_name="rollbar_items_prev", eng_error=eng_error,
        ) or []

    # ----------------------------------------------------------------
    # 2b. Build validation evidence from raw current-period data
    # ----------------------------------------------------------------

    # Rollbar items attributed to this engineer (for Errors Attributed evidence)
    identity_lower = eng.rollbar_identity.lower()
    attributed_rollbar_ids = [
        item["id"]
        for item in rb_items_curr
        if RollbarClient.extract_blame_identity(item) is not None
        and RollbarClient.extract_blame_identity(item).lower() == identity_lower
    ]

    evidence: Dict[str, List] = {
        "merged_pr_numbers": [pr.number for pr in merged_prs_curr],
        "reviewed_pr_numbers": [pr.number for pr in reviewed_prs_curr],
        "jira_ticket_keys": [
            getattr(issue, "key", str(issue)) for issue in jira_issues_curr
        ],
        "rollbar_item_ids": attributed_rollbar_ids,
    }

    # ----------------------------------------------------------------
    # 3. Compute metrics for both periods, then merge
    # ----------------------------------------------------------------

    def has_jira_error() -> Optional[str]:
        """Return error string if any Jira fetch failed."""
        for key, err in eng_error.source_errors.items():
            if "jira" in key:
                return err
        return None

    def has_gh_error() -> Optional[str]:
        for key, err in eng_error.source_errors.items():
            if "github" in key:
                return err
        return None

    def has_rb_error() -> Optional[str]:
        for key, err in eng_error.source_errors.items():
            if "rollbar" in key:
                return err
        return None

    # Helper to build a failed metric when source is unavailable
    def source_error_metric(name: str, unit: str, lib: bool, source_error: Optional[str]) -> MetricResult:
        if source_error:
            return MetricResult.error_result(name, unit, lib, source_error)
        return MetricResult.error_result(name, unit, lib, "No data (source not configured)")

    metrics: List[MetricResult] = []

    # ---- Metric 1: My Cycle Time (Jira) ----
    jira_err = has_jira_error()
    if jira_err or not jira_issues_curr:
        m_curr = source_error_metric("My Cycle Time", "days", True, jira_err)
        m_prev = source_error_metric("My Cycle Time", "days", True, jira_err)
    else:
        m_curr = _build_metric(
            "My Cycle Time", cycle_time.compute, jira_issues_curr,
            unit="days", lower_is_better=True,
        )
        m_prev = _build_metric(
            "My Cycle Time", cycle_time.compute, jira_issues_prev,
            unit="days", lower_is_better=True,
        )
    metrics.append(_merge_periods(m_curr, m_prev))

    # ---- Metric 2: My Resolved Contribution (Jira) ----
    if jira_err:
        metrics.append(source_error_metric("My Resolved Contribution", "pts", False, jira_err))
    else:
        m_curr = _build_metric(
            "My Resolved Contribution", resolved_contribution.compute,
            jira_issues_curr, cfg.jira_story_points_field,
            unit="pts", lower_is_better=False,
        )
        m_prev = _build_metric(
            "My Resolved Contribution", resolved_contribution.compute,
            jira_issues_prev, cfg.jira_story_points_field,
            unit="pts", lower_is_better=False,
        )
        metrics.append(_merge_periods(m_curr, m_prev))

    # ---- Metric 3: My PR Merge Throughput (GitHub) ----
    gh_err = has_gh_error()
    if gh_err and not merged_prs_curr:
        metrics.append(source_error_metric("My PR Merge Throughput", "count", False, gh_err))
    else:
        m_curr = _build_metric(
            "My PR Merge Throughput", pr_merge_throughput.compute, merged_prs_curr,
            unit="count", lower_is_better=False,
        )
        m_prev = _build_metric(
            "My PR Merge Throughput", pr_merge_throughput.compute, merged_prs_prev,
            unit="count", lower_is_better=False,
        )
        metrics.append(_merge_periods(m_curr, m_prev))

    # ---- Metric 4: My Review Contribution (GitHub) ----
    if gh_err and not reviewed_prs_curr:
        metrics.append(source_error_metric("My Review Count", "count", False, gh_err))
        metrics.append(source_error_metric("My Avg Time to First Review", "hours", True, gh_err))
    else:
        # review_contribution returns (count, avg_hours) — split into two metrics
        def _review_count(prs, login, client):
            count, _ = review_contribution.compute(prs, login, client)
            return float(count)

        def _review_avg_hours(prs, login, client):
            _, avg = review_contribution.compute(prs, login, client)
            return avg

        m_curr_count = _build_metric(
            "My Review Count", _review_count,
            reviewed_prs_curr, eng.github_login, gh_client,
            unit="count", lower_is_better=False,
        )
        m_prev_count = _build_metric(
            "My Review Count", _review_count,
            reviewed_prs_prev, eng.github_login, gh_client,
            unit="count", lower_is_better=False,
        )
        metrics.append(_merge_periods(m_curr_count, m_prev_count))

        m_curr_speed = _build_metric(
            "My Avg Time to First Review", _review_avg_hours,
            reviewed_prs_curr, eng.github_login, gh_client,
            unit="hours", lower_is_better=True,
        )
        m_prev_speed = _build_metric(
            "My Avg Time to First Review", _review_avg_hours,
            reviewed_prs_prev, eng.github_login, gh_client,
            unit="hours", lower_is_better=True,
        )
        metrics.append(_merge_periods(m_curr_speed, m_prev_speed))

    # ---- Metric 5: My Code Review Speed (GitHub) ----
    if gh_err and not reviewed_prs_curr:
        metrics.append(source_error_metric("My Code Review Speed", "hours", True, gh_err))
    else:
        m_curr = _build_metric(
            "My Code Review Speed", code_review_speed.compute,
            reviewed_prs_curr, eng.github_login, gh_client,
            unit="hours", lower_is_better=True,
        )
        m_prev = _build_metric(
            "My Code Review Speed", code_review_speed.compute,
            reviewed_prs_prev, eng.github_login, gh_client,
            unit="hours", lower_is_better=True,
        )
        metrics.append(_merge_periods(m_curr, m_prev))

    # ---- Metric 6: Errors Attributed to My Changes (Rollbar) ----
    rb_err = has_rb_error()
    if rb_err and not rb_items_curr:
        metrics.append(source_error_metric("Errors Attributed to My Changes", "count", True, rb_err))
    else:
        m_curr = _build_metric(
            "Errors Attributed to My Changes", errors_attributed.compute,
            rb_items_curr, eng.rollbar_identity,
            unit="count", lower_is_better=True,
        )
        m_prev = _build_metric(
            "Errors Attributed to My Changes", errors_attributed.compute,
            rb_items_prev, eng.rollbar_identity,
            unit="count", lower_is_better=True,
        )
        metrics.append(_merge_periods(m_curr, m_prev))

    # ---- Metric 7: My MTTR (Rollbar) ----
    if rb_err and not rb_items_curr:
        metrics.append(source_error_metric("My MTTR", "hours", True, rb_err))
    else:
        m_curr = _build_metric(
            "My MTTR", mttr.compute,
            rb_items_curr, eng.rollbar_identity, jira_client_inst,
            unit="hours", lower_is_better=True,
        )
        m_prev = _build_metric(
            "My MTTR", mttr.compute,
            rb_items_prev, eng.rollbar_identity, jira_client_inst,
            unit="hours", lower_is_better=True,
        )
        metrics.append(_merge_periods(m_curr, m_prev))

    # ---- Metric 8: My CI Reliability (GitHub) ----
    if gh_err and not workflow_runs_curr:
        metrics.append(source_error_metric("My CI Reliability", "%", False, gh_err))
    else:
        m_curr = _build_metric(
            "My CI Reliability", ci_reliability.compute, workflow_runs_curr,
            unit="%", lower_is_better=False,
        )
        m_prev = _build_metric(
            "My CI Reliability", ci_reliability.compute, workflow_runs_prev,
            unit="%", lower_is_better=False,
        )
        metrics.append(_merge_periods(m_curr, m_prev))

    # ----------------------------------------------------------------
    # 4. Render & deliver (CSV + optional Google Sheets)
    # ----------------------------------------------------------------
    try:
        md_report = render(eng, metrics, current_period, previous_period)
        csv_path = write_csv(eng, metrics, current_period, previous_period, output_dir)
        logger.info("CSV saved: %s", csv_path)

        if dry_run:
            # Dry-run: print the full Markdown report to stdout (Actions log).
            # No Sheets credentials required; Discord delivery also skipped.
            print(md_report)
            logger.info("[DRY RUN] Report printed for %s — Sheets/Discord write skipped", eng.name)
        else:
            sheets_ok = sheets.write_report(
                tab_name=eng.google_sheet_tab,
                engineer_name=eng.name,
                metrics=metrics,
                current_period=current_period,
                previous_period=previous_period,
            )
            if not sheets_ok:
                logger.error(
                    "Failed to write Google Sheet report for %s (tab: %s)",
                    eng.name,
                    eng.google_sheet_tab,
                )
                return {"success": False, "error": "Google Sheets write failed"}

        logger.info("Report computed for %s", eng.name)
        return {
            "success": True,
            "metrics": metrics,
            "evidence": evidence,
            "md_report": md_report,
            "current_period": current_period,
            "previous_period": previous_period,
        }

    except Exception as exc:
        logger.error("Failed to render/deliver report for %s: %s", eng.name, exc)
        logger.debug(traceback.format_exc())
        return {"success": False, "error": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    configure_logging()
    args = parse_args()

    logger.info(
        "KPI Pipeline starting | period=%dd trend=%dd dry_run=%s",
        args.period_days,
        args.trend_days,
        args.dry_run,
    )

    # Load config (raises EnvironmentError with clear message if secrets missing)
    try:
        cfg = load_app_config()
    except (EnvironmentError, FileNotFoundError) as exc:
        logger.critical("Configuration error: %s", exc)
        sys.exit(1)

    if not cfg.engineers:
        logger.critical("No engineers found in engineers.yml — nothing to do.")
        sys.exit(1)

    # Compute date windows
    current_period, previous_period = get_periods(args.period_days)
    logger.info(
        "Current period:  %s", format_period(*current_period)
    )
    logger.info(
        "Previous period: %s", format_period(*previous_period)
    )

    # Initialise Google Sheets client (live runs only)
    sheets: Optional[GoogleSheetsClient] = None
    if not args.dry_run:
        missing = missing_sheets_vars()
        if missing:
            logger.critical(
                "Live run requires Google Sheets credentials but these are not set:\n%s\n"
                "Use --dry_run to run without Google Sheets.",
                "\n".join(f"  • {v}" for v in missing),
            )
            sys.exit(1)
        sheets = GoogleSheetsClient(
            service_account_json=cfg.google_service_account_json,
            sheet_id=cfg.google_sheet_id,
        )

    # Initialise Discord client (skipped in dry_run or when token absent)
    discord: Optional[DiscordClient] = None
    if not args.dry_run and cfg.discord_bot_token:
        discord = DiscordClient(bot_token=cfg.discord_bot_token, dry_run=False)
    elif args.dry_run and cfg.discord_bot_token:
        discord = DiscordClient(bot_token=cfg.discord_bot_token, dry_run=True)
    else:
        logger.info(
            "Discord delivery disabled — DISCORD_BOT_TOKEN not set%s",
            " (dry_run)" if args.dry_run else "",
        )

    # ---- Per-engineer processing (KPI compute + CSV + Sheets) ----
    engineer_results: Dict[str, Dict] = {}
    eng_map: Dict[str, Engineer] = {}

    for eng in cfg.engineers:
        eng_map[eng.name] = eng
        try:
            result = process_engineer(
                eng=eng,
                cfg=cfg,
                current_period=current_period,
                previous_period=previous_period,
                dry_run=args.dry_run,
                output_dir=args.output_dir,
                sheets=sheets,
            )
            engineer_results[eng.name] = result or {"success": False, "error": "no result"}
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.error("Unexpected failure for engineer %s: %s", eng.name, error_msg)
            logger.debug(traceback.format_exc())
            engineer_results[eng.name] = {"success": False, "error": error_msg}

    # ---- Discord delivery (after all engineers have been processed) ----
    if discord is not None:
        for eng in cfg.engineers:
            result = engineer_results.get(eng.name, {})
            if not result.get("success"):
                logger.warning("Skipping Discord DM for %s — processing failed", eng.name)
                continue

            discord_report = render_discord_engineer(
                engineer=eng,
                metrics=result["metrics"],
                current_period=result["current_period"],
                previous_period=result["previous_period"],
                evidence=result["evidence"],
            )
            discord.send_engineer_report(
                discord_user_id=eng.discord_user_id,
                report_markdown=discord_report,
                engineer_name=eng.name,
            )

        # ---- Manager summary ----
        if cfg.manager and cfg.manager.discord_channel_id:
            manager_reports = [
                {
                    "eng": eng_map[name],
                    "metrics": res.get("metrics", []),
                    "evidence": res.get("evidence", {}),
                    "current_period": res.get("current_period", current_period),
                    "success": res.get("success", False),
                    "error": res.get("error"),
                }
                for name, res in engineer_results.items()
                if name in eng_map
            ]
            manager_summary = render_discord_manager(
                engineer_reports=manager_reports,
                manager=cfg.manager,
            )
            discord.send_manager_summary(
                channel_id=cfg.manager.discord_channel_id,
                summary_markdown=manager_summary,
                manager_name=cfg.manager.name,
            )
        else:
            logger.info("No manager Discord channel configured — skipping manager summary")

    # ---- Admin summary (printed to stdout / GitHub Actions log) ----
    summary = render_admin_summary(engineer_results)
    print("\n" + "=" * 70)
    if args.dry_run:
        print("[DRY RUN] Admin Summary")
    else:
        print("Admin Summary")
    print("=" * 70)
    print(summary)

    # ---- Exit code ----
    any_success = any(v.get("success") for v in engineer_results.values())
    if any_success:
        logger.info("Pipeline finished. At least one report delivered.")
        sys.exit(0)
    else:
        logger.error("Pipeline finished. ALL engineers failed — check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
