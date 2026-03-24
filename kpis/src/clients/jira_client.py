"""
Jira API client for the KPI pipeline.

Uses the `jira` Python library (basic_auth with API token).

Assumptions / TODOs:
- The Jira instance uses cloud (Atlassian Cloud) REST API v3.
- Workflow has "In Progress" and "Done" status names (customise STATUS_IN_PROGRESS
  / STATUS_DONE if your board uses different names).
- Story points are stored in a custom field whose name is configured via the
  JIRA_STORY_POINTS_FIELD environment variable (e.g. "customfield_10016").
- Cycle time is computed from changelog history fetched per issue.
  Jira Cloud expands changelog automatically when requested.

To adapt for Jira Server / Data Center:
  - Replace basic_auth with token_auth if using PAT.
  - Verify changelog field names match.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from jira import JIRA, JIRAError
from jira.resources import Issue

from utils.dates import parse_iso, in_period
from utils.logging import get_logger

logger = get_logger(__name__)

# Status names — adjust if your board uses different terminology
STATUS_IN_PROGRESS = "In Progress"
STATUS_DONE = "Done"

# JQL date format required by Jira
_JQL_DATE_FMT = "%Y-%m-%d %H:%M"

# Page size for paginated issue fetches
_PAGE_SIZE = 100

# Polite sleep between pages to avoid overloading Jira Cloud rate limits
_PAGE_SLEEP_SECS = 0.5


class JiraClient:
    """Thin wrapper around the jira library for KPI-relevant queries."""

    def __init__(self, base_url: str, email: str, api_token: str) -> None:
        """
        Args:
            base_url:   Jira instance URL, e.g. https://yourorg.atlassian.net
            email:      Atlassian account email used for authentication.
            api_token:  Atlassian API token (from id.atlassian.com/manage-profile/security).
        """
        try:
            self._jira = JIRA(
                server=base_url,
                basic_auth=(email, api_token),
                options={"rest_api_version": 3},
            )
        except JIRAError as exc:
            status = getattr(exc, "status_code", None)
            if status == 401:
                raise PermissionError(
                    f"Jira auth failed (401 Unauthorized) — verify JIRA_EMAIL and JIRA_API_TOKEN. "
                    f"Token must be an Atlassian API token, not a password."
                ) from exc
            if status == 403:
                raise PermissionError(
                    f"Jira auth failed (403 Forbidden) — token is valid but account lacks "
                    f"permission. Ensure the account has Browse Projects access."
                ) from exc
            # For other errors keep a short message (strip HTML body if present)
            short = " ".join(str(exc).split())
            if len(short) > 200:
                short = short[:200] + "..."
            raise JIRAError(short) from exc
        logger.info("JiraClient initialised for %s", base_url)

    # ------------------------------------------------------------------
    # Resolved issues
    # ------------------------------------------------------------------

    def get_resolved_issues(
        self,
        account_id: str,
        since: datetime,
        until: datetime,
    ) -> List[Issue]:
        """Return issues assigned to `account_id` that transitioned to Done in [since, until).

        JQL strategy:
          assignee = <account_id>
          AND statusCategory = Done
          AND status changed to "Done" after <since> before <until>

        Also expands changelog for cycle-time calculations.

        Args:
            account_id: Jira user account ID (stable; preferred over username).
            since:      Period start (UTC-aware datetime).
            until:      Period end (UTC-aware datetime).

        Returns:
            List of Jira Issue objects with changelog expanded.
        """
        since_str = since.strftime(_JQL_DATE_FMT)
        until_str = until.strftime(_JQL_DATE_FMT)

        jql = (
            f'assignee = "{account_id}" '
            f'AND status changed to "{STATUS_DONE}" '
            f'AFTER "{since_str}" '
            f'BEFORE "{until_str}"'
        )

        logger.debug("Jira JQL: %s", jql)
        issues: List[Issue] = []
        start_at = 0

        while True:
            try:
                batch = self._jira.search_issues(
                    jql,
                    startAt=start_at,
                    maxResults=_PAGE_SIZE,
                    expand="changelog",
                )
            except JIRAError as exc:
                status = getattr(exc, "status_code", None)
                if status == 401:
                    raise PermissionError(
                        "Jira auth failed (401 Unauthorized) — check JIRA_EMAIL and JIRA_API_TOKEN."
                    ) from exc
                short = " ".join(str(exc).split())
                if len(short) > 200:
                    short = short[:200] + "..."
                logger.error("JiraClient.get_resolved_issues failed at offset %d: %s", start_at, short)
                raise JIRAError(short) from exc

            issues.extend(batch)
            if len(batch) < _PAGE_SIZE:
                break
            start_at += _PAGE_SIZE
            time.sleep(_PAGE_SLEEP_SECS)

        logger.debug(
            "get_resolved_issues(%s): %d issues in [%s, %s)",
            account_id,
            len(issues),
            since_str,
            until_str,
        )
        return issues

    # ------------------------------------------------------------------
    # Changelog parsing
    # ------------------------------------------------------------------

    def get_issue_changelog(self, issue: Issue) -> List[Dict[str, Any]]:
        """Return a flattened list of status-change events from the issue changelog.

        Each entry is a dict with keys:
            field       : str — e.g. "status"
            from_string : str — e.g. "To Do"
            to_string   : str — e.g. "In Progress"
            created     : datetime (UTC-aware)

        The issue should have been fetched with expand="changelog".
        If changelog is missing, a fresh fetch is attempted.
        """
        if not hasattr(issue, "changelog") or not issue.changelog:
            logger.debug("Fetching changelog for issue %s", issue.key)
            try:
                issue = self._jira.issue(issue.key, expand="changelog")
            except JIRAError as exc:
                logger.warning("Could not fetch changelog for %s: %s", issue.key, exc)
                return []

        events: List[Dict[str, Any]] = []
        for history in issue.changelog.histories:
            created_dt = parse_iso(history.created)
            for item in history.items:
                if item.field == "status":
                    events.append(
                        {
                            "field": item.field,
                            "from_string": item.fromString or "",
                            "to_string": item.toString or "",
                            "created": created_dt,
                        }
                    )

        # Sort chronologically
        events.sort(key=lambda e: e["created"] or datetime.min.replace(tzinfo=timezone.utc))
        return events

    # ------------------------------------------------------------------
    # Story-points helper
    # ------------------------------------------------------------------

    @staticmethod
    def get_story_points(issue: Issue, field_name: str) -> float:
        """Extract story points from a Jira issue using the custom field name.

        Returns 0.0 if the field is absent or None (common for bugs / tasks
        that were not estimated).
        """
        value = getattr(issue.fields, field_name, None)
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
