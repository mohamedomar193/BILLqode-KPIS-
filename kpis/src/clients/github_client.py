"""
GitHub API client for the KPI pipeline.

Wraps PyGithub for standard REST calls and falls back to raw requests for
endpoints not yet exposed by PyGithub (e.g. timeline events).

Key design choices:
- Rate limiting: after every paginated call we check remaining quota and sleep
  if < 50 requests remain, to avoid hitting secondary rate limits.
- Caching: per-run in-memory cache for expensive calls (PR lists, workflow runs)
  keyed by (login, period_key).
- All timestamps returned as UTC-aware datetime objects.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from github import Github, GithubException
from github.PullRequest import PullRequest
from github.PullRequestReview import PullRequestReview
from github.WorkflowRun import WorkflowRun

from utils.dates import parse_iso, in_period
from utils.logging import get_logger

logger = get_logger(__name__)

# Seconds to sleep when GitHub rate limit is nearly exhausted
_RATE_LIMIT_SLEEP_SECS = 60
_RATE_LIMIT_THRESHOLD = 50


class GitHubClient:
    """Thin wrapper around PyGithub + raw GitHub REST API."""

    def __init__(self, token: str, repo_full_name: str) -> None:
        """
        Args:
            token:          GitHub personal access token (or Actions GITHUB_TOKEN).
            repo_full_name: Repository in "owner/repo" format.
        """
        self._gh = Github(token, per_page=100)
        self._token = token
        self._repo_full_name = repo_full_name
        self._repo = self._gh.get_repo(repo_full_name)
        # Simple in-memory caches keyed by tuple
        self._pr_cache: Dict[Tuple, List[PullRequest]] = {}
        self._run_cache: Dict[str, List[WorkflowRun]] = {}
        logger.info("GitHubClient initialised for repo %s", repo_full_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_rate_limit(self) -> None:
        """Sleep if GitHub API rate limit is nearly exhausted."""
        try:
            remaining = self._gh.get_rate_limit().core.remaining
            if remaining < _RATE_LIMIT_THRESHOLD:
                logger.warning(
                    "GitHub rate limit low (%d remaining) — sleeping %ds",
                    remaining,
                    _RATE_LIMIT_SLEEP_SECS,
                )
                time.sleep(_RATE_LIMIT_SLEEP_SECS)
        except Exception:
            pass  # Non-fatal; continue

    def _raw_get(self, url: str, accept: Optional[str] = None) -> Any:
        """Make a raw GET request to the GitHub API with the bot token."""
        headers = {
            "Authorization": f"token {self._token}",
            "Accept": accept or "application/vnd.github+json",
        }
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _raw_get_paginated(self, url: str, accept: Optional[str] = None) -> List[dict]:
        """Paginate a raw GitHub REST endpoint until all pages consumed."""
        results: List[dict] = []
        next_url: Optional[str] = url
        headers = {
            "Authorization": f"token {self._token}",
            "Accept": accept or "application/vnd.github+json",
        }
        while next_url:
            self._check_rate_limit()
            resp = requests.get(next_url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict) and "items" in data:
                results.extend(data["items"])
            # Follow Link header for pagination
            next_url = None
            link_header = resp.headers.get("Link", "")
            for part in link_header.split(","):
                part = part.strip()
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip().strip("<>")
                    break
        return results

    # ------------------------------------------------------------------
    # Merged PRs authored by engineer
    # ------------------------------------------------------------------

    def get_merged_prs(
        self, login: str, since: datetime, until: datetime
    ) -> List[PullRequest]:
        """Return PRs authored by `login` that were merged within [since, until).

        Uses the GitHub Search API for efficiency (avoids full PR list scan).
        Results are cached per (login, since, until) within the current run.
        """
        cache_key = (login, since.isoformat(), until.isoformat())
        if cache_key in self._pr_cache:
            return self._pr_cache[cache_key]

        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ")
        query = (
            f"repo:{self._repo_full_name} is:pr is:merged author:{login} "
            f"merged:{since_str}..{until_str}"
        )

        self._check_rate_limit()
        issues = self._gh.search_issues(query=query)
        prs: List[PullRequest] = []
        for issue in issues:
            self._check_rate_limit()
            try:
                pr = self._repo.get_pull(issue.number)
                prs.append(pr)
            except GithubException as exc:
                logger.warning("Could not fetch PR #%d: %s", issue.number, exc)

        self._pr_cache[cache_key] = prs
        logger.debug("get_merged_prs(%s): %d PRs in period", login, len(prs))
        return prs

    # ------------------------------------------------------------------
    # PRs reviewed by engineer (not authored by them)
    # ------------------------------------------------------------------

    def get_prs_reviewed_by(
        self, login: str, since: datetime, until: datetime
    ) -> List[PullRequest]:
        """Return PRs (not authored by login) that login reviewed in period.

        Strategy: search for PRs reviewed by the user in the period using the
        GitHub Search API.  We still need to fetch full PR objects for timeline.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        until_str = until.strftime("%Y-%m-%dT%H:%M:%SZ")
        # Note: "reviewed-by:" only filters by user, date range from "updated"
        query = (
            f"repo:{self._repo_full_name} is:pr reviewed-by:{login} "
            f"-author:{login} updated:{since_str}..{until_str}"
        )

        self._check_rate_limit()
        issues = self._gh.search_issues(query=query)
        prs: List[PullRequest] = []
        for issue in issues:
            self._check_rate_limit()
            try:
                pr = self._repo.get_pull(issue.number)
                prs.append(pr)
            except GithubException as exc:
                logger.warning("Could not fetch reviewed PR #%d: %s", issue.number, exc)

        logger.debug("get_prs_reviewed_by(%s): %d PRs", login, len(prs))
        return prs

    # ------------------------------------------------------------------
    # Reviews on a single PR
    # ------------------------------------------------------------------

    def get_pr_reviews(self, pr: PullRequest) -> List[PullRequestReview]:
        """Return all reviews submitted on a PR."""
        self._check_rate_limit()
        try:
            return list(pr.get_reviews())
        except GithubException as exc:
            logger.warning("get_pr_reviews PR#%d failed: %s", pr.number, exc)
            return []

    # ------------------------------------------------------------------
    # PR comments (issue-level comments, not review comments)
    # ------------------------------------------------------------------

    def get_pr_comments(self, pr: PullRequest) -> List[Any]:
        """Return all issue comments on a PR (author's handle + created_at)."""
        self._check_rate_limit()
        try:
            return list(pr.get_issue_comments())
        except GithubException as exc:
            logger.warning("get_pr_comments PR#%d failed: %s", pr.number, exc)
            return []

    # ------------------------------------------------------------------
    # Timeline events (for ready_for_review detection)
    # ------------------------------------------------------------------

    def get_pr_timeline_events(self, pr: PullRequest) -> List[dict]:
        """Fetch timeline events for a PR using the raw REST API.

        Requires the 'mockingbird-preview' Accept header for draft events.
        Returns a flat list of event dicts with at minimum 'event' and 'created_at'.
        """
        owner, repo = self._repo_full_name.split("/", 1)
        url = f"https://api.github.com/repos/{owner}/{repo}/issues/{pr.number}/timeline"
        try:
            return self._raw_get_paginated(
                url,
                accept="application/vnd.github.mockingbird-preview+json",
            )
        except Exception as exc:
            logger.warning("get_pr_timeline_events PR#%d failed: %s", pr.number, exc)
            return []

    # ------------------------------------------------------------------
    # Ready-for-review time
    # ------------------------------------------------------------------

    def get_ready_for_review_time(self, pr: PullRequest) -> datetime:
        """Return when the PR became ready for review.

        Logic:
        - If the PR was never a draft (pr.draft == False and no 'ready_for_review'
          event), return pr.created_at.
        - If the PR was a draft, look for the 'ready_for_review' timeline event
          and use its timestamp.  Fall back to pr.created_at if not found.
        """
        if not pr.draft:
            # Check if there's a ready_for_review event anyway (previously drafted)
            events = self.get_pr_timeline_events(pr)
            for event in events:
                if event.get("event") == "ready_for_review":
                    ts = parse_iso(event.get("created_at"))
                    if ts:
                        return ts
            # Truly never drafted — use creation time
            return pr.created_at.replace(tzinfo=timezone.utc)

        # Draft PR — scan timeline for the transition
        events = self.get_pr_timeline_events(pr)
        for event in events:
            if event.get("event") == "ready_for_review":
                ts = parse_iso(event.get("created_at"))
                if ts:
                    return ts

        logger.debug("PR#%d still draft or no ready_for_review event; using created_at", pr.number)
        return pr.created_at.replace(tzinfo=timezone.utc)

    # ------------------------------------------------------------------
    # Workflow runs for a list of PRs
    # ------------------------------------------------------------------

    def get_workflow_runs_for_prs(self, prs: List[PullRequest]) -> List[WorkflowRun]:
        """Return workflow runs associated with the head SHAs of the given PRs.

        Uses pr.head.sha to look up runs; deduplicates by run ID.
        Skips PRs whose SHAs have already been queried (in-run cache).

        NOTE: Cancelled / skipped runs are included in the raw list.
        The ci_reliability metric handles them in its denominator logic.
        """
        seen_ids: set = set()
        runs: List[WorkflowRun] = []

        for pr in prs:
            sha = pr.head.sha
            if sha in self._run_cache:
                for run in self._run_cache[sha]:
                    if run.id not in seen_ids:
                        seen_ids.add(run.id)
                        runs.append(run)
                continue

            self._check_rate_limit()
            try:
                sha_runs = list(self._repo.get_workflow_runs(head_sha=sha))
                self._run_cache[sha] = sha_runs
                for run in sha_runs:
                    if run.id not in seen_ids:
                        seen_ids.add(run.id)
                        runs.append(run)
            except GithubException as exc:
                logger.warning("get_workflow_runs SHA %s failed: %s", sha[:8], exc)
                self._run_cache[sha] = []

        logger.debug("get_workflow_runs_for_prs: %d total runs for %d PRs", len(runs), len(prs))
        return runs
