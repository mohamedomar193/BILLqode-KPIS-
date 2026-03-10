"""
Configuration loader for the KPI pipeline.

Reads all required environment variables (from GitHub Secrets injected into
the Actions runner, or from your shell when running locally).

Also loads the engineer roster from engineers.yml.

Usage:
    from config import load_app_config
    cfg = load_app_config()
    for eng in cfg.engineers:
        print(eng.name, eng.github_login)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml

from utils.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Engineer:
    """Represents one engineer in the roster."""
    name: str
    jira_account_id: str
    github_login: str
    rollbar_identity: str
    google_sheet_tab: str   # Name of the worksheet tab for this engineer


@dataclass
class AppConfig:
    """All runtime configuration, loaded once at startup."""

    # --- Jira ---
    jira_base_url: str
    jira_email: str
    jira_api_token: str
    jira_story_points_field: str  # e.g. "customfield_10016"

    # --- GitHub ---
    gh_token: str
    github_repo: str       # "owner/repo"
    github_org: str = ""   # optional; used for org-level queries

    # --- Rollbar ---
    rollbar_token: str = ""
    rollbar_project: str = ""
    rollbar_env: str = "production"

    # --- Google Sheets ---
    google_service_account_json: str = ""  # raw JSON string of service account key
    google_sheet_id: str = ""              # spreadsheet ID from the URL

    # --- Engineers ---
    engineers: List[Engineer] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Required vs optional env vars
# ---------------------------------------------------------------------------

_REQUIRED_VARS: list[str] = [
    "JIRA_BASE_URL",
    "JIRA_EMAIL",
    "JIRA_API_TOKEN",
    "JIRA_STORY_POINTS_FIELD",
    "GH_TOKEN",
    "GH_REPO",
]

# Required for live runs only (skipped when --dry_run is used)
_SHEETS_REQUIRED_VARS: list[str] = [
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    "GOOGLE_SHEET_ID",
]

_OPTIONAL_VARS: dict[str, str] = {
    "GH_ORG": "",
    "ROLLBAR_TOKEN": "",
    "ROLLBAR_PROJECT": "",
    "ROLLBAR_ENV": "production",
}


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _require(var: str) -> str:
    """Return env var value, raise EnvironmentError if not set."""
    val = os.environ.get(var, "").strip()
    if not val:
        raise EnvironmentError(f"Required environment variable '{var}' is not set.")
    return val


def _optional(var: str, default: str = "") -> str:
    """Return env var value or default."""
    return os.environ.get(var, default).strip() or default


def _validate_all_required() -> list[str]:
    """Return list of missing required variables (empty list = all present)."""
    return [v for v in _REQUIRED_VARS if not os.environ.get(v, "").strip()]


def missing_sheets_vars() -> list[str]:
    """Return list of missing Google Sheets variables (empty list = all present).

    Called by main.py when not in dry_run mode to enforce that Sheets credentials
    are present before attempting to write results.
    """
    return [v for v in _SHEETS_REQUIRED_VARS if not os.environ.get(v, "").strip()]


def load_engineers(engineers_yml_path: Optional[Path] = None) -> List[Engineer]:
    """Load engineers from engineers.yml.

    Defaults to kpis/engineers.yml relative to this file's location.
    """
    if engineers_yml_path is None:
        # kpis/src/config.py → kpis/engineers.yml
        engineers_yml_path = Path(__file__).parent.parent / "engineers.yml"

    logger.info("Loading engineers from %s", engineers_yml_path)

    if not engineers_yml_path.exists():
        raise FileNotFoundError(f"engineers.yml not found at {engineers_yml_path}")

    with open(engineers_yml_path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    engineers: List[Engineer] = []
    for entry in data.get("engineers", []):
        engineers.append(
            Engineer(
                name=entry["name"],
                jira_account_id=entry.get("jira_account_id", ""),
                github_login=entry.get("github_login", ""),
                rollbar_identity=entry.get("rollbar_identity", ""),
                google_sheet_tab=entry.get("google_sheet_tab", entry["name"] + " KPI"),
            )
        )

    logger.info("Loaded %d engineers", len(engineers))
    return engineers


def load_app_config(engineers_yml_path: Optional[Path] = None) -> AppConfig:
    """Load and validate all configuration.

    Raises EnvironmentError listing ALL missing required variables so the
    engineer can fix them all in one go rather than one at a time.
    """
    missing = _validate_all_required()
    if missing:
        raise EnvironmentError(
            "The following required environment variables are not set:\n"
            + "\n".join(f"  • {v}" for v in missing)
            + "\n\nSet them in your shell or as GitHub repository secrets."
        )

    engineers = load_engineers(engineers_yml_path)

    return AppConfig(
        # Jira
        jira_base_url=_require("JIRA_BASE_URL").rstrip("/"),
        jira_email=_require("JIRA_EMAIL"),
        jira_api_token=_require("JIRA_API_TOKEN"),
        jira_story_points_field=_require("JIRA_STORY_POINTS_FIELD"),
        # GitHub
        gh_token=_require("GH_TOKEN"),
        github_repo=_require("GH_REPO"),
        github_org=_optional("GH_ORG"),
        # Rollbar
        rollbar_token=_optional("ROLLBAR_TOKEN"),
        rollbar_project=_optional("ROLLBAR_PROJECT"),
        rollbar_env=_optional("ROLLBAR_ENV", "production"),
        # Google Sheets (validated separately in main.py for live runs only)
        google_service_account_json=_optional("GOOGLE_SERVICE_ACCOUNT_JSON"),
        google_sheet_id=_optional("GOOGLE_SHEET_ID"),
        # Engineers
        engineers=engineers,
    )
