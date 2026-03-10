"""
Google Sheets client for the KPI pipeline.

Authenticates using a Google Service Account JSON string stored in the
GOOGLE_SERVICE_ACCOUNT_JSON environment variable.

Each engineer has a dedicated worksheet (tab) identified by their
``google_sheet_tab`` field in engineers.yml.  On every pipeline run the
worksheet is cleared and rewritten so the sheet always shows the latest
metrics — no duplicate rows accumulate.

Privacy model: only the service account and users who have been granted
explicit access to the spreadsheet can read it.  Engineers cannot see each
other's tabs unless spreadsheet-level sharing is configured to allow it.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from utils.logging import get_logger
from utils.safe_run import MetricResult

logger = get_logger(__name__)

# OAuth 2.0 scopes required by gspread
_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSheetsClient:
    """Writes KPI metrics to a Google Spreadsheet.

    One instance is shared across all engineers in a pipeline run.
    Each engineer's metrics are written to their own worksheet tab.
    """

    def __init__(
        self,
        service_account_json: str,
        sheet_id: str,
        dry_run: bool = False,
    ) -> None:
        """Initialise the client.

        Args:
            service_account_json: Raw JSON string of the service account
                credentials (the content of the downloaded key file).
            sheet_id: Google Spreadsheet ID taken from the spreadsheet URL
                (the long alphanumeric string between /d/ and /edit).
            dry_run: If True all write operations are skipped and logged
                instead; no network calls are made.
        """
        self._dry_run = dry_run
        self._sheet_id = sheet_id
        self._gc: Optional[gspread.Client] = None

        if not dry_run:
            self._gc = self._build_client(service_account_json)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_report(
        self,
        tab_name: str,
        engineer_name: str,
        metrics: List[MetricResult],
        current_period: Tuple,
        previous_period: Tuple,
    ) -> bool:
        """Write engineer KPI metrics to their dedicated worksheet.

        The worksheet is cleared on every run so the sheet always
        reflects the most recent pipeline execution.

        Args:
            tab_name: Name of the worksheet tab (from engineer's
                ``google_sheet_tab`` field).
            engineer_name: Human-readable engineer name for the report header.
            metrics: List of MetricResult objects (already merged across periods).
            current_period: (start_dt, end_dt) for the current window.
            previous_period: (start_dt, end_dt) for the comparison window.

        Returns:
            True on success, False on failure.
        """
        if self._dry_run:
            logger.info(
                "[DRY RUN] Would write %d metrics to Google Sheet tab '%s'",
                len(metrics),
                tab_name,
            )
            return True

        try:
            spreadsheet = self._gc.open_by_key(self._sheet_id)
            worksheet = self._get_or_create_worksheet(spreadsheet, tab_name)
            rows = self._build_rows(
                engineer_name, metrics, current_period, previous_period
            )
            worksheet.clear()
            worksheet.update(rows, "A1")
            logger.info(
                "Wrote %d metric rows to Google Sheet tab '%s'", len(metrics), tab_name
            )
            return True

        except Exception as exc:
            logger.error(
                "Failed to write to Google Sheet tab '%s': %s: %s",
                tab_name,
                type(exc).__name__,
                exc,
            )
            return False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_client(service_account_json: str) -> gspread.Client:
        """Authenticate and return a gspread client."""
        creds_dict = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=_SCOPES)
        return gspread.authorize(creds)

    @staticmethod
    def _get_or_create_worksheet(
        spreadsheet: gspread.Spreadsheet, tab_name: str
    ) -> gspread.Worksheet:
        """Return the named worksheet, creating it if it does not exist."""
        try:
            return spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info("Creating new worksheet tab: '%s'", tab_name)
            return spreadsheet.add_worksheet(title=tab_name, rows=60, cols=10)

    @staticmethod
    def _fmt(value) -> str:
        """Format a metric value for display in the sheet."""
        if value is None:
            return "N/A"
        if isinstance(value, float):
            # Drop the decimal point for whole numbers
            if value == int(value):
                return str(int(value))
            return f"{value:.2f}"
        return str(value)

    def _build_rows(
        self,
        engineer_name: str,
        metrics: List[MetricResult],
        current_period: Tuple,
        previous_period: Tuple,
    ) -> List[List[str]]:
        """Build the 2-D list of cell values to write into the sheet.

        Layout:
            Row 1  : Report title
            Row 2  : Current period range
            Row 3  : Previous period range
            Row 4  : Generation timestamp
            Row 5  : (blank separator)
            Row 6  : Column headers
            Row 7+ : One row per metric
        """
        curr_start, curr_end = current_period
        prev_start, prev_end = previous_period
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        header_block: List[List[str]] = [
            [f"KPI Report — {engineer_name}", "", "", "", "", ""],
            [
                f"Current period:  {curr_start.date()} → {curr_end.date()}",
                "",
                "",
                "",
                "",
                "",
            ],
            [
                f"Previous period: {prev_start.date()} → {prev_end.date()}",
                "",
                "",
                "",
                "",
                "",
            ],
            [f"Generated: {generated_at}", "", "", "", "", ""],
            ["", "", "", "", "", ""],
            ["Metric", "Current", "Previous", "% Change", "Unit", "Error"],
        ]

        metric_rows: List[List[str]] = []
        for m in metrics:
            if m.pct_change is None:
                pct_str = "N/A"
            else:
                sign = "+" if m.pct_change >= 0 else ""
                pct_str = f"{sign}{m.pct_change:.1f}%"

            metric_rows.append(
                [
                    m.name,
                    self._fmt(m.current_value),
                    self._fmt(m.previous_value),
                    pct_str,
                    m.unit,
                    m.error or "",
                ]
            )

        return header_block + metric_rows
