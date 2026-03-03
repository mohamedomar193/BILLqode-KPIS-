"""
CSV writer for the KPI pipeline.

Writes one CSV file per engineer containing all 8 metrics for the current
run.  The CSV can be used for offline analysis, audit trails, or importing
into dashboards.

Output path: /tmp/kpi_{engineer_name}_{date}.csv (or configurable via arg)

CSV schema:
    engineer_name, metric_name, current_value, previous_value, pct_change,
    unit, period_start, period_end, prev_period_start, prev_period_end, error
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from config import Engineer
from utils.logging import get_logger
from utils.safe_run import MetricResult

logger = get_logger(__name__)


def write(
    engineer: Engineer,
    metrics: List[MetricResult],
    current_period: Tuple[datetime, datetime],
    previous_period: Tuple[datetime, datetime],
    output_dir: str = "/tmp",
) -> str:
    """Write metric results to a CSV file for one engineer.

    Args:
        engineer:        Engineer dataclass instance.
        metrics:         List of MetricResult objects.
        current_period:  (start, end) UTC-aware datetimes for current window.
        previous_period: (start, end) UTC-aware datetimes for previous window.
        output_dir:      Directory to write CSV into (default /tmp).

    Returns:
        Absolute path to the written CSV file.
    """
    curr_start, curr_end = current_period
    prev_start, prev_end = previous_period
    date_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    safe_name = engineer.name.lower().replace(" ", "_")
    filename = f"kpi_{safe_name}_{date_str}.csv"

    out_path = Path(output_dir) / filename

    rows = []
    for m in metrics:
        rows.append(
            {
                "engineer_name": engineer.name,
                "metric_name": m.name,
                "current_value": m.current_value,
                "previous_value": m.previous_value,
                "pct_change": m.pct_change,
                "unit": m.unit,
                "period_start": curr_start.isoformat(),
                "period_end": curr_end.isoformat(),
                "prev_period_start": prev_start.isoformat(),
                "prev_period_end": prev_end.isoformat(),
                "error": m.error or "",
            }
        )

    df = pd.DataFrame(rows)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(out_path, index=False)

    logger.info("CSV written: %s", out_path)
    return str(out_path)
