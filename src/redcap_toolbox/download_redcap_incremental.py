#!/usr/bin/env python

"""
Download REDCap data incrementally to a CSV file. On first run, downloads
all records as a base. On subsequent runs, downloads only records changed
since the last run and merges them into the output.

Relies on REDCAP_API_URL and REDCAP_API_TOKEN environment variables.

Incremental state is stored alongside <output_file> in a .incremental/
directory:
  .incremental/base.csv        - Accumulated full dataset
  .incremental/.last_download  - Timestamp of last successful download

To force a full re-download, delete the .incremental/ directory.

Usage:
  download_redcap_incremental [options] <output_file>

Options:
  -h --help               Show this screen.
  --overlap=<duration>    Overlap duration for missed-change protection [default: 24h]
                          Accepts: 60s, 5m, 24h, 3d, or a bare number (seconds).
  --tz=<tz>               Timezone for timestamps, e.g. America/Chicago (default: local time)
  -v --verbose            Print verbose output
"""

import logging
import os
import re
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import polars as pl
import redcap
from docopt import docopt

from redcap_toolbox.csv_utils import key_cols_for, read_csv

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

API_URL: str | None = None
API_TOK: str | None = None
PROJ: Any = None


_UNIT_MAP = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


def parse_overlap(s: str) -> timedelta:
    """Parse an overlap duration string such as '60s', '5m', '24h', '3d'.

    A bare number (no unit suffix) is interpreted as seconds.
    """
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(s|m|h|d)?", s.strip())
    if not m:
        raise ValueError(f"Invalid overlap duration: {s!r}")
    value = float(m.group(1))
    unit = m.group(2) or "s"
    return timedelta(**{_UNIT_MAP[unit]: value})


def incremental_dir(output_file: Path) -> Path:
    return output_file.parent / ".incremental"


def base_file(output_file: Path) -> Path:
    return incremental_dir(output_file) / "base.csv"


def timestamp_file(output_file: Path) -> Path:
    return incremental_dir(output_file) / ".last_download"


def read_timestamp(output_file: Path) -> datetime | None:
    ts_file = timestamp_file(output_file)
    if not ts_file.exists():
        return None
    return datetime.fromisoformat(ts_file.read_text().strip())


def write_timestamp(output_file: Path, ts: datetime) -> None:
    timestamp_file(output_file).write_text(ts.isoformat())


def export_records(date_begin: datetime | None = None) -> str:
    kwargs: dict[str, Any] = {"format_type": "csv"}
    if date_begin is not None:
        kwargs["date_begin"] = date_begin.strftime(TIMESTAMP_FORMAT)
    return PROJ.export_records(**kwargs)  # type: ignore[union-attr]


def merge(base_df: pl.DataFrame, inc_df: pl.DataFrame) -> pl.DataFrame:
    dropped = set(base_df.columns) - set(inc_df.columns)
    if dropped:
        raise ValueError(
            f"Incremental download is missing columns present in base:\n"
            "{sorted(dropped)}.\n"
            "You could lose data! Investigate what's going on, and fix it or delete\n"
            ".incremental/ to start fresh."
        )
    key_cols = key_cols_for(inc_df)
    combined = pl.concat([base_df, inc_df], how="diagonal").fill_null("")
    return combined.unique(subset=key_cols, keep="last", maintain_order=True)


def run(output_file: Path, overlap: timedelta, tz: ZoneInfo | None = None) -> None:
    inc_dir = incremental_dir(output_file)
    inc_dir.mkdir(parents=True, exist_ok=True)

    last_ts = read_timestamp(output_file)
    if tz is None:
        download_start = datetime.now().astimezone()
    else:
        download_start = datetime.now(tz=tz)

    if last_ts is None:
        logger.info("No previous download found; performing full base download.")
        raw = export_records()
        base_file(output_file).write_text(raw, encoding="utf8")
        logger.info("Base download complete.")
    else:
        date_begin = last_ts - overlap
        logger.info(
            f"Downloading changes since {date_begin.strftime(TIMESTAMP_FORMAT)} "
            f"(overlap: {overlap})."
        )
        raw = export_records(date_begin=date_begin)
        inc_df = read_csv(raw.encode())

        if inc_df.is_empty():
            logger.info("No new records found.")
        else:
            logger.info(f"Merging {len(inc_df)} incremental rows into base.")
            base_df = read_csv(base_file(output_file))
            merged = merge(base_df, inc_df)
            merged.write_csv(base_file(output_file))
            logger.info(f"Merge complete; {len(merged)} total rows.")

    write_timestamp(output_file, download_start)
    shutil.copy2(base_file(output_file), output_file)


def main() -> None:
    global API_URL, API_TOK, PROJ

    args = docopt(__doc__ or "")
    if args["--verbose"]:
        logger.setLevel(logging.INFO)

    try:
        overlap = parse_overlap(args["--overlap"])
    except ValueError:
        logger.error(
            "--overlap must be a duration like '60s', '5m', '24h', '3d', "
            "or a bare number of seconds."
        )
        sys.exit(1)

    tz = None
    if args["--tz"]:
        try:
            tz = ZoneInfo(args["--tz"])
        except ZoneInfoNotFoundError:
            logger.error(f"Unknown timezone: {args['--tz']!r}")
            sys.exit(1)

    try:
        API_URL = os.environ["REDCAP_API_URL"]
        API_TOK = os.environ["REDCAP_API_TOKEN"]
        PROJ = redcap.Project(API_URL, API_TOK)
    except KeyError:
        logger.error("REDCAP_API_URL and REDCAP_API_TOKEN must both be set!")
        sys.exit(1)

    output_file = Path(args["<output_file>"])
    run(output_file, overlap, tz=tz)


if __name__ == "__main__":
    main()
