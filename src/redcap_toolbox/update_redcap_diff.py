#!/usr/bin/env python

"""
Update a REDCap database with the minimum changes needed to make the system in
sync.

Requires the following environment variables to be set:
REDCAP_API_URL
REDCAP_API_TOKEN

Usage: update_redcap_diff.py [options] <base_csv> <updated_csv>

Options:
    --allow-new         Allow adding new rows to REDCap.
    --background        Use background import mode.
    --dry-run           Don't actually make changes.
    --max-records N     Exit with error if update exceeds N rows; 0 disables limit. [default: 1000]
    -h --help           Show this screen.
    -v --verbose        Show debug logging.
"""

import logging
import os
import sys
from typing import Any
import traceback

import docopt
import polars as pl
import redcap
import redcap_toolbox.minchange

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize these as None - they'll be set in main()
API_URL: str | None = None
API_TOK: str | None = None
PROJ: Any = None

INDEX_COLUMNS = [
    "redcap_event_name",
    "redcap_repeat_instrument",
    "redcap_repeat_instance",
]


def update_redcap_diff(
    base_csv: str,
    updated_csv: str,
    dry_run: bool,
    allow_new: bool = False,
    background_import: bool = False,
    max_records: int = 1000,
) -> None:
    # Read CSV with all columns as strings
    base_df = pl.read_csv(base_csv, infer_schema_length=0)
    base_df = base_df.cast({col: pl.String for col in base_df.columns})
    key_cols = [base_df.columns[0]]
    for icol in INDEX_COLUMNS:
        if icol in base_df.columns:
            key_cols.append(icol)
    base_df = base_df.sort(key_cols)
    logger.debug(f"Using key columns: {key_cols}")
    # Read CSV with all columns as strings
    updated_df = pl.read_csv(updated_csv, infer_schema_length=0)
    updated_df = updated_df.cast({col: pl.String for col in updated_df.columns})
    updated_df = updated_df.sort(key_cols)

    diffs = redcap_toolbox.minchange.transformation_dicts(
        base_df, updated_df, key_cols=key_cols, allow_new=allow_new
    )
    if len(diffs) == 0:
        logger.info("No changes to make")
        return
    if max_records and len(diffs) > max_records:
        raise ValueError(
            f"Update would affect {len(diffs)} rows, exceeding --max-records limit of {max_records}"
        )
    logger.debug(f"Diffs: {diffs}")

    if dry_run:
        logger.warning("DRY RUN, NOT UPDATING ANYTHING")
        logger.info(f"First change would have been {diffs[0]}")
    else:
        try:
            result = PROJ.import_records(diffs, background_import=background_import or None)
            logger.info(f"Import record result: {result}")
        except Exception as e:
            logger.error(f"Error importing records: {e}")
            logger.error(traceback.format_exc())
            return sys.exit(1)


def main() -> int:
    global API_URL, API_TOK, PROJ

    args = docopt.docopt(__doc__ or "")
    if args["--verbose"]:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)

    base_csv = args["<base_csv>"]
    updated_csv = args["<updated_csv>"]
    dry_run = args["--dry-run"]
    allow_new = args["--allow-new"]
    background_import = args["--background"]
    max_records = int(args["--max-records"])

    # Initialize API connection
    try:
        API_URL = os.environ["REDCAP_API_URL"]
        API_TOK = os.environ["REDCAP_API_TOKEN"]
        PROJ = redcap.Project(API_URL, API_TOK)
    except KeyError:
        if dry_run:
            logger.warning(
                "REDCAP environment variables are not set, continuing because dry_run is set"
            )
        else:
            logger.error("REDCAP_API_URL and REDCAP_API_TOKEN must both be set!")
            return 1

    update_redcap_diff(
        base_csv,
        updated_csv,
        dry_run,
        allow_new,
        background_import,
        max_records,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
