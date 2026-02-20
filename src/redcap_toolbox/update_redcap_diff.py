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
    --batch-size N      Import records in batches of N rows.
    --dry-run           Don't actually make changes.
    --max-records N     Exit with error if update exceeds N rows; 0 disables limit. [default: 1000]
    --strict-cols       Require updated CSV columns to exactly match base CSV columns.
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
from redcap_toolbox.csv_utils import key_cols_for, read_csv

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize these as None - they'll be set in main()
API_URL: str | None = None
API_TOK: str | None = None
PROJ: Any = None

EXIT_OK = 0
EXIT_ERROR = 1


def update_redcap_diff(
    base_csv: str,
    updated_csv: str,
    dry_run: bool,
    allow_new: bool = False,
    background_import: bool = False,
    max_records: int = 1000,
    strict_cols: bool = False,
    batch_size: int | None = None,
) -> int:
    """Return the number of rows updated (0 if none; dry-run returns the count that would be updated).

    Raises ValueError for invalid inputs or limit violations.
    Other exceptions from the REDCap API propagate to the caller.
    """
    base_df = read_csv(base_csv)
    key_cols = key_cols_for(base_df)
    base_df = base_df.sort(key_cols)
    logger.debug(f"Using key columns: {key_cols}")
    updated_df = read_csv(updated_csv)
    missing_key_cols = [c for c in key_cols if c not in updated_df.columns]
    if missing_key_cols:
        raise ValueError(
            f"Updated CSV is missing key columns: {sorted(missing_key_cols)}"
        )
    updated_df = updated_df.sort(key_cols)

    if not strict_cols:
        extra_cols = set(updated_df.columns) - set(base_df.columns)
        if extra_cols:
            raise ValueError(
                f"Updated CSV has columns not in base CSV: {sorted(extra_cols)}"
            )
        base_df = base_df.select(updated_df.columns)

    diffs = redcap_toolbox.minchange.transformation_dicts(
        base_df, updated_df, key_cols=key_cols, allow_new=allow_new
    )
    if len(diffs) == 0:
        logger.info("No changes to make")
        return 0
    if max_records and len(diffs) > max_records:
        raise ValueError(
            f"Update would affect {len(diffs)} rows, exceeding --max-records limit of {max_records}"
        )
    logger.debug(f"Diffs: {diffs}")

    effective_batch_size = batch_size or len(diffs)
    batches = [
        diffs[i : i + effective_batch_size]
        for i in range(0, len(diffs), effective_batch_size)
    ]

    if dry_run:
        logger.warning("DRY RUN, NOT UPDATING ANYTHING")
        logger.info(
            f"Would import {len(batches)} {'batch' if len(batches) == 1 else 'batches'} of up to {effective_batch_size} records ({len(diffs)} total)"
        )
        logger.info(f"First change would have been {diffs[0]}")
        return len(diffs)

    for i, batch in enumerate(batches):
        if len(batches) > 1:
            logger.info(
                f"Importing batch {i + 1}/{len(batches)} ({len(batch)} records)"
            )
        result = PROJ.import_records(batch, background_import=background_import or None)
        logger.info(f"Import record result: {result}")
    return len(diffs)


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
    strict_cols = args["--strict-cols"]
    batch_size = int(args["--batch-size"]) if args["--batch-size"] else None

    if max_records < 0:
        logger.error("--max-records must be nonnegative")
        return EXIT_ERROR
    if batch_size is not None and batch_size < 1:
        logger.error("--batch-size must be a positive integer")
        return EXIT_ERROR

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
            return EXIT_ERROR

    try:
        update_redcap_diff(
            base_csv,
            updated_csv,
            dry_run,
            allow_new,
            background_import,
            max_records,
            strict_cols,
            batch_size,
        )
    except ValueError as e:
        logger.error(str(e))
        return EXIT_ERROR
    except Exception as e:
        logger.error(f"Error importing records: {e}")
        logger.error(traceback.format_exc())
        return EXIT_ERROR
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
