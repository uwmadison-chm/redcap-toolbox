#!/usr/bin/env python

"""
Update a REDCap database with the minimum changes needed to make the system in
sync.

Requires the following environment variables to be set:
REDCAP_API_URL
REDCAP_API_TOKEN

Usage: update_redcap_diff.py [options] <base_csv> <updated_csv>

Options:
    --allow-new   Allow adding new rows to REDCap.
    --dry-run     Don't actually make changes.
    -h --help     Show this screen.
    -v --verbose  Show debug logging.
"""

import logging
import os
import sys

import docopt
import pandas as pd
import redcap
import redcap_toolbox.minchange

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize these as None - they'll be set in main()
API_URL = None
API_TOK = None
PROJ = None

INDEX_COLUMNS = [
    "redcap_event_name",
    "redcap_repeat_instrument",
    "redcap_repeat_instance",
]


def update_redcap_diff(base_csv, updated_csv, dry_run, allow_new=False):
    base_df = pd.read_csv(base_csv, dtype=str, keep_default_na=False)
    index_cols = [base_df.columns[0]]
    for icol in INDEX_COLUMNS:
        if icol in base_df.columns:
            index_cols.append(icol)
    base_df.set_index(index_cols, inplace=True)
    logger.debug(f"Using index: {index_cols}")
    updated_df = pd.read_csv(updated_csv, dtype=str, keep_default_na=False)
    updated_df.set_index(index_cols, inplace=True)

    diffs = redcap_toolbox.minchange.transformation_dicts(
        base_df, updated_df, allow_new=allow_new
    )
    if len(diffs) == 0:
        logger.info("No changes to make")
        return
    logger.debug(f"Diffs: {diffs}")

    if dry_run:
        logger.warning("DRY RUN, NOT UPDATING ANYTHING")
        logger.info(f"First change would have been {diffs[0]}")
    else:
        result = PROJ.import_records(diffs)
        logger.info(f"Import record result: {result}")


def main():
    global API_URL, API_TOK, PROJ

    args = docopt.docopt(__doc__)
    if args["--verbose"]:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)

    base_csv = args["<base_csv>"]
    updated_csv = args["<updated_csv>"]
    dry_run = args["--dry-run"]
    allow_new = args["--allow-new"]

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
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
