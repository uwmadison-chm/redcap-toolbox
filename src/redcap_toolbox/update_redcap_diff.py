#!/usr/bin/env python

"""
Update a REDCap database with the minimum changes needed to make the system in
sync.

Requires the following environment variables to be set:
REDCAP_API_URL
REDCAP_API_TOKEN

Usage: update_redcap_diff.py [options] <base_csv> <updated_csv>

Options:
    --dry-run     Don't actually make changes.
    -h --help     Show this screen.
    -v --verbose  Show debug logging.
"""

import os
import sys

import docopt
import pandas as pd
import redcap

import redcap_toolbox.minchange

import logging

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


try:
    API_URL = os.environ['REDCAP_API_URL']
    API_TOK = os.environ['REDCAP_API_TOKEN']
except KeyError:
    logger.error('REDCAP_API_URL and REDCAP_API_TOKEN must both be set!')
    sys.exit(1)
PROJ = redcap.Project(API_URL, API_TOK)


def update_redcap_diff(base_csv, updated_csv, dry_run):
    base_df = pd.read_csv(base_csv, dtype=str, na_values=["nan", "NaN"], keep_default_na=False)
    index_cols = [base_df.columns[0]]
    if "redcap_event_name" in base_df.columns:
        index_cols.append("redcap_event_name")
    base_df.set_index(index_cols, inplace=True)
    logger.debug(f"Using index: {index_cols}")
    updated_df = pd.read_csv(updated_csv, dtype=str, na_values=["nan", "NaN"], keep_default_na=False)
    updated_df.set_index(index_cols, inplace=True)
    
    diffs = redcap_toolbox.minchange.transformation_dicts(base_df, updated_df)
    if len(diffs) == 0:
        logger.info("No changes to make")
        return
    logger.debug(f"Diffs: {diffs}")
    
    if dry_run:
        logger.warning("DRY RUN, NOT UPDATING ANYTHING")
        logger.warning(f"First change would have been {diffs[0]}")
    else:
        result = PROJ.import_records(diffs)
        logger.info(f"Import record result: {result}")
    
def main():
    args = docopt.docopt(__doc__)
    if args["--verbose"]:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)
    update_redcap_diff(args["<base_csv>"], args["<updated_csv>"], args["--dry-run"])

if __name__ == "__main__":
    main()
