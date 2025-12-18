#!/usr/bin/env python

"""
Download the reports in a REDCap study to a CSV files. Takes in either a CSV
file with a list of report IDs, one per line, or a list of report IDs seperated
by commas.

If your report IDs look like: 32001, 32004 with no prefix provided, the output report filenames
will look like this in the output directory:
redcap__report_32001.csv
redcap__report_32004.csv

Relies on the environment variables REDCAP_API_URL and REDCAP_API_TOKEN.

Written by Stuti Shrivastava <sshrivastav6@wisc.edu>

Usage:
  download_redcap_report.py [options] ([--id=<id>]... | [--file=<file>]) <output_dir>
  download_redcap_report.py -h | --help

Options:
  -h --help            Show this screen.
  -d --debug           Print debugging output
  --file=<file>        A file containing a list of report IDs, one per line
  --id=<id>            A list of report IDs separated by commas
  --prefix=<prefix>    A filename prefix for the output [default: redcap]

"""

import logging
import os
import traceback
from pathlib import Path

import redcap
from docopt import docopt
from requests import RequestException

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_URL = os.environ["REDCAP_API_URL"]
API_TOK = os.environ["REDCAP_API_TOKEN"]
PROJ = redcap.Project(API_URL, API_TOK)


def file_to_list(filename: str) -> list[str]:
    out = []
    with open(filename) as f:
        out = [line.strip() for line in f.readlines()]
    out = [str(rep_id) for rep_id in out if rep_id != ""]
    return out


def download_redcap_report(
    report_ids: list[str], out_dir: Path, prefix: str, verbose: bool
) -> None:
    for rep_id in report_ids:
        logger.debug(f"Downloading report for record ID: {rep_id}")
        try:
            data = PROJ.export_report(report_id=rep_id, format_type="csv")
            out_file = Path(out_dir).joinpath(f"{prefix}__report_{rep_id}.csv")
            with open(out_file, "w", encoding="utf8") as f:
                f.write(data)
                logger.info(f"Report {rep_id} downloaded to {out_file}")
        except RequestException as e:
            logger.warning(e)
            logger.warning(f"Report {rep_id} not found! Skipping ...")
            if verbose:
                logger.warning(traceback.format_exc())


def main() -> None:
    args = docopt(__doc__ or "")
    if args["--debug"]:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)

    out_dir = Path(args["<output_dir>"])
    if not out_dir.is_dir():
        logger.warning(
            f"Output directory {out_dir} does not exist! Making directory ..."
        )
        out_dir.mkdir(parents=True, exist_ok=True)

    if args["--file"]:
        report_ids = file_to_list(args["--file"])
    elif args["--id"]:
        report_ids = [str(rep_id) for rep_id in args["--id"]]
    else:
        raise ValueError("No report IDs provided!")
    logger.debug(f"Report IDs: {report_ids}")
    if len(report_ids) == 0:
        raise ValueError(
            f"No report IDs provided! {report_ids}. Provide either --id or --file."
        )

    download_redcap_report(report_ids, out_dir, args["--prefix"], args["--debug"])


if __name__ == "__main__":
    main()
