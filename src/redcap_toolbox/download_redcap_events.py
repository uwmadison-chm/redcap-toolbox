#!/usr/bin/env python

"""
Download the events in a REDCap study to a CSV file.

Relies on the environment variables REDCAP_API_URL and REDCAP_API_TOKEN.

Written by Stuti Shrivastava <sshrivastav6@wisc.edu>

Usage:
  download_redcap_events.py [options] <output_file>
  download_redcap_events.py -h | --help

Options:
  -h --help            Show this screen.
  -d --debug           Print debugging output
  --prefix=<prefix>    A filename prefix for the output [default: redcap]

"""

import logging
import os
import sys
import traceback
from pathlib import Path
from typing import Any

import redcap
from docopt import docopt
from requests import RequestException

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_URL: str | None = None
API_TOK: str | None = None
PROJ: Any = None


def file_to_list(filename: str) -> list[str]:
    out = []
    with open(filename) as f:
        out = [line.strip() for line in f.readlines()]
    out = [str(rep_id) for rep_id in out if rep_id != ""]
    return out


def download_redcap_events(
    out_file: Path, prefix: str, verbose: bool
) -> None:

    try:
        logger.debug(f"Downloading events mapping file... ")
        data = PROJ.export_events(format_type="csv")
        logger.debug(f"Event map {data}")
        with open(out_file, "w", encoding="utf8") as f:
            f.write(data)
            logger.info(f"Events map saved to {out_file}")

        logger.debug(f"Downloading events intrument mapping file... ")
        data_rep = PROJ.export_repeating_instruments_events(format_type="csv")
        rep_name = out_file.name.replace("event", "repeating_event")
        rep_file = out_file.parent.joinpath(rep_name)
        logger.debug(f"Event map {data_rep} ")
        with open(rep_file, "w", encoding="utf8") as f:
            f.write(data_rep)
            logger.info(f"Repeating instruments to events map saved to {rep_file}")

    except RequestException as e:
        logger.warning(e)
        if verbose:
            logger.warning(traceback.format_exc())


def main() -> None:
    global API_URL, API_TOK, PROJ

    args = docopt(__doc__ or "")
    if args["--debug"]:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)

    try:
        API_URL = os.environ["REDCAP_API_URL"]
        API_TOK = os.environ["REDCAP_API_TOKEN"]
        PROJ = redcap.Project(API_URL, API_TOK)
    except KeyError:
        logger.error("REDCAP_API_URL and REDCAP_API_TOKEN must both be set!")
        sys.exit(1)

    output_file = Path(args["<output_file>"])
    out_dir = output_file.parent
    if not out_dir.is_dir():
        logger.debug(
            f"Output directory {out_dir} does not exist! Making directory ..."
        )
        out_dir.mkdir(parents=True, exist_ok=True)

    download_redcap_events(output_file, args["--prefix"], args["--debug"])


if __name__ == "__main__":
    main()
