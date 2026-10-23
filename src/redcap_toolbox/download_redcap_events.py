#!/usr/bin/env python

"""
Download the events in a REDCap study to a configuration directory.
Two files are saved with provided prefix like StudyName or "redcap" if none provided.
 - <prefix>__event_map.csv
 - <prefix>__repeating_event_map.csv

Relies on the environment variables REDCAP_API_URL and REDCAP_API_TOKEN.

Written by Stuti Shrivastava <sshrivastav6@wisc.edu>

Usage:
  download_redcap_events.py [options] <out_dir>
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

EVENT_MAP_SUFFIX = "__event_map.csv"
REPEATING_EVENT_MAP_SUFFIX = "__repeating_event_map.csv"


def download_redcap_events(output_dir: Path, prefix_str: str, verbose: bool) -> None:
    try:
        logger.debug("Downloading events mapping file... ")
        data = PROJ.export_events(format_type="csv")
        logger.debug(f"Event map {data}")
        out_file = output_dir.joinpath(f"{prefix_str}{EVENT_MAP_SUFFIX}")

        with open(out_file, "w", encoding="utf8") as f:
            f.write(data)
            logger.info(f"Events map saved to {out_file}")

        logger.debug("Downloading events instrument mapping file... ")
        data_rep = PROJ.export_repeating_instruments_events(format_type="csv")
        rep_file = output_dir.joinpath(f"{prefix_str}{REPEATING_EVENT_MAP_SUFFIX}")
        logger.debug(f"Repeating instrument to event map {data_rep} ")

        with open(rep_file, "w", encoding="utf8") as f:
            f.write(data_rep)
            logger.info(f"Repeating instruments to events map saved to {rep_file}")

    except RequestException as e:
        logger.warning(e)
        if verbose:
            logger.warning(traceback.format_exc())
        sys.exit(1)


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

    out_dir = Path(args["<out_dir>"])
    if not out_dir.is_dir():
        logger.debug(f"Output directory {out_dir} does not exist! Making directory ...")
        out_dir.mkdir(parents=True, exist_ok=True)

    download_redcap_events(out_dir, args["--prefix"], args["--debug"])


if __name__ == "__main__":
    main()
