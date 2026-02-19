#!/usr/bin/env python

"""
Download the data for a REDCap study to a CSV files. Optionally takes a
filename with a list of forms to skip from the download, one per line.

Relies on the environment variables REDCAP_API_URL and REDCAP_API_TOKEN.

Written by Nate Vack <njvack@wisc.edu> and Dan Fitch <dfitch@wisc.edu>

Usage:
  download_redcap.py [options] <output_file>

Options:
  -h --help        Show this screen.
  --forms=<file>   A file containing a list of forms to download, one per line;
                   if not specified, download all forms.
  --survey-fields  Include survey timestamps in output
  -d --debug       Print debugging output
"""

import logging
import os
import sys
from pathlib import Path
from typing import Any

import redcap
from docopt import docopt

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
    return out


def download_redcap(
    out_file: Path, form_list_file: str | None, export_survey_fields: bool
) -> None:
    forms = None
    if form_list_file:
        forms = file_to_list(form_list_file)
    logger.debug(f"Instruments: {forms}")
    data = PROJ.export_records(
        format_type="csv", forms=forms, export_survey_fields=export_survey_fields
    )
    with open(out_file, "w", encoding="utf8") as f:
        f.write(data)


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

    download_redcap(
        Path(args["<output_file>"]), args["--forms"], args["--survey-fields"]
    )


if __name__ == "__main__":
    main()
