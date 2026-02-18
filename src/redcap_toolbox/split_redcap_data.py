#!/usr/bin/env python

"""
Takes a REDCap CSV file and splits it into:

* A file for each event
* A file for repeated instruments in events where they happen

So, if your data has events 'scr,' 'pre,' and 'post', and 'pre' and 'post'
each have a repeated instrument called 'meds', you can expect

redcap__scr.csv
redcap__pre.csv
redcap__pre__meds.csv
redcap__post.csv
redcap__post__meds.csv

In addition, if you don't like the whole _arm_1 appended to your event names
(who does like that?) or you're using events to denote arms and want all your
event's data together, you can use the event_map file for this. That file
should be a CSV file and contain the columns 'redcap_event' and 'filename_event'

Example event maps might look like:
scr__all_arm_1,scr
pre__control_arm_1,pre
pre__intervention_arm_1,pre

Usage:
  split_redcap_data.py [options] <input_file> <output_directory>

Options:
  --event-map=<event_file>  A file mapping redcap events to file events
  --prefix=<prefix>         A filename prefix for the output [default: redcap]
  --no-condense             Don't filter empty rows, columns and files
  -h --help                 Show this screen
  -d --debug                Print debugging output
"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

import docopt
import polars as pl

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def make_event_map(mapping_file: str | None) -> dict[str, str]:
    if mapping_file is None:
        return {}
    map_df = pl.read_csv(
        mapping_file, dtypes={"redcap_event": pl.String, "filename_event": pl.String}
    )
    event_map = dict(zip(map_df["redcap_event"], map_df["filename_event"]))
    return event_map


def combine_names(event_name: str, rep_name: str) -> str:
    parts = [name for name in [event_name, rep_name] if str(name) != ""]
    joined = "__".join(parts)
    logger.debug(f"Made name {joined} from {event_name} and {rep_name}")
    return joined


def split_data(
    data: pl.DataFrame, event_map: dict[str, str]
) -> dict[str, pl.DataFrame]:
    """
    Returns a dict of nonprefixed_filename: data pairs
    """
    data_lists = defaultdict(list)
    index_col = data.columns[0]
    event_groups = data.partition_by("redcap_event_name", as_dict=True)
    for rc_event, event_data in event_groups.items():
        # partition_by returns tuples as keys, extract the first element
        rc_event_str = rc_event[0] if isinstance(rc_event, tuple) else rc_event
        out_event_name = event_map.get(str(rc_event_str), str(rc_event_str))
        rep_groups = event_data.partition_by("redcap_repeat_instrument", as_dict=True)
        for rep_group, rep_data in rep_groups.items():
            # partition_by returns tuples as keys, extract the first element
            rep_group_str = rep_group[0] if isinstance(rep_group, tuple) else rep_group
            # Convert None to empty string to avoid "None" in filenames
            rep_group_str = "" if rep_group_str is None else str(rep_group_str)
            out_name = combine_names(out_event_name, rep_group_str)
            data_lists[out_name].append(rep_data)
    # Now, we need to concat the dataframes and sort them by the index column
    dataframes = {
        name: pl.concat(df_list).sort(index_col) for name, df_list in data_lists.items()
    }
    for name, df in dataframes.items():
        logger.debug(f"Name: {name}, Shape: {df.shape})")
    return dataframes


def condense_df(
    df: pl.DataFrame, condense_rows: bool = True, condense_cols: bool = True
) -> pl.DataFrame:
    """
    Drop any rows and columns with only blank values
    This is not a super great way to do this, as it means the shape of the data we write
    (columns in particular) will depend on the content of the data.
    A better way to do this would be to make a list of what columns should appear in
    which events using the API and filter using that list. That's not so simple, though.
    """
    index_col = df.columns[0]
    reserved_cols = {
        index_col,
        "redcap_event_name",
        "redcap_repeat_instrument",
        "redcap_repeat_instance",
    }
    rowdrop_cols = list(set(df.columns) - reserved_cols)

    df_rows_cleaned = df
    if condense_rows:
        # Keep rows where not all rowdrop_cols are empty
        row_mask = ~pl.all_horizontal([pl.col(c) == "" for c in rowdrop_cols])
        df_rows_cleaned = df.filter(row_mask)

    cols_to_keep = df.columns
    if condense_cols:
        # Find columns that are not all empty
        cols_to_keep = []
        for c in df_rows_cleaned.columns:
            # For string columns, check if all values are empty strings
            # For non-string columns (like record_id), always keep them
            if df_rows_cleaned[c].dtype == pl.String:
                if not df_rows_cleaned[c].eq("").all():
                    cols_to_keep.append(c)
            else:
                # Keep non-string columns
                cols_to_keep.append(c)

    return df_rows_cleaned.select(cols_to_keep)


def split_redcap_data(
    input_file: str | Path,
    output_directory: str | Path,
    prefix: str = "redcap",
    mapping_file: str | None = None,
    condense: bool = True,
) -> None:
    event_map = make_event_map(mapping_file)
    logger.debug(f"Event map: {event_map}")
    # Read CSV with all columns as strings
    data = pl.read_csv(input_file, infer_schema_length=0)
    # Convert all columns to string type
    data = data.cast({col: pl.String for col in data.columns})
    # Make sure the event and repeating columns are present, so we can process
    # the data the same in all cases.
    if "redcap_event_name" not in data.columns:
        logger.debug("Single event file, adding event column")
        data = data.with_columns(pl.lit("").alias("redcap_event_name"))
    if "redcap_repeat_instrument" not in data.columns:
        data = data.with_columns(
            pl.lit("").alias("redcap_repeat_instrument"),
            pl.lit("").alias("redcap_repeat_instance"),
        )
        logger.debug("Non-repeating file, added repeat columns")
    named_dataframes = split_data(data, event_map)
    logger.debug(named_dataframes)
    for name, df in named_dataframes.items():
        if condense:
            df = condense_df(df)
        file_base = combine_names(prefix, name)
        filename = f"{file_base}.csv"
        out_path = Path(output_directory) / filename
        logger.info(f"Saving dataframe with shape {df.shape} to {out_path}")
        df.write_csv(out_path)


def main() -> None:
    args = docopt.docopt(__doc__ or "")
    if args["--debug"]:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)
    split_redcap_data(
        Path(args["<input_file>"]),
        Path(args["<output_directory>"]),
        args["--prefix"],
        args["--event-map"],
        not args["--no-condense"],
    )


if __name__ == "__main__":
    main()
