from pathlib import Path

import polars as pl

# REDCap structural columns that form a composite key alongside record_id
REDCAP_KEY_COLS = [
    "redcap_event_name",
    "redcap_repeat_instrument",
    "redcap_repeat_instance",
]


def read_csv(source: str | Path | bytes) -> pl.DataFrame:
    """Read a CSV file, keeping all columns as strings and empty fields as empty strings."""
    return pl.read_csv(
        source,
        infer_schema_length=0,
        missing_utf8_is_empty_string=True,
    )


def key_cols_for(df: pl.DataFrame) -> list[str]:
    """Return the key columns for a REDCap dataframe.

    Always includes the first column (default: record_id), plus any of the standard
    REDCap structural columns that are present in the dataframe.

    This set of columns should be a unique index for any REDCap export.
    """
    extras = [c for c in REDCAP_KEY_COLS if c in set(df.columns)]
    return [df.columns[0]] + extras
