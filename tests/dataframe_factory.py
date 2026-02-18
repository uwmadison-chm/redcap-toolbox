#!/usr/bin/env python

import polars as pl


def _create_dataframe(data, key_cols=None):
    """Helper function to create a DataFrame, optionally sorted by key columns."""
    df = pl.DataFrame(data)
    if key_cols is None:
        key_cols = ["record_id", "redcap_event_name"]
    return df.sort(key_cols)


def create_standard_df():
    """Create the standard test DataFrame."""
    data = {
        "record_id": ["1", "2", "2"],
        "redcap_event_name": ["scr_arm_1", "scr_arm_1", "pre_arm_1"],
        "redcap_repeat_instrument": ["", "", ""],
        "field1": ["a", "b", "c"],
        "field2": ["d", "e", "f"],
        "field3": ["10", "20", "30"],
    }
    df = _create_dataframe(data)
    # Cast record_id to integer for proper sorting
    df = df.with_columns(pl.col("record_id").cast(pl.Int64))
    return df


def create_df_with_changes():
    """Create DataFrame with some field changes from the standard."""
    df = create_standard_df()
    # Change field3 for record 2, scr_arm_1 from 20 to 40
    df = df.with_columns(
        pl.when(
            (pl.col("record_id") == 2) & (pl.col("redcap_event_name") == "scr_arm_1")
        )
        .then(pl.lit("40"))
        .otherwise(pl.col("field3"))
        .alias("field3")
    )
    # Change field1 for record 2, pre_arm_1 from "c" to "g"
    df = df.with_columns(
        pl.when(
            (pl.col("record_id") == 2) & (pl.col("redcap_event_name") == "pre_arm_1")
        )
        .then(pl.lit("g"))
        .otherwise(pl.col("field1"))
        .alias("field1")
    )
    return df


def create_df_with_wrong_index():
    """Create DataFrame with different key columns - missing redcap_event_name."""
    df = create_standard_df()
    # Drop redcap_event_name to simulate having different key columns
    return df.drop("redcap_event_name")


def create_df_with_extra_columns():
    """Create DataFrame with additional columns."""
    df = create_df_with_changes()
    return df.with_columns(pl.Series("field4", ["x", "y", "z"]))


def create_df_with_new_rows():
    """Create DataFrame with new rows added."""
    base_df = create_standard_df()

    # Create new rows
    new_data = {
        "record_id": ["3", "4"],
        "redcap_event_name": ["scr_arm_1", "pre_arm_1"],
        "redcap_repeat_instrument": ["", ""],
        "field1": ["new_val", ""],
        "field2": ["", "new_val2"],
        "field3": ["50", "60"],
    }
    new_rows_df = _create_dataframe(new_data)
    new_rows_df = new_rows_df.with_columns(pl.col("record_id").cast(pl.Int64))

    return pl.concat([base_df, new_rows_df])


def create_df_with_new_rows_all_blank():
    """Create DataFrame with new rows that have all blank columns."""
    base_df = create_standard_df()

    new_data = {
        "record_id": ["3"],
        "redcap_event_name": ["scr_arm_1"],
        "redcap_repeat_instrument": [""],
        "field1": [""],
        "field2": [""],
        "field3": [""],
    }
    new_rows_df = _create_dataframe(new_data)
    new_rows_df = new_rows_df.with_columns(pl.col("record_id").cast(pl.Int64))

    return pl.concat([base_df, new_rows_df])


def create_df_with_mixed_changes_and_new_rows():
    """Create DataFrame with both existing changes and new rows."""
    # Start with changes
    df = create_df_with_changes()

    # Add one more change
    df = df.with_columns(
        pl.when(
            (pl.col("record_id") == 2) & (pl.col("redcap_event_name") == "pre_arm_1")
        )
        .then(pl.lit("changed"))
        .otherwise(pl.col("field1"))
        .alias("field1")
    )

    # Add new row
    new_data = {
        "record_id": ["3"],
        "redcap_event_name": ["scr_arm_1"],
        "redcap_repeat_instrument": [""],
        "field1": ["new_val"],
        "field2": [""],
        "field3": ["50"],
    }
    new_rows_df = _create_dataframe(new_data)
    new_rows_df = new_rows_df.with_columns(pl.col("record_id").cast(pl.Int64))

    return pl.concat([df, new_rows_df])


def create_df_with_different_index_levels():
    """Create DataFrame with different number of key columns - missing redcap_event_name."""
    df = create_standard_df()
    # Drop redcap_event_name to simulate having only one key column
    return df.drop("redcap_event_name")


def create_df_with_different_index_names():
    """Create DataFrame with different key column names."""
    df = create_standard_df()
    df = df.rename({"redcap_event_name": "different_event"})
    return df.sort(["record_id", "different_event"])


def create_df_with_matching_index_format_new_values():
    """Create DataFrame with matching index format but new values."""
    data = {
        "record_id": ["1", "2", "3"],
        "redcap_event_name": ["scr_arm_1", "scr_arm_1", "pre_arm_1"],
        "redcap_repeat_instrument": ["", "", ""],
        "field1": ["a", "b", "new"],
        "field2": ["d", "e", "new2"],
        "field3": ["10", "20", "40"],
    }
    df = _create_dataframe(data)
    df = df.with_columns(pl.col("record_id").cast(pl.Int64))
    return df
