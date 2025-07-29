#!/usr/bin/env python

import pandas as pd


def _create_dataframe(data):
    """Helper function to create a DataFrame with standard index."""
    return pd.DataFrame(data).set_index(["record_id", "redcap_event_name"])


def create_standard_df():
    """Create the standard test DataFrame."""
    data = {
        "record_id": [1, 2, 2],
        "redcap_event_name": ["scr_arm_1", "scr_arm_1", "pre_arm_1"],
        "redcap_repeat_instrument": ["", "", ""],
        "field1": ["a", "b", "c"],
        "field2": ["d", "e", "f"],
        "field3": [10, 20, 30],
    }
    return _create_dataframe(data)


def create_df_with_changes():
    """Create DataFrame with some field changes from the standard."""
    df = create_standard_df()
    # Change field3 for record 2, scr_arm_1 from 20 to 40
    df.loc[(2, "scr_arm_1"), "field3"] = 40
    # Change field1 for record 2, pre_arm_1 from "c" to "g"
    df.loc[(2, "pre_arm_1"), "field1"] = "g"
    return df


def create_df_with_wrong_index():
    """Create DataFrame with different index columns."""
    df = create_standard_df().reset_index()
    return df.set_index(["record_id", "field2"])


def create_df_with_extra_columns():
    """Create DataFrame with additional columns."""
    df = create_df_with_changes()
    return df.assign(field4=["x", "y", "z"])


def create_df_with_new_rows():
    """Create DataFrame with new rows added."""
    base_df = create_standard_df()

    # Create new rows
    new_data = {
        "record_id": [3, 4],
        "redcap_event_name": ["scr_arm_1", "pre_arm_1"],
        "redcap_repeat_instrument": ["", ""],
        "field1": ["new_val", ""],
        "field2": ["", "new_val2"],
        "field3": [50, 60],
    }
    new_rows_df = _create_dataframe(new_data)

    return pd.concat([base_df, new_rows_df])


def create_df_with_new_rows_all_blank():
    """Create DataFrame with new rows that have all blank columns."""
    base_df = create_standard_df()

    new_data = {
        "record_id": [3],
        "redcap_event_name": ["scr_arm_1"],
        "redcap_repeat_instrument": [""],
        "field1": [""],
        "field2": [""],
        "field3": [""],
    }
    new_rows_df = _create_dataframe(new_data)

    return pd.concat([base_df, new_rows_df])


def create_df_with_mixed_changes_and_new_rows():
    """Create DataFrame with both existing changes and new rows."""
    # Start with changes
    df = create_df_with_changes()

    # Add one more change
    df.loc[(2, "pre_arm_1"), "field1"] = "changed"

    # Add new row
    new_data = {
        "record_id": [3],
        "redcap_event_name": ["scr_arm_1"],
        "redcap_repeat_instrument": [""],
        "field1": ["new_val"],
        "field2": [""],
        "field3": [50],
    }
    new_rows_df = _create_dataframe(new_data)

    return pd.concat([df, new_rows_df])


def create_df_with_different_index_levels():
    """Create DataFrame with different number of index levels."""
    df = create_standard_df().reset_index()
    return df.set_index(["record_id"])


def create_df_with_different_index_names():
    """Create DataFrame with different index names."""
    df = create_standard_df().reset_index()
    df = df.rename(columns={"redcap_event_name": "different_event"})
    return df.set_index(["record_id", "different_event"])


def create_df_with_matching_index_format_new_values():
    """Create DataFrame with matching index format but new values."""
    data = {
        "record_id": [1, 2, 3],
        "redcap_event_name": ["scr_arm_1", "scr_arm_1", "pre_arm_1"],
        "redcap_repeat_instrument": ["", "", ""],
        "field1": ["a", "b", "new"],
        "field2": ["d", "e", "new2"],
        "field3": [10, 20, 40],
    }
    return _create_dataframe(data)
