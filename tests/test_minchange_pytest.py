#!/usr/bin/env python

import pytest

from src.redcap_toolbox.minchange import transformation_dicts
from tests.dataframe_factory import (
    create_standard_df,
    create_df_with_changes,
    create_df_with_wrong_index,
    create_df_with_extra_columns,
    create_df_with_new_rows,
    create_df_with_new_rows_all_blank,
    create_df_with_mixed_changes_and_new_rows,
    create_df_with_different_index_levels,
    create_df_with_different_index_names,
    create_df_with_matching_index_format_new_values,
    create_df_with_duplicate_keys,
)


def test_transformation_dicts_no_changes():
    """
    Test when there are no differences between source and target.
    """
    source_df = create_standard_df()
    copy_df = create_standard_df()

    result = transformation_dicts(
        source_df, copy_df, key_cols=["record_id", "redcap_event_name"]
    )
    assert result == []


def test_transformation_dicts_with_changes():
    """
    Test when there are differences between source and target.
    """
    source_df = create_standard_df()
    diff_df = create_df_with_changes()

    result = transformation_dicts(
        source_df, diff_df, key_cols=["record_id", "redcap_event_name"]
    )
    expected = [
        {"record_id": 2, "redcap_event_name": "pre_arm_1", "field1": "g"},
        {"record_id": 2, "redcap_event_name": "scr_arm_1", "field3": "40"},
    ]
    assert result == expected


def test_transformation_dicts_different_indexes():
    """
    Test when source and target DataFrames have different key columns.
    """
    source_df = create_standard_df()
    wrong_index_df = create_df_with_wrong_index()

    # Test with key_cols that exist in source but not in target (redcap_event_name doesn't exist in wrong_index_df)
    with pytest.raises(ValueError, match="Key columns do not match"):
        transformation_dicts(
            source_df, wrong_index_df, key_cols=["record_id", "redcap_event_name"]
        )


def test_transformation_dicts_different_columns():
    """
    Test when source and target DataFrames have different columns.
    """
    source_df = create_standard_df()
    extra_columns_df = create_df_with_extra_columns()

    with pytest.raises(
        ValueError, match="Source and target dfs have different columns"
    ):
        transformation_dicts(
            source_df, extra_columns_df, key_cols=["record_id", "redcap_event_name"]
        )


# Tests for new row functionality with allow_new=True
def test_transformation_dicts_new_rows_allow_new_true():
    """
    Test adding new rows when allow_new=True.
    """
    source_df = create_standard_df()
    target_df = create_df_with_new_rows()

    result = transformation_dicts(
        source_df,
        target_df,
        key_cols=["record_id", "redcap_event_name"],
        allow_new=True,
    )

    # Should include new rows with only non-blank columns
    expected = [
        {
            "record_id": 3,
            "redcap_event_name": "scr_arm_1",
            "field1": "new_val",
            "field3": "50",
        },
        {
            "record_id": 4,
            "redcap_event_name": "pre_arm_1",
            "field2": "new_val2",
            "field3": "60",
        },
    ]
    assert result == expected


def test_transformation_dicts_new_rows_allow_new_false():
    """
    Test that new rows are rejected when allow_new=False (default behavior).
    """
    source_df = create_standard_df()
    target_df = create_df_with_new_rows()

    with pytest.raises(
        ValueError, match="Source and target dfs have different key values"
    ):
        transformation_dicts(
            source_df,
            target_df,
            key_cols=["record_id", "redcap_event_name"],
            allow_new=False,
        )


def test_transformation_dicts_new_rows_all_blank_columns():
    """
    Test that new rows with all blank columns still include the key columns.
    """
    source_df = create_standard_df()
    target_df = create_df_with_new_rows_all_blank()

    result = transformation_dicts(
        source_df,
        target_df,
        key_cols=["record_id", "redcap_event_name"],
        allow_new=True,
    )

    # Should include the key columns even with all blank value columns
    expected = [
        {"record_id": 3, "redcap_event_name": "scr_arm_1"},
    ]
    assert result == expected


def test_transformation_dicts_mixed_existing_and_new_rows():
    """
    Test scenario with both existing row changes and new rows.
    """
    source_df = create_standard_df()
    target_df = create_df_with_mixed_changes_and_new_rows()

    result = transformation_dicts(
        source_df,
        target_df,
        key_cols=["record_id", "redcap_event_name"],
        allow_new=True,
    )

    # Should include both existing changes and new rows
    expected = [
        {"record_id": 2, "redcap_event_name": "pre_arm_1", "field1": "changed"},
        {"record_id": 2, "redcap_event_name": "scr_arm_1", "field3": "40"},
        {
            "record_id": 3,
            "redcap_event_name": "scr_arm_1",
            "field1": "new_val",
            "field3": "50",
        },
    ]
    assert result == expected


def test_transformation_dicts_index_format_mismatch_different_levels():
    """
    Test rejection when key column lists have different lengths.
    """
    source_df = create_standard_df()
    target_df = create_df_with_different_index_levels()

    with pytest.raises(ValueError, match="Key columns do not match"):
        transformation_dicts(
            source_df,
            target_df,
            key_cols=["record_id", "redcap_event_name"],
            allow_new=True,
        )


def test_transformation_dicts_index_format_mismatch_different_names():
    """
    Test rejection when key column names don't match.
    """
    source_df = create_standard_df()
    target_df = create_df_with_different_index_names()

    with pytest.raises(ValueError, match="Key columns do not match"):
        transformation_dicts(
            source_df,
            target_df,
            key_cols=["record_id", "redcap_event_name"],
            allow_new=True,
        )


def test_transformation_dicts_duplicate_keys_in_source():
    """Duplicate key combinations in source should raise ValueError."""
    source_df = create_df_with_duplicate_keys()
    target_df = create_df_with_duplicate_keys()

    with pytest.raises(ValueError, match="duplicate key combinations"):
        transformation_dicts(
            source_df, target_df, key_cols=["record_id", "redcap_event_name"]
        )


def test_transformation_dicts_duplicate_keys_in_target():
    """Duplicate key combinations in target should raise ValueError."""
    source_df = create_standard_df()
    target_df = create_df_with_duplicate_keys()

    with pytest.raises(ValueError, match="duplicate key combinations"):
        transformation_dicts(
            source_df, target_df, key_cols=["record_id", "redcap_event_name"]
        )


def test_transformation_dicts_index_format_match_different_values():
    """
    Test acceptance when key columns match but have different values (new rows).
    """
    source_df = create_standard_df()
    target_df = create_df_with_matching_index_format_new_values()

    result = transformation_dicts(
        source_df,
        target_df,
        key_cols=["record_id", "redcap_event_name"],
        allow_new=True,
    )

    # Should accept since key columns match
    expected = [
        {
            "record_id": 3,
            "redcap_event_name": "pre_arm_1",
            "field1": "new",
            "field2": "new2",
            "field3": "40",
        },
    ]
    assert result == expected
