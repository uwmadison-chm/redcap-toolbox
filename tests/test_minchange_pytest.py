#!/usr/bin/env python

import pytest

from src.redcap_toolbox.minchange import transformation_dicts
from tests.DataFrameSetup import DataFrameSetup


@pytest.fixture
def setup_df():
    """
    Fixture that sets up the dict of data frames for testing. Uses default values from
    DataFrameSetup.
    """
    df = DataFrameSetup()
    source_df = df.source_df
    wrong_index_df = df.wrong_index_df
    extra_columns_df = df.extra_columns_df
    diff_df = df.diff_df

    return {
        "source_df": source_df,
        "diff_df": diff_df,
        "wrong_index_df": wrong_index_df,
        "extra_columns_df": extra_columns_df,
    }


def test_transformation_dicts_no_changes(setup_df):
    """
    Test when there are no differences between source and target.
    ."""
    source_df = setup_df["source_df"]
    copy_df = setup_df["source_df"].copy()

    result = transformation_dicts(source_df, copy_df)
    assert result == []


def test_transformation_dicts_with_changes(setup_df):
    """
    Test when there are differences between source and target.
    """
    source_df = setup_df["source_df"]
    diff_df = setup_df["diff_df"]

    result = transformation_dicts(source_df, diff_df)
    expected = [
        {"record_id": 2, "redcap_event_name": "scr_arm_1", "field3": 40},
        {"record_id": 2, "redcap_event_name": "pre_arm_1", "field1": "g"},
    ]
    assert result == expected


def test_transformation_dicts_different_indexes(setup_df):
    """
    Test when source and target DataFrames have different indexes.
    """
    source_df = setup_df["source_df"]
    wrong_index_df = setup_df["wrong_index_df"]

    with pytest.raises(
        ValueError, match="Source and target dfs have different indexes"
    ):
        transformation_dicts(source_df, wrong_index_df)


def test_transformation_dicts_different_columns(setup_df):
    """
    Test when source and target DataFrames have different columns.
    """
    source_df = setup_df["source_df"]
    extra_columns_df = setup_df["extra_columns_df"]

    with pytest.raises(
        ValueError, match="Source and target dfs have different columns"
    ):
        transformation_dicts(source_df, extra_columns_df)
