#!/usr/bin/env python

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from src.redcap_toolbox.split_redcap_data import (
    make_event_map,
    combine_names,
    split_data,
    condense_df,
    split_redcap_data,
)
from tests.dataframe_factory import create_standard_df


@pytest.fixture
def event_df():
    """
    Returns a pre-defined event map polars dataframe. This is used across all tests.
    """
    return pl.DataFrame(
        {
            "redcap_event": ["scr_arm_1", "pre_arm_1", "post_arm_1"],
            "filename_event": ["scr", "pre", "post"],
        }
    )


@pytest.fixture
def instance_data():
    """
    Creates test data with updated redcap_repeat_instrument column.
    """
    data = create_standard_df()
    # Set meds for the row with (record_id=2, redcap_event_name='pre_arm_1')
    data = data.with_columns(
        pl.when(
            (pl.col("record_id") == 2) & (pl.col("redcap_event_name") == "pre_arm_1")
        )
        .then(pl.lit("meds"))
        .otherwise(pl.lit(""))
        .alias("redcap_repeat_instrument")
    )
    return data


@pytest.fixture
@patch("src.redcap_toolbox.split_redcap_data.pl.read_csv")
def split_data_setup(
    mock_read_csv,
    event_df: pl.DataFrame,
    instance_data: pl.DataFrame,
):
    # Mock dataset and event map
    data = instance_data.sort(["record_id", "redcap_event_name"])

    mock_read_csv.return_value = event_df
    event_map = make_event_map("mock_mapping_file.csv")

    return data, event_map


@pytest.fixture
def condense_df_setup():
    """
    Creates test data for condense_df tests.
    """
    data = create_standard_df()
    # Update specific rows - set field2 to "" for all rows, update event names
    data = data.with_columns(
        pl.when(pl.col("record_id") == 1)
        .then(pl.lit("scr"))
        .when((pl.col("record_id") == 2) & (pl.col("redcap_event_name") == "scr_arm_1"))
        .then(pl.lit("scr"))
        .otherwise(pl.lit("pre"))
        .alias("redcap_event_name"),
        pl.lit("").alias("field2"),
    )
    return data


@patch("src.redcap_toolbox.split_redcap_data.read_csv")
def test_make_event_map_with_file(mock_read_csv, event_df: pl.DataFrame):
    """
    Tests make_event_map with pre-defined event dataframe.
    """
    mock_read_csv.return_value = event_df

    result = make_event_map("mock_mapping_file.csv")
    expected = {"scr_arm_1": "scr", "pre_arm_1": "pre", "post_arm_1": "post"}

    assert result == expected
    mock_read_csv.assert_called_once_with("mock_mapping_file.csv")


def test_make_event_map_no_file():
    """
    Tests the absence of event dataframe. This should return an empty dict.
    """
    result = make_event_map(None)
    assert result == {}


def test_combine_names_with_event_and_rep_name():
    """
    Tests the combine_name call which is used to make file names uniquely when
    provided with both event name and repeat instrument name.
    """
    result = combine_names("scr", "meds")
    assert result == "scr__meds"


def test_combine_names_with_empty_rep_name():
    """
    Tests the combine_name call which is used to make file names uniquely when
    provided with only the event name.
    """
    result = combine_names("scr", "")
    assert result == "scr"


def test_split_data(split_data_setup):
    """
    Tests the data is split correctly in their respective event and
    repeat instrument combinations.
    """
    data, event_map = split_data_setup
    result = split_data(data, event_map)
    assert "scr" in result
    assert "pre__meds" in result

    # Verify resulting DataFrames
    scr_df = result["scr"]
    pre_meds_df = result["pre__meds"]
    assert scr_df.shape == (2, 6)  # 2 rows, 6 columns (all columns preserved)
    assert pre_meds_df.shape == (1, 6)  # 1 row, 6 columns (all columns preserved)


def test_condense_df(condense_df_setup):
    """
    Tests the condense_df function which removes empty columns while keeping reserved columns of
    record ID, redcap_event_name, redcap_repeat_instrument, and redcap_repeat_instance.
    """
    data = condense_df_setup
    result = condense_df(data)

    # Should have: record_id, redcap_event_name, field1, field3
    # field2 and redcap_repeat_instrument are dropped because they're all empty
    assert result.shape == (3, 4)  # 3 rows and 4 columns
    assert "field2" not in result.columns
    assert "field1" in result.columns


@patch("src.redcap_toolbox.split_redcap_data.pl.read_csv")
@patch("src.redcap_toolbox.split_redcap_data.pl.DataFrame.write_csv")
@patch("src.redcap_toolbox.split_redcap_data.make_event_map")
def test_split_redcap_data(
    mock_make_event_map,
    mock_write_csv,
    mock_read_csv,
    event_df: pl.DataFrame,
    instance_data: pl.DataFrame,
    split_data_setup,
):
    """
    Tests that the split data main function call works correctly reading in the data, event file
    and generating the correct output files.
    """
    data, event_map = split_data_setup
    mock_read_csv.return_value = data
    mock_make_event_map.return_value = event_map
    # Run the function
    split_redcap_data(
        "mock_input.csv",
        "mock_output_dir",
        prefix="test_prefix",
        mapping_file="mock_mapping_file.csv",
        condense=False,
    )

    # Ensure files were saved correctly
    mock_write_csv.assert_any_call(Path("mock_output_dir") / "test_prefix__scr.csv")
    mock_write_csv.assert_any_call(
        Path("mock_output_dir") / "test_prefix__pre__meds.csv"
    )
    assert mock_write_csv.call_count == 2
