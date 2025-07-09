#!/usr/bin/env python

from pathlib import Path
from unittest.mock import patch

import pandas as pd
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
    Returns a pre-defined event map pandas dataframe. This is used across all tests.
    """
    return pd.DataFrame(
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
    data = create_standard_df().reset_index()
    data.update({"redcap_repeat_instrument": ["", "", "meds"]})
    return data


@pytest.fixture
@patch("src.redcap_toolbox.split_redcap_data.pd.read_csv")
def split_data_setup(
    mock_read_csv,
    event_df: pd.DataFrame,
    instance_data: pd.DataFrame,
):
    # Mock dataset and event map
    data = instance_data.set_index(["record_id", "redcap_event_name"])

    event_df.set_index("redcap_event", inplace=True)
    mock_read_csv.return_value = event_df
    event_map = make_event_map("mock_mapping_file.csv")

    return data, event_map


@pytest.fixture
def condense_df_setup():
    """
    Creates test data for condense_df tests.
    """
    data = create_standard_df().reset_index()
    data.update({"redcap_event_name": ["scr", "scr", "pre"], "field2": ["", "", ""]})
    data = data.set_index(["record_id", "redcap_event_name"])
    return data


@patch("src.redcap_toolbox.split_redcap_data.pd.read_csv")
def test_make_event_map_with_file(mock_read_csv, event_df: pd.DataFrame):
    """
    Tests make_event_map with pre-defined event dataframe.
    """
    event_df.set_index("redcap_event", inplace=True)
    mock_read_csv.return_value = event_df

    result = make_event_map("mock_mapping_file.csv")
    expected = {"scr_arm_1": "scr", "pre_arm_1": "pre", "post_arm_1": "post"}

    assert result == expected
    mock_read_csv.assert_called_once_with(
        "mock_mapping_file.csv", index_col="redcap_event", dtype=str
    )


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
    assert scr_df.shape == (2, 4)  # 2 rows, 4 columns
    assert pre_meds_df.shape == (1, 4)  # 1 row, 4 columns


def test_condense_df(condense_df_setup):
    """
    Tests the condense_df function which removes empty columns while keeping reserved columns of
    record ID, redcap_event_name, redcap_repeat_instrument, and redcap_repeat_instance.
    """
    data = condense_df_setup
    result = condense_df(data)

    assert result.shape == (3, 2)  # 3 rows and 3 columns
    assert "field2" not in result.columns
    assert "field1" in result.columns


@patch("src.redcap_toolbox.split_redcap_data.pd.read_csv")
@patch("src.redcap_toolbox.split_redcap_data.pd.DataFrame.to_csv")
@patch("src.redcap_toolbox.split_redcap_data.make_event_map")
def test_split_redcap_data(
    mock_make_event_map,
    mock_to_csv,
    mock_read_csv,
    event_df: pd.DataFrame,
    instance_data: pd.DataFrame,
    split_data_setup,
):
    """
    Tests that the split data main function call works correctly reading in the data, event file
    and generating the correct output files.
    """
    data, event_map = split_data_setup
    data.reset_index(inplace=True)
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
    mock_to_csv.assert_any_call(
        Path("mock_output_dir") / "test_prefix__scr.csv", index=False
    )
    mock_to_csv.assert_any_call(
        Path("mock_output_dir") / "test_prefix__pre__meds.csv", index=False
    )
    assert mock_to_csv.call_count == 2
