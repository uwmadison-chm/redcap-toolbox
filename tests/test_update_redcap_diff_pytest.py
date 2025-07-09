#!/usr/bin/env python

import pytest
import tempfile
import os
import pandas as pd
from unittest.mock import patch, MagicMock

from src.redcap_toolbox.update_redcap_diff import update_redcap_diff, main


@pytest.fixture
def temp_csv_files():
    """Create temporary CSV files for testing."""
    base_data = {
        "record_id": [1, 2, 2],
        "redcap_event_name": ["scr_arm_1", "scr_arm_1", "pre_arm_1"],
        "field1": ["a", "b", "c"],
        "field2": ["d", "e", "f"],
        "field3": [10, 20, 30],
    }

    # Updated data with new rows
    updated_data = {
        "record_id": [1, 2, 2, 3],
        "redcap_event_name": ["scr_arm_1", "scr_arm_1", "pre_arm_1", "scr_arm_1"],
        "field1": ["a", "b", "c", "new_val"],
        "field2": ["d", "e", "f", ""],
        "field3": [10, 20, 30, 50],
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as base_file:
        base_df = pd.DataFrame(base_data)
        base_df.to_csv(base_file.name, index=False)
        base_path = base_file.name

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as updated_file:
        updated_df = pd.DataFrame(updated_data)
        updated_df.to_csv(updated_file.name, index=False)
        updated_path = updated_file.name

    yield base_path, updated_path

    # Cleanup
    os.unlink(base_path)
    os.unlink(updated_path)


@patch("src.redcap_toolbox.update_redcap_diff.PROJ")
def test_update_redcap_diff_with_allow_new_flag(mock_proj, temp_csv_files):
    """Test that update_redcap_diff function accepts allow_new parameter."""
    base_path, updated_path = temp_csv_files

    # Mock the import_records method
    mock_proj.import_records.return_value = {"count": 1}

    # This should work when allow_new=True
    update_redcap_diff(base_path, updated_path, dry_run=False, allow_new=True)

    # Verify import_records was called
    mock_proj.import_records.assert_called_once()

    # Get the arguments passed to import_records
    call_args = mock_proj.import_records.call_args[0][0]

    # Should include the new row
    assert len(call_args) == 1
    assert call_args[0]["record_id"] == "3"
    assert call_args[0]["redcap_event_name"] == "scr_arm_1"
    assert call_args[0]["field1"] == "new_val"
    assert call_args[0]["field3"] == "50"
    # field2 should not be included since it's blank
    assert "field2" not in call_args[0]


@patch("src.redcap_toolbox.update_redcap_diff.PROJ")
def test_update_redcap_diff_without_allow_new_flag(mock_proj, temp_csv_files):
    """Test that update_redcap_diff function rejects new rows when allow_new=False."""
    base_path, updated_path = temp_csv_files

    # This should raise an error when allow_new=False (default)
    with pytest.raises(
        ValueError, match="Source and target dfs have different indexes"
    ):
        update_redcap_diff(base_path, updated_path, dry_run=False, allow_new=False)

    # Verify import_records was not called
    mock_proj.import_records.assert_not_called()


@patch("src.redcap_toolbox.update_redcap_diff.PROJ")
def test_update_redcap_diff_dry_run_with_new_rows(mock_proj, temp_csv_files):
    """Test dry run mode with new rows."""
    base_path, updated_path = temp_csv_files

    update_redcap_diff(base_path, updated_path, dry_run=True, allow_new=True)

    # Verify import_records was not called in dry run
    mock_proj.import_records.assert_not_called()


@patch("redcap.Project")
@patch("docopt.docopt")
@patch("src.redcap_toolbox.update_redcap_diff.update_redcap_diff")
@patch.dict(
    os.environ, {"REDCAP_API_URL": "test_url", "REDCAP_API_TOKEN": "test_token"}
)
def test_main_with_allow_new_flag(
    mock_update_func, mock_docopt, mock_redcap, temp_csv_files
):
    """Test that main function parses --allow-new flag correctly."""
    base_path, updated_path = temp_csv_files

    # Mock docopt to return allow-new flag
    mock_docopt.return_value = {
        "<base_csv>": base_path,
        "<updated_csv>": updated_path,
        "--dry-run": False,
        "--allow-new": True,
        "--verbose": False,
    }

    main()

    # Verify update_redcap_diff was called with allow_new=True
    mock_update_func.assert_called_once_with(base_path, updated_path, False, True)


@patch("redcap.Project")
@patch("docopt.docopt")
@patch("src.redcap_toolbox.update_redcap_diff.update_redcap_diff")
@patch.dict(
    os.environ, {"REDCAP_API_URL": "test_url", "REDCAP_API_TOKEN": "test_token"}
)
def test_main_without_allow_new_flag(
    mock_update_func, mock_docopt, mock_redcap, temp_csv_files
):
    """Test that main function defaults allow_new to False when flag not provided."""
    base_path, updated_path = temp_csv_files

    # Mock docopt to return no allow-new flag
    mock_docopt.return_value = {
        "<base_csv>": base_path,
        "<updated_csv>": updated_path,
        "--dry-run": False,
        "--allow-new": False,
        "--verbose": False,
    }

    main()

    # Verify update_redcap_diff was called with allow_new=False
    mock_update_func.assert_called_once_with(base_path, updated_path, False, False)
