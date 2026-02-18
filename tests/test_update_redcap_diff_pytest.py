#!/usr/bin/env python

import pytest
import tempfile
import os
import polars as pl
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
        base_df = pl.DataFrame(base_data)
        base_df.write_csv(base_file.name)
        base_path = base_file.name

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as updated_file:
        updated_df = pl.DataFrame(updated_data)
        updated_df.write_csv(updated_file.name)
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
        ValueError, match="Source and target dfs have different key values"
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
        "--background": False,
    }

    main()

    # Verify update_redcap_diff was called with allow_new=True, background_import=False
    mock_update_func.assert_called_once_with(
        base_path, updated_path, False, True, False
    )


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
        "--background": False,
    }

    main()

    # Verify update_redcap_diff was called with allow_new=False, background_import=False
    mock_update_func.assert_called_once_with(
        base_path, updated_path, False, False, False
    )


@patch("src.redcap_toolbox.update_redcap_diff.PROJ")
def test_update_redcap_diff_with_background_import_true(mock_proj, temp_csv_files):
    """Test that update_redcap_diff passes background_import=True to import_records."""
    base_path, updated_path = temp_csv_files

    # Mock the import_records method
    mock_proj.import_records.return_value = {"count": 1}

    # Call with background_import=True
    update_redcap_diff(
        base_path, updated_path, dry_run=False, allow_new=True, background_import=True
    )

    # Verify import_records was called with background_import=True
    mock_proj.import_records.assert_called_once()
    call_kwargs = mock_proj.import_records.call_args[1]
    assert call_kwargs.get("background_import") is True


@patch("src.redcap_toolbox.update_redcap_diff.PROJ")
def test_update_redcap_diff_with_background_import_false(mock_proj, temp_csv_files):
    """Test that update_redcap_diff passes background_import=None when False."""
    base_path, updated_path = temp_csv_files

    # Mock the import_records method
    mock_proj.import_records.return_value = {"count": 1}

    # Call with background_import=False (default)
    update_redcap_diff(
        base_path, updated_path, dry_run=False, allow_new=True, background_import=False
    )

    # Verify import_records was called with background_import=None
    mock_proj.import_records.assert_called_once()
    call_kwargs = mock_proj.import_records.call_args[1]
    assert call_kwargs.get("background_import") is None


@patch("redcap.Project")
@patch("docopt.docopt")
@patch("src.redcap_toolbox.update_redcap_diff.update_redcap_diff")
@patch.dict(
    os.environ, {"REDCAP_API_URL": "test_url", "REDCAP_API_TOKEN": "test_token"}
)
def test_main_with_background_flag(
    mock_update_func, mock_docopt, mock_redcap, temp_csv_files
):
    """Test that main function parses --background flag correctly."""
    base_path, updated_path = temp_csv_files

    # Mock docopt to return background flag
    mock_docopt.return_value = {
        "<base_csv>": base_path,
        "<updated_csv>": updated_path,
        "--dry-run": False,
        "--allow-new": False,
        "--verbose": False,
        "--background": True,
    }

    main()

    # Verify update_redcap_diff was called with background_import=True
    mock_update_func.assert_called_once_with(
        base_path, updated_path, False, False, True
    )
