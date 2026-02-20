#!/usr/bin/env python

from datetime import datetime, timedelta

import polars as pl
import pytest

from redcap_toolbox.download_redcap_incremental import (
    key_cols_for,
    merge,
    parse_overlap,
    read_csv,
    run,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_df(data: dict) -> pl.DataFrame:
    """Build a string-typed DataFrame for use in merge tests."""
    return pl.DataFrame({k: [str(v) for v in vs] for k, vs in data.items()})


SIMPLE_CSV = "record_id,field1\n1,a\n2,b\n"
UPDATED_CSV = "record_id,field1\n2,updated\n3,new\n"
EMPTY_CSV = "record_id,field1\n"


@pytest.fixture
def output_file(tmp_path):
    return tmp_path / "study.csv"


# ---------------------------------------------------------------------------
# read_csv
# ---------------------------------------------------------------------------


def test_read_csv_path_all_strings(tmp_path):
    p = tmp_path / "test.csv"
    p.write_text("record_id,age\n1,25\n2,30\n")
    df = read_csv(p)
    assert all(t == pl.String for t in df.dtypes)


def test_read_csv_bytes_all_strings():
    df = read_csv(b"record_id,age\n1,25\n2,30\n")
    assert all(t == pl.String for t in df.dtypes)


def test_read_csv_empty_field_is_empty_string(tmp_path):
    p = tmp_path / "test.csv"
    p.write_text("record_id,field1\n1,\n2,x\n")
    df = read_csv(p)
    assert df["field1"][0] == ""
    assert df["field1"][1] == "x"


def test_read_csv_bytes_empty_field_is_empty_string():
    df = read_csv(b"record_id,field1\n1,\n2,x\n")
    assert df["field1"][0] == ""


# ---------------------------------------------------------------------------
# key_cols_for
# ---------------------------------------------------------------------------


def test_key_cols_simple_project():
    df = make_df({"record_id": ["1"], "field1": ["a"]})
    assert key_cols_for(df) == ["record_id"]


def test_key_cols_longitudinal():
    df = make_df(
        {"record_id": ["1"], "redcap_event_name": ["scr_arm_1"], "field1": ["a"]}
    )
    assert key_cols_for(df) == ["record_id", "redcap_event_name"]


def test_key_cols_repeating_instruments():
    df = make_df(
        {
            "record_id": ["1"],
            "redcap_event_name": ["scr_arm_1"],
            "redcap_repeat_instrument": [""],
            "redcap_repeat_instance": ["1"],
            "field1": ["a"],
        }
    )
    assert key_cols_for(df) == [
        "record_id",
        "redcap_event_name",
        "redcap_repeat_instrument",
        "redcap_repeat_instance",
    ]


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


def test_merge_incremental_record_overwrites_base():
    base = make_df({"record_id": ["1", "2"], "field1": ["a", "b"]})
    inc = make_df({"record_id": ["2"], "field1": ["updated"]})
    result = merge(base, inc)
    assert result.filter(pl.col("record_id") == "2")["field1"][0] == "updated"


def test_merge_unchanged_record_preserved():
    base = make_df({"record_id": ["1", "2"], "field1": ["a", "b"]})
    inc = make_df({"record_id": ["2"], "field1": ["updated"]})
    result = merge(base, inc)
    assert result.filter(pl.col("record_id") == "1")["field1"][0] == "a"


def test_merge_new_record_in_incremental_is_added():
    base = make_df({"record_id": ["1"], "field1": ["a"]})
    inc = make_df({"record_id": ["2"], "field1": ["b"]})
    result = merge(base, inc)
    assert len(result) == 2


def test_merge_new_column_in_incremental_fills_base_with_empty_string():
    base = make_df({"record_id": ["1", "2"], "field1": ["a", "b"]})
    inc = make_df({"record_id": ["2"], "field1": ["updated"], "field2": ["new"]})
    result = merge(base, inc)
    assert result.filter(pl.col("record_id") == "1")["field2"][0] == ""
    assert result.filter(pl.col("record_id") == "2")["field2"][0] == "new"


def test_merge_missing_column_in_incremental_raises():
    base = make_df({"record_id": ["1"], "field1": ["a"], "field2": ["x"]})
    inc = make_df({"record_id": ["1"], "field1": ["updated"]})
    with pytest.raises(ValueError, match="missing columns"):
        merge(base, inc)


def test_merge_longitudinal_keys_only_matching_event_updated():
    base = make_df(
        {
            "record_id": ["1", "1"],
            "redcap_event_name": ["scr_arm_1", "pre_arm_1"],
            "field1": ["a", "b"],
        }
    )
    inc = make_df(
        {"record_id": ["1"], "redcap_event_name": ["scr_arm_1"], "field1": ["updated"]}
    )
    result = merge(base, inc)
    assert len(result) == 2
    assert (
        result.filter(pl.col("redcap_event_name") == "scr_arm_1")["field1"][0]
        == "updated"
    )
    assert result.filter(pl.col("redcap_event_name") == "pre_arm_1")["field1"][0] == "b"


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


from unittest.mock import patch


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_first_run_creates_incremental_dir_and_files(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))

    inc_dir = output_file.parent / ".incremental"
    assert (inc_dir / "base.csv").exists()
    assert (inc_dir / ".last_download").exists()


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_first_run_writes_output_file(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))

    assert output_file.exists()
    df = read_csv(output_file)
    assert len(df) == 2


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_first_run_calls_export_without_date(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))
    mock_export.assert_called_once_with()


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_second_run_calls_export_with_date_begin(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))

    mock_export.return_value = EMPTY_CSV
    run(output_file, overlap=timedelta(hours=24))

    assert mock_export.call_count == 2
    _, second_call = mock_export.call_args_list
    assert second_call.kwargs.get("date_begin") is not None


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_second_run_date_begin_respects_overlap(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    before_first = datetime.now().astimezone()
    run(output_file, overlap=timedelta(hours=24))
    after_first = datetime.now().astimezone()

    mock_export.return_value = EMPTY_CSV
    run(output_file, overlap=timedelta(hours=24))

    _, second_call = mock_export.call_args_list
    date_begin = second_call.kwargs["date_begin"]
    # date_begin should be approximately (first run start - 24h)
    assert before_first - timedelta(hours=24, seconds=1) <= date_begin
    assert date_begin <= after_first - timedelta(hours=24) + timedelta(seconds=1)


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_second_run_merges_updates_and_new_records(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))

    mock_export.return_value = UPDATED_CSV
    run(output_file, overlap=timedelta(hours=24))

    df = read_csv(output_file)
    assert len(df) == 3
    assert df.filter(pl.col("record_id") == "2")["field1"][0] == "updated"
    assert df.filter(pl.col("record_id") == "3")["field1"][0] == "new"


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_second_run_no_new_records_preserves_output(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))

    mock_export.return_value = EMPTY_CSV
    run(output_file, overlap=timedelta(hours=24))

    df = read_csv(output_file)
    assert len(df) == 2


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_timestamp_is_recorded_before_download(mock_export, output_file):
    before = datetime.now().astimezone()
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))
    after = datetime.now().astimezone()

    ts_text = (output_file.parent / ".incremental" / ".last_download").read_text()
    ts = datetime.fromisoformat(ts_text)
    assert before <= ts <= after


@patch("redcap_toolbox.download_redcap_incremental.export_records")
def test_timestamp_updated_after_second_run(mock_export, output_file):
    mock_export.return_value = SIMPLE_CSV
    run(output_file, overlap=timedelta(hours=24))
    ts_file = output_file.parent / ".incremental" / ".last_download"
    first_ts = datetime.fromisoformat(ts_file.read_text())

    mock_export.return_value = EMPTY_CSV
    run(output_file, overlap=timedelta(hours=24))
    second_ts = datetime.fromisoformat(ts_file.read_text())

    assert second_ts >= first_ts


# ---------------------------------------------------------------------------
# parse_overlap
# ---------------------------------------------------------------------------


def test_parse_overlap_bare_number_is_seconds():
    assert parse_overlap("60") == timedelta(seconds=60)


def test_parse_overlap_seconds():
    assert parse_overlap("90s") == timedelta(seconds=90)


def test_parse_overlap_minutes():
    assert parse_overlap("5m") == timedelta(minutes=5)


def test_parse_overlap_hours():
    assert parse_overlap("24h") == timedelta(hours=24)


def test_parse_overlap_days():
    assert parse_overlap("3d") == timedelta(days=3)


def test_parse_overlap_fractional():
    assert parse_overlap("1.5h") == timedelta(hours=1.5)


def test_parse_overlap_invalid_raises():
    with pytest.raises(ValueError):
        parse_overlap("5x")


def test_parse_overlap_empty_raises():
    with pytest.raises(ValueError):
        parse_overlap("")
