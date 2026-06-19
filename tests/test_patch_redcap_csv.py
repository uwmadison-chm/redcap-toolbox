#!/usr/bin/env python

import logging

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from src.redcap_toolbox.patch_redcap_csv import (
    Mode,
    PatchSet,
    apply_patches,
    collect_patches,
)
from tests.dataframe_factory import create_standard_df

KEY = ["record_id", "redcap_event_name"]


def patch_df(**columns) -> pl.DataFrame:
    """Build a patch dataframe from string columns."""
    return pl.DataFrame(columns)


def set_cell(df, record_id, event, **fields):
    """Return df with the given fields set on the (record_id, event) row."""
    mask = (pl.col("record_id") == record_id) & (pl.col("redcap_event_name") == event)
    for col, val in fields.items():
        df = df.with_columns(
            pl.when(mask).then(pl.lit(val)).otherwise(pl.col(col)).alias(col)
        )
    return df


def test_no_patches_is_identity():
    base = create_standard_df()
    result = apply_patches(base, [], KEY)
    assert_frame_equal(result, base.sort(KEY))


def test_sets_single_cell():
    base = create_standard_df()
    patch = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["g"])
    result = apply_patches(base, [patch], KEY)
    expected = set_cell(base, 2, "pre_arm_1", field1="g").sort(KEY)
    assert_frame_equal(result, expected)


def test_blank_clears_existing_value():
    base = create_standard_df()
    patch = patch_df(record_id=["2"], redcap_event_name=["scr_arm_1"], field3=[""])
    result = apply_patches(base, [patch], KEY)
    expected = set_cell(base, 2, "scr_arm_1", field3="").sort(KEY)
    assert_frame_equal(result, expected)


def test_absent_column_left_untouched():
    base = create_standard_df()
    # Patch touches field1 only; field2/field3 must be unchanged everywhere.
    patch = patch_df(record_id=["1"], redcap_event_name=["scr_arm_1"], field1=["z"])
    result = apply_patches(base, [patch], KEY)
    expected = set_cell(base, 1, "scr_arm_1", field1="z").sort(KEY)
    assert_frame_equal(result, expected)


def test_longitudinal_targets_only_matching_event():
    base = create_standard_df()
    patch = patch_df(record_id=["2"], redcap_event_name=["scr_arm_1"], field1=["x"])
    result = apply_patches(base, [patch], KEY)
    # record 2 / pre_arm_1 must keep its original field1 ("c").
    expected = set_cell(base, 2, "scr_arm_1", field1="x").sort(KEY)
    assert_frame_equal(result, expected)


def test_missing_key_column_raises():
    base = create_standard_df()
    patch = patch_df(record_id=["2"], field1=["g"])  # no redcap_event_name
    with pytest.raises(ValueError, match="missing key columns"):
        apply_patches(base, [patch], KEY)


def test_unknown_key_raises_by_default():
    base = create_standard_df()
    patch = patch_df(record_id=["99"], redcap_event_name=["scr_arm_1"], field1=["g"])
    with pytest.raises(ValueError, match="unknown key"):
        apply_patches(base, [patch], KEY)


def test_unknown_key_added_with_allow_new():
    base = create_standard_df()
    patch = patch_df(record_id=["3"], redcap_event_name=["scr_arm_1"], field1=["new"])
    result = apply_patches(base, [patch], KEY, allow_new=True)
    assert result.height == base.height + 1
    added = result.filter(
        (pl.col("record_id") == 3) & (pl.col("redcap_event_name") == "scr_arm_1")
    )
    assert added.height == 1
    assert added["field1"][0] == "new"
    assert added["field2"][0] == ""  # unspecified cells default to blank


def test_extra_cols_error_by_default():
    base = create_standard_df()
    patch = patch_df(
        record_id=["1"], redcap_event_name=["scr_arm_1"], qc_note=["looks ok"]
    )
    with pytest.raises(ValueError, match="not in base CSV"):
        apply_patches(base, [patch], KEY)


def test_extra_cols_warn_includes_column(caplog):
    base = create_standard_df()
    patch = patch_df(
        record_id=["1"], redcap_event_name=["scr_arm_1"], qc_note=["looks ok"]
    )
    with caplog.at_level(logging.WARNING):
        result = apply_patches(base, [patch], KEY, extra_cols=Mode.WARN)
    assert "qc_note" in result.columns
    assert any("not in base CSV" in r.message for r in caplog.records)
    row = result.filter(pl.col("record_id") == 1)
    assert row["qc_note"][0] == "looks ok"


def test_extra_cols_allow_is_silent(caplog):
    base = create_standard_df()
    patch = patch_df(record_id=["1"], redcap_event_name=["scr_arm_1"], qc_note=["ok"])
    with caplog.at_level(logging.WARNING):
        result = apply_patches(base, [patch], KEY, extra_cols=Mode.ALLOW)
    assert "qc_note" in result.columns
    assert caplog.records == []


def test_conflict_error_mode_raises():
    base = create_standard_df()
    p1 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["g"])
    p2 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["h"])
    with pytest.raises(ValueError, match="Conflicting values"):
        apply_patches(base, [p1, p2], KEY, cell_conflicts=Mode.ERROR)


def test_conflict_warn_mode_last_wins(caplog):
    base = create_standard_df()
    p1 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["g"])
    p2 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["h"])
    with caplog.at_level(logging.WARNING):
        result = apply_patches(base, [p1, p2], KEY, cell_conflicts=Mode.WARN)
    assert any("Conflicting values" in r.message for r in caplog.records)
    row = result.filter(
        (pl.col("record_id") == 2) & (pl.col("redcap_event_name") == "pre_arm_1")
    )
    assert row["field1"][0] == "h"


def test_conflict_allow_mode_silent(caplog):
    base = create_standard_df()
    p1 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["g"])
    p2 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["h"])
    with caplog.at_level(logging.WARNING):
        result = apply_patches(base, [p1, p2], KEY, cell_conflicts=Mode.ALLOW)
    assert caplog.records == []
    row = result.filter(
        (pl.col("record_id") == 2) & (pl.col("redcap_event_name") == "pre_arm_1")
    )
    assert row["field1"][0] == "h"


def test_same_value_rewrite_is_not_a_conflict(caplog):
    base = create_standard_df()
    p1 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["g"])
    p2 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["g"])
    with caplog.at_level(logging.WARNING):
        apply_patches(base, [p1, p2], KEY, cell_conflicts=Mode.ERROR)
    assert caplog.records == []


def test_apply_is_deterministic():
    base = create_standard_df()
    p1 = patch_df(record_id=["1"], redcap_event_name=["scr_arm_1"], field1=["x"])
    p2 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field2=["y"])
    first = apply_patches(base, [p1, p2], KEY)
    second = apply_patches(base, [p1, p2], KEY)
    assert_frame_equal(first, second)


def collect(
    base_keys,
    base_cols,
    patches,
    *,
    allow_new=False,
    extra_cols=Mode.ERROR,
    cell_conflicts=Mode.WARN,
):
    """collect_patches with the public defaults filled in."""
    return collect_patches(
        base_keys,
        base_cols,
        patches,
        KEY,
        allow_new=allow_new,
        extra_cols=extra_cols,
        cell_conflicts=cell_conflicts,
    )


def test_collect_patches_folds_last_write_wins():
    base_keys = {("2", "pre_arm_1")}
    p1 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field1=["g"])
    p2 = patch_df(record_id=["2"], redcap_event_name=["pre_arm_1"], field2=["h"])
    result = collect(base_keys, ["field1", "field2"], [p1, p2])
    assert isinstance(result, PatchSet)
    assert result.cell_values[("2", "pre_arm_1")] == {"field1": "g", "field2": "h"}
    assert result.new_keys == []
    assert result.extra_cols == []


def test_collect_patches_records_new_keys_in_order():
    base_keys = {("1", "scr_arm_1")}
    p1 = patch_df(record_id=["3"], redcap_event_name=["scr_arm_1"], field1=["c"])
    p2 = patch_df(record_id=["2"], redcap_event_name=["scr_arm_1"], field1=["b"])
    result = collect(base_keys, ["field1"], [p1, p2], allow_new=True)
    assert result.new_keys == [("3", "scr_arm_1"), ("2", "scr_arm_1")]


def test_collect_patches_tracks_extra_cols_once():
    base_keys = {("1", "scr_arm_1")}
    p1 = patch_df(record_id=["1"], redcap_event_name=["scr_arm_1"], qc_note=["a"])
    p2 = patch_df(record_id=["1"], redcap_event_name=["scr_arm_1"], qc_note=["b"])
    result = collect(base_keys, [], [p1, p2], extra_cols=Mode.ALLOW)
    assert result.extra_cols == ["qc_note"]


def test_mode_from_str_is_case_insensitive():
    assert Mode.from_str("ERROR") is Mode.ERROR
    assert Mode.from_str(" Warn ") is Mode.WARN
    assert Mode.from_str("allow") is Mode.ALLOW


def test_mode_from_str_invalid_raises():
    with pytest.raises(ValueError, match="Invalid mode"):
        Mode.from_str("nope")
