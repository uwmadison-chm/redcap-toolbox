#!/usr/bin/env python

"""
A function to return a dict with the minimal changes to transform
one dataframe to another (identically-shaped) dataframe.
"""

from typing import Any

import polars as pl


def transformation_dicts(
    source_df: pl.DataFrame,
    target_df: pl.DataFrame,
    key_cols: list[str],
    allow_new: bool = False,
) -> list[dict[str, Any]]:
    """
    Calculates the minimal set of changes to transform source_df into target_df.
    source_df and target_df must be identically-shaped -- in particular,
    the key columns must match, and source_df.columns must equal target_df.columns.

    This is the main function for this module.

    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        key_cols: List of column names that form the composite key (replaces pandas index)
        allow_new: Whether to allow new rows in target_df

    Each dict looks like:
    {
            "key_col_1": key_value_1,
            ...
            "key_col_n": key_value_n,
            "column_name_1": new_value_1,
            ...
            "column_name_n": new_value_n
    }
    Rows that have no changes are not included.
    """
    # Validate key columns exist in both DataFrames
    if not _key_columns_match(source_df, target_df, key_cols):
        raise ValueError("Key columns do not match")

    # Check columns match
    if source_df.columns != target_df.columns:
        raise ValueError("Source and target dfs have different columns")

    # Check for duplicate key combinations
    _check_no_duplicate_keys(source_df, key_cols, "Source")
    _check_no_duplicate_keys(target_df, key_cols, "Target")

    # Handle different cases based on allow_new
    if allow_new:
        return _handle_with_new_rows(source_df, target_df, key_cols)
    else:
        # Original behavior - key values must be identical
        source_keys = source_df.select(key_cols).sort(key_cols)
        target_keys = target_df.select(key_cols).sort(key_cols)
        if not source_keys.equals(target_keys):
            raise ValueError("Source and target dfs have different key values")
        return _compare_dataframes(source_df, target_df, key_cols)


def get_common_keys(
    df1: pl.DataFrame, df2: pl.DataFrame, key_cols: list[str]
) -> pl.DataFrame:
    """
    Get keys that exist in both dataframes (like pandas index.intersection).

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        key_cols: List of column names that form the composite key

    Returns:
        DataFrame containing only the key columns with values present in both df1 and df2
    """
    return (
        df1.select(key_cols)
        .join(df2.select(key_cols), on=key_cols, how="inner")
        .unique()
    )


def get_unique_keys(
    df1: pl.DataFrame, df2: pl.DataFrame, key_cols: list[str]
) -> pl.DataFrame:
    """
    Get keys in df2 but not in df1 (like pandas index.difference).

    Args:
        df1: First DataFrame (keys to exclude)
        df2: Second DataFrame (source of keys)
        key_cols: List of column names that form the composite key

    Returns:
        DataFrame containing only the key columns with values in df2 but not in df1
    """
    return (
        df2.select(key_cols)
        .join(df1.select(key_cols), on=key_cols, how="anti")
        .unique()
    )


def _key_columns_match(
    source_df: pl.DataFrame, target_df: pl.DataFrame, key_cols: list[str]
) -> bool:
    """Check if both DataFrames have the required key columns."""
    return all(col in source_df.columns for col in key_cols) and all(
        col in target_df.columns for col in key_cols
    )


def _check_no_duplicate_keys(df: pl.DataFrame, key_cols: list[str], label: str) -> None:
    """Raise ValueError if the DataFrame has duplicate key combinations."""
    if df.select(key_cols).is_duplicated().any():
        raise ValueError(f"{label} DataFrame has duplicate key combinations")


def _handle_with_new_rows(
    source_df: pl.DataFrame, target_df: pl.DataFrame, key_cols: list[str]
) -> list[dict[str, Any]]:
    """Handle transformation when new rows are allowed."""
    results = []

    # Handle existing rows (rows in both source and target)
    common_keys = get_common_keys(source_df, target_df, key_cols)
    if len(common_keys) > 0:
        source_common = source_df.join(common_keys, on=key_cols, how="inner")
        target_common = target_df.join(common_keys, on=key_cols, how="inner")
        results.extend(_compare_dataframes(source_common, target_common, key_cols))

    # Handle new rows (rows in target but not in source)
    new_keys = get_unique_keys(source_df, target_df, key_cols)
    if len(new_keys) > 0:
        new_rows = target_df.join(new_keys, on=key_cols, how="inner")
        for row_dict in new_rows.iter_rows(named=True):
            results.append(_create_new_row_dict(row_dict, key_cols))

    return results


def _create_new_row_dict(
    row_dict: dict[str, Any], key_cols: list[str]
) -> dict[str, Any]:
    """Create dictionary for new row, filtering out blank columns."""
    # Start with key columns
    result = {k: row_dict[k] for k in key_cols}

    # Add non-blank value columns
    for col_name, value in row_dict.items():
        if col_name not in key_cols:
            if value != "" and value is not None:
                result[col_name] = value

    # Always return the dict (even if only keys) - this handles the case
    # where all columns are blank but we still want to include the keys
    return result


def _compare_dataframes(
    source_df: pl.DataFrame, target_df: pl.DataFrame, key_cols: list[str]
) -> list[dict[str, Any]]:
    """
    Compare two DataFrames and return list of dicts with changes.
    Replaces the old pandas .ne() approach with explicit polars comparisons.
    """
    results = []

    # Get value columns (non-key columns)
    value_cols = [c for c in source_df.columns if c not in key_cols]

    # Sort both DataFrames by key columns to ensure alignment
    source_sorted = source_df.sort(key_cols)
    target_sorted = target_df.sort(key_cols)

    # Compare row by row
    for source_row, target_row in zip(
        source_sorted.iter_rows(named=True), target_sorted.iter_rows(named=True)
    ):
        # Start with key columns
        changes = {k: target_row[k] for k in key_cols}
        has_changes = False

        # Check each value column for differences
        for col in value_cols:
            if source_row[col] != target_row[col]:
                changes[col] = target_row[col]
                has_changes = True

        # Only include rows with actual changes
        if has_changes:
            results.append(changes)

    return results
