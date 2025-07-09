#!/usr/bin/env python

"""
A function to return a dict with the minimal changes to transform
one dataframe to another (identically-shaped) dataframe.
"""

from collections.abc import Iterable


def transformation_dicts(source_df, target_df, allow_new=False):
    """
    Calculates the minimal set of changes to transform source_df into target_df.
    source_df and target_df must be identically-shaped -- in particular,
    source_df.index must equal target_df.index, and source_df.columns must equal
    target_df.columns.

    Each dict looks like:
    {
            "index_name_1": index_value_1,
            ...
            "index_name_n": index_value_n,
            "column_name_1": new_value_1,
            ...
            "column_name_n": new_value_n
    }
    Rows that have no changes are not included.
    """
    # Validate index formats match
    if not _index_formats_match(source_df, target_df):
        raise ValueError("Index formats do not match")

    # Check columns match
    if not source_df.columns.equals(target_df.columns):
        raise ValueError("Source and target dfs have different columns")

    # Handle different cases based on allow_new
    if allow_new:
        return _handle_with_new_rows(source_df, target_df)
    else:
        # Original behavior - indexes must be identical
        if not source_df.index.equals(target_df.index):
            raise ValueError("Source and target dfs have different indexes")
        diff = source_df.ne(target_df)
        return [
            d
            for (idx, vals) in diff.iterrows()
            if (d := diff_dict(idx, vals, target_df))
        ]


def _index_formats_match(source_df, target_df):
    """Check if index formats match (same names and number of levels)."""
    return source_df.index.names == target_df.index.names and len(
        source_df.index.names
    ) == len(target_df.index.names)


def _handle_with_new_rows(source_df, target_df):
    """Handle transformation when new rows are allowed."""
    results = []

    # Handle existing rows (rows in both source and target)
    common_idx = source_df.index.intersection(target_df.index)
    if len(common_idx) > 0:
        source_common = source_df.loc[common_idx]
        target_common = target_df.loc[common_idx]
        diff = source_common.ne(target_common)
        results.extend(
            [
                d
                for (idx, vals) in diff.iterrows()
                if (d := diff_dict(idx, vals, target_df))
            ]
        )

    # Handle new rows (rows in target but not in source)
    new_idx = target_df.index.difference(source_df.index)
    if len(new_idx) > 0:
        new_rows = target_df.loc[new_idx]
        for idx, row in new_rows.iterrows():
            new_row_dict = _create_new_row_dict(idx, row, target_df)
            if new_row_dict:
                results.append(new_row_dict)

    return results


def _create_new_row_dict(idx, row, target_df):
    """Create dictionary for new row, filtering out blank columns."""
    if not isinstance(idx, Iterable) or isinstance(idx, str):
        idx = (idx,)

    # Start with index
    result = dict(zip(target_df.index.names, idx))

    # Add non-blank columns
    for col_name, value in row.items():
        if value != "" and str(value) != "nan":
            result[col_name] = value

    # Always return the dict (even if only index) - this handles the case
    # where all columns are blank but we still want to include the index
    return result


def diff_dict(idx, vals, target_df):
    if not isinstance(idx, Iterable) or isinstance(idx, str):
        idx = (idx,)
    true_indexes = vals[vals].index
    if len(true_indexes) == 0:
        return None
    return (
        dict(zip(target_df.index.names, idx))
        | target_df.loc[idx, true_indexes].to_dict()
    )
