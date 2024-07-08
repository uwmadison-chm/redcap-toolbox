#!/usr/bin/env python

"""
A function to return a dict with the minimal changes to transform
one dataframe to another (identically-shaped) dataframe.
"""


from collections.abc import Iterable


def transformation_dicts(source_df, target_df):
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
    if not source_df.index.equals(target_df.index):
        raise ValueError("Source and target dfs have different indexes")
    if not source_df.columns.equals(target_df.columns):
        raise ValueError("Source and target dfs have different columns")
    diff = source_df.ne(target_df)
    return [
        d for (idx, vals) in diff.iterrows() if (d := diff_dict(idx, vals, target_df))
    ]


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
