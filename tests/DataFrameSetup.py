#!/usr/bin/env python

import pandas as pd


class DataFrameSetup:
    """Class to dynamically create DataFrames for testing."""

    def __init__(
        self,
        source_data=None,
        source_index=None,
        diff_data=None,
        wrong_index=None,
    ):
        self.source_data = source_data or {
            "record_id": [1, 2, 2],
            "redcap_event_name": ["scr_arm_1", "scr_arm_1", "pre_arm_1"],
            "redcap_repeat_instrument": ["", "", ""],
            "field1": ["a", "b", "c"],
            "field2": ["d", "e", "f"],
            "field3": [10, 20, 30],
        }
        self.source_index = source_index or ["record_id", "redcap_event_name"]

        self.diff_data = diff_data or {
            # Change at row 3 in field 1
            "field1": ["a", "b", "g"],
            # Change at row 2 in field 3
            "field3": [10, 40, 30],
        }

        self.wrong_index = wrong_index or ["record_id", "field2"]

        # Call methods to create dataframes
        self.source_df = self._create_df()
        self.diff_df = self._create_diff_df()
        self.wrong_index_df = self._create_wrong_index_df()
        self.extra_columns_df = self._add_extra_columns(self._copy_df(self.diff_df))

    # Methods for creating dataframes
    def _create_df(self):
        """Create the dataframe"""
        return pd.DataFrame(self.source_data).set_index(self.source_index)

    def _copy_df(self, target_df=None):
        """Make a copy of the input dataframe"""
        if target_df is None:
            return self.source_df.copy()
        else:
            return target_df.copy()

    def _create_diff_df(self):
        """Make the changes in the dataframe"""
        target_df = self.source_df.copy().reset_index()
        target_df.update(self.diff_data)
        target_df.set_index(self.source_index, inplace=True)
        return target_df

    def _create_wrong_index_df(self, target_df=None):
        """Make dataframe with wrong index"""
        if target_df is None:
            target_df = self.source_df.copy().reset_index()
        else:
            target_df = target_df.copy().reset_index()
        return target_df.set_index(self.wrong_index)

    def _add_extra_columns(self, target_df=None):
        """Add extra columns to dataframe"""
        if target_df is None:
            return self.source_df.copy().assign(field4=["x", "y", "z"])
        else:
            return target_df.copy().assign(field4=["x", "y", "z"])
