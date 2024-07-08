#!/usr/bin/env python
# Copyright 2022 Board of Regents of the University of Wisconsin System
# Written by Nate Vack <njvack@wisc.edu>
# Source is available at https://github.com/uwmadison-chm/redcap-tools

"""
Takes a REDCap CSV file and splits it into:

* A file for each event
* A file for repeated instruments in events where they happen

So, if your data has events 'scr,' 'pre,' and 'post', and 'pre' and 'post'
each have a repeated instument called 'meds', you can expect

redcap__scr.csv
redcap__pre.csv
redcap__pre__meds.csv
redcap__post.csv
redcap__post__meds.csv

In addition, if you don't like the whole _arm_1 appeneded to your event names
(who does like that?) or you're using events to denote arms and want all your
event's data together, you can use the event_map file for this. That file
should be a CSV file and contain the columns 'redcap_event' and 'filename_event'

Example event maps might look like:
scr__all_arm_1,scr
pre__control_arm_1,pre
pre__intervention_arm_1,pre

Usage:
  split_redcap_data.py [options] <input_file> <output_directory>

Options:
  --event-map=<event_file>  A file mapping redcap events to file events
  --prefix=<prefix>         A filename prefix for the output [default: redcap]
  --no-condense             Don't filter empty rows, columns and files
  -h --help                 Show this screen
  -d --debug                Print debugging output
"""

from collections import defaultdict
import logging
from pathlib import Path

import docopt
import pandas as pd


logging.basicConfig(format='%(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def make_event_map(mapping_file):
    if mapping_file is None:
        return {}
    map_df = pd.read_csv(mapping_file, index_col='redcap_event', dtype=str)
    event_map = map_df['filename_event'].to_dict()
    return event_map


def combine_names(event_name, rep_name):
    parts = [name for name in [event_name, rep_name] if name != '']
    joined = '__'.join(parts)
    logger.debug(f'Made name {joined} from {event_name} and {rep_name}')
    return joined


def split_data(data, event_map):
    """
    Returns a dict of nonprefixed_filename: data pairs
    """
    data_lists = defaultdict(list)
    index_col = data.columns[0]
    event_groups = data.groupby(by='redcap_event_name')
    for rc_event, event_data in event_groups:
        out_event_name = event_map.get(rc_event, rc_event)
        rep_groups = event_data.groupby(by='redcap_repeat_instrument')
        for rep_group, rep_data in rep_groups:
            out_name = combine_names(out_event_name, rep_group)
            data_lists[out_name].append(rep_data)
    # Now, we need to concat the dataframes and sort them by the index column
    dataframes = {
        name: pd.concat(df_list).sort_values(by=index_col)
        for name, df_list in data_lists.items()
    }
    for name, df in dataframes.items():
        logger.debug(f'Name: {name}, Shape: {df.shape})')
    return dataframes


def condense_df(df):
    df = df.replace('', float('nan'))
    index_col = df.columns[0]
    reserved_cols = set([
        index_col,
        'redcap_event_name',
        'redcap_repeat_instrument',
        'redcap_repeat_instance'
    ])
    rowdrop_cols = set(df.columns) - reserved_cols
    df.dropna(axis='index', how='all', subset=rowdrop_cols).dropna()
    col_condensed = df.dropna(axis='columns', how='all')
    return col_condensed

def split_redcap_data(
    input_file,
    output_directory,
    prefix='redcap',
    mapping_file=None,
    condense=True
):
    event_map = make_event_map(mapping_file)
    logger.debug(f'Event map: {event_map}')
    data = pd.read_csv(input_file, index_col=None, dtype=str, na_filter=False)
    # Make sure the event and repeating columns are present, so we can process
    # the data the same in all cases.
    if 'redcap_event_name' not in data.columns:
        logger.debug('Single event file, adding event column')
        data['redcap_event_name'] = ''
    if 'redcap_repeat_instrument' not in data.columns:
        data['redcap_repeat_instrument'] = ''
        data['redcap_repeat_instance'] = ''
        logger.debug('Non-repeating file, added repeat columns')
    named_dataframes = split_data(data, event_map)
    logger.debug(named_dataframes)
    for name, df in named_dataframes.items():
        if condense:
            df = condense_df(df)
        file_base = combine_names(prefix, name)
        filename = f'{file_base}.csv'
        out_path = Path(output_directory) / filename
        logger.info(f'Saving dataframe with shape {df.shape} to {out_path}')
        df.to_csv(out_path, index=False)


def main():
    args = docopt.docopt(__doc__)
    if args['--debug']:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)
    split_redcap_data(
        args['<input_file>'],
        args['<output_directory>'],
        args['--prefix'],
        args['--event-map'],
        not args['--no-condense']
    )

if __name__ == '__main__':
    main()