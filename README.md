# redcap-toolbox

Python package of REDCap tools

## Background

The goal of `redcap-toolbox` is to provide researchers with the tools to download and upload data to existing REDCap
builds. One key aspect of this toolbox is that the minimum number of changes needed to the records are calculated prior
to importing data in to REDCap, thereby reducing the API load.

## Installing REDCap Toolbox and Supported Versions

redcap-toolbox is available on PyPI:

```console
$ pip install redcap-toolbox
```

redcap-toolbox officially supports Python 3.10+.

## Getting started

`redcap-toolbox` relies on the environmental variables `REDCAP_API_URL` and `REDCAP_API_TOKEN`.

### Downloading data

* Download REDCap dataset using `download_redcap` and provide an output file name. By default, all forms are exported.
* If you want to include survey timestamps, add the optional flag: `--survey-fields`
* To download specific instrument forms, enter form names to export in a text file, one per line. If in the web UI, the
  form name has a space in it, replace the space with an underscore. Then, provide that file using the optional flag:
  `--forms get_forms.csv`

An example call might look like this:

`download_redcap --survey-fields --forms get_forms.csv source_data/full_data.csv`

which will download the data set with only the forms defined in the `get_forms.csv` with timestamps

### Downloading data incrementally

* When you want to keep a local copy up to date without re-downloading the full dataset each time, you should use `download_redcap_incremental`. Exporting the entirety of large datasets (we have one that is upwards of 150M) causes substantial load on the server -- not great when the vast majority of that data is rows that haven't changed since your last export.

* On the first run it downloads everything; on subsequent runs it fetches only records changed since the last run and merges them in.

* Incremental state is stored in a `.incremental/` directory alongside the output file:
  ```
  .incremental/base.csv        - Accumulated full dataset
  .incremental/last_download  - Timestamp of last successful download
  ```

* To force a full re-download, delete the `.incremental/` directory.

An example call might look like this:

`download_redcap_incremental --overlap=1h source_data/full_data.csv`

Options:
* `--overlap=<duration>` — overlap window for missed-change protection (default: `24h`).
  Accepts `60s`, `5m`, `24h`, `3d`, or a bare number of seconds.
* `--tz=<tz>` — timezone for timestamps, e.g. `America/Chicago` (default: local time)
* `-v / --verbose` — print progress messages


### Obtaining REDCap data event files

* Use `download_redcap_events` to get the event mapping and the instrument-event mapping files.

* So, if your data has events 'scr', 'pre', and 'post', and 'pre' and 'post'
  each have a repeated instrument called 'meds', you can expect the output files to look like this:
  ``` 
  File: StudyName_event_map.csv
  event_name | arm_num | unique_event_name | custom_event_label | event_id
  scr        | 1       | scr_arm_1         |                    | 12345
  pre        | 1       | pre_arm_1         |                    | 12346
  post       | 1       | post_arm_1        |                    | 12347
  ```

  ``` 
  File: StudyName_repeating_event_map.csv
  event_name | form_name | custom_form_label
  pre_arm_1  | meds      | 
  post_arm_1 | meds      |
  ```
An example call might look like this:

`download_redcap_events --prefix StudyName source_data_config`

where the event and mapping files will be saved in the `source_data_config` directory with the prefix `StudyName` added. Example:
```
source_data_config
├── StudyName__event_map.csv
└── StudyName__repeating_event_map.csv
```

### Splitting REDCap data into event files

* Use `split_redcap_data` to split the data downloaded using `download_redcap`:
    * A file for each event
    * A file for repeated instruments in events where they happen

An example call might look like this:

`split_redcap_data --event-map=source_data_config/StudyName__event_map.csv --prefix StudyName --no-condense source_data/full_data.csv source_data`

where the split event files will be saved in the `source_data` directory as well with the prefix `StudyName` added.

* So, if your data has events 'scr', 'pre', and 'post', and 'pre' and 'post'
  each have a repeated instrument called 'meds', you can expect the output files to like this:


```
source_data
├── full_data.csv
├── StudyName__scr.csv
├── StudyName__pre.csv
├── StudyName__pre__meds.csv
├── StudyName__post.csv
└── StudyName__post__meds.csv
```

### Update records in REDCap

* Update the REDCap database with the minimum changes needed to make the system in sync.
* Allow for background import process in REDCap and batch uploads for larger imports.

An example call might look like this:

`update_redcap_diff StudyName__scr.csv StudyName__scr_cache.csv`

where the `_cache.csv` file contains the changes made to the original data file.

Options:
* `--allow-new` allows adding new rows to REDCap.
* `--background` enables the use of background import mode 
* `--batch-size=<N>` sets the limits on the batch size to N rows.
* `--max-records N` this will cause an exit with error if update exceeds N rows; 0 disables limit. The default is set to `1000`.
* `--strict-cols` requires updated CSV columns to exactly match base CSV columns.
* `-v / --verbose` — print progress messages


### Building the updated file from patch CSVs

* `patch_redcap_csv` builds the updated/cache file for `update_redcap_diff` by applying one or more sparse "patch" CSVs to a base export. Each patch only needs the key columns plus the field(s) it changes, so the scripts that generate updates can stay simple — they declare *what* to change and let this tool find the right rows.

* Every patch must carry all of the base's key columns (`record_id`, plus whichever of `redcap_event_name`, `redcap_repeat_instrument`, `redcap_repeat_instance` are present in the base) so each row uniquely identifies the data it touches. For non-repeating data, leave the repeat columns blank. For a column that is present in a patch: a value sets the cell, a blank clears it (matching `update_redcap_diff`), and a column absent from the patch is left untouched.

* Patches are applied left-to-right, last write wins. The result is written to stdout, or to `--output`.

An example call might look like this:

`patch_redcap_csv base.csv wearables.csv compliance.csv -o updated.csv`

Options:
* `--allow-new` — permit patches to introduce keys not present in the base (otherwise an unknown key is an error).
* `--extra-cols=<mode>` — how to handle patch columns absent from the base: `error` (default), `warn`, or `allow`.
* `--cell-conflicts=<mode>` — how to handle a cell written by more than one patch with differing values: `error`, `warn` (default), or `allow`.

Then, feed the result to `update_redcap_diff`:

`update_redcap_diff --dry-run base.csv updated.csv`

See `examples/patch_redcap_csv/` for runnable sample data.


### Downloading reports

* Reports can be downloaded using `download_redcap_report` with either a list of report IDs separated by commas or a
  file with list of report IDs, one per line.
* Use the `--prefix` flag, to specify the prefix to be added for the filenames. Default is `redcap`.

An example call might look like this:

`download_redcap_report --file report_ids.csv --prefix StudyName report_data`

or

`download_redcap_report --id 32001,32004 --prefix StudyName report_data`

which will save all the reports for IDs listed in the `report_ids.csv` file in the `report_data` directory.

* The output report filenames will look like this:
  ```
  report_data
  ├── StudyName__report_32001.csv
  └── StudyName__report_32004.csv
  ```

## Credits

`redcap-toolbox` was written by Nate Vack <njvack@wisc.edu>, with features added by Nicholas
Vanhaute <nvanhaute@wisc.edu> and Stuti Shrivastava <sshrivastav6@wisc.edu>.
`redcap-toolbox` is copyright 2023 by the Boards of Regents of the University of Wisconsin System.
