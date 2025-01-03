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

redcap-toolbox officially supports Python 3.8+.

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

### Splitting REDCap data into event files

* Use `split_redcap_data` to split the REDCap CSV file into:
    * A file for each event
    * A file for repeated instruments in events where they happen

* So, if your data has events 'scr', 'pre', and 'post', and 'pre' and 'post'
  each have a repeated instrument called 'meds', you can expect the output files to like this:
  ```
  ├── redcap__scr.csv
  ├── redcap__pre.csv
  ├── redcap__pre__meds.csv
  ├── redcap__post.csv
  ├── redcap__post.csv
  └── redcap__post__meds.csv
  ```

* In addition, if you don't like the whole _arm_1 appended to your event names
  (who does like that?) or you're using events to denote arms and want all your
  event's data together, you can use the event_map file for this. That file
  should be a CSV file and contain the columns 'redcap_event' and 'filename_event'

    * Example event maps might look like:
      ```
      scr__all_arm_1,scr
      pre__control_arm_1,pre
      pre__intervention_arm_1,pre
      ```

An example call might look like this:

`split_redcap_data --event-map=event_map.csv --prefix StudyName --no-condense source_data/full_data.csv source_data`

where the split event files will be saved in the `source_data` directory as well with the prefix `StudyName` added to
them.

```
source_data
├── full_data.csv
├── StudyName__scr.csv
├── StudyName__pre.csv
├── StudyName__pre_meds.csv
├── StudyName__post.csv
└── StudyName__post_meds.csv
```

### Update records in REDCap

* Update the REDCap database with the minimum changes needed to make the system in sync.
* It is important that the updated data file has the same number of rows and columns as the original data file.
* This functionality is especially useful when updating the record information for Tracking purposes.

An example call might look like this:

`update_redcap_diff StudyName__scr.csv StudyName__scr_cache.csv`

where the `_cache.csv` file contains the changes made to the original data file.

## Credits

`redcap-toolbox` was written by Nate Vack <njvack@wisc.edu>, with features added by Nicholas
Vanhaute <nvanhaute@wisc.edu> and Stuti Shrivastava <sshrivastav6@wisc.edu>.
`redcap-toolbox` is copyright 2023 by the Boards of Regents of the University of Wisconsin System.