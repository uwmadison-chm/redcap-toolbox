# patch_redcap_csv example

Sample data showing how `patch_redcap_csv` applies sparse patch CSVs to a base
REDCap export. Each patch carries all of the base's key columns (here
`record_id`, `redcap_event_name`, `redcap_repeat_instrument`,
`redcap_repeat_instance`) plus the field(s) it changes; the tool materializes a
full CSV you can feed to `update_redcap_diff`. Leave the repeat columns blank to
target the non-repeating row, or fill them in to target a specific instance (see
`compliance.csv`).

```console
# Apply both patches (last write wins) and print the materialized CSV
$ patch_redcap_csv base.csv wearables.csv compliance.csv

# Materialize, then push the minimal change set to REDCap
$ patch_redcap_csv base.csv wearables.csv compliance.csv -o updated.csv
$ update_redcap_diff --dry-run base.csv updated.csv
```

Patches that reference a key not in `base.csv` error by default (pass
`--allow-new` to add rows); patch columns not in `base.csv` error by default
(see `--extra-cols`); and two patches that set the same cell to different values
warn by default (see `--cell-conflicts`).
