#!/usr/bin/env python

"""
Apply one or more sparse "patch" CSVs to a base REDCap export, producing a new
CSV that can be fed to update_redcap_diff.

Each patch CSV must contain all of the base's key columns (record_id plus any of
redcap_event_name / redcap_repeat_instrument / redcap_repeat_instance present in
the base) so that every row uniquely identifies the data it changes. Any other
column present in a patch updates that cell:

    * column absent from the patch  -> cell left untouched
    * column present but blank       -> cell cleared (same as update_redcap_diff)
    * column present with a value     -> cell set

Patches are applied left-to-right; the last write to a cell wins.

Usage: patch_redcap_csv.py [options] <base_csv> <patch_csv>...

Options:
    -o --output FILE       Write the result here (default: stdout).
    --allow-new            Permit patches to introduce keys (rows) not present in base.
    --extra-cols MODE      How to handle patch columns absent from the base:
                           error, warn, or allow. [default: error]
    --cell-conflicts MODE  How to handle a cell written by more than one patch
                           with differing values: error, warn, or allow. [default: warn]
    -h --help              Show this screen.
    -v --verbose           Show debug logging.
"""

import logging
import sys
from dataclasses import dataclass, field
from enum import Enum

import docopt
import polars as pl

from redcap_toolbox.csv_utils import key_cols_for, read_csv

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EXIT_OK = 0
EXIT_ERROR = 1


class Mode(Enum):
    """How to handle a flagged condition: hard error, warn, or silently allow."""

    ERROR = "error"
    WARN = "warn"
    ALLOW = "allow"

    @classmethod
    def from_str(cls, value: str) -> "Mode":
        """Parse a Mode from a string, case-insensitively."""
        try:
            return cls(value.strip().lower())
        except ValueError:
            valid = ", ".join(m.value for m in cls)
            raise ValueError(f"Invalid mode {value!r}; choose from: {valid}")


@dataclass
class PatchSet:
    """The folded-together result of all patches, before materializing onto the base.

    cell_values maps each string-space key tuple to the final {column: value}
    for the cells patches touched. new_keys lists keys absent from the base in
    first-seen order (only populated with allow_new). extra_cols lists patch
    columns absent from the base that should be kept, in first-seen order.
    """

    cell_values: dict[tuple, dict[str, str]] = field(default_factory=dict)
    new_keys: list[tuple] = field(default_factory=list)
    extra_cols: list[str] = field(default_factory=list)


def _require_key_cols(df: pl.DataFrame, key_cols: list[str], label: str) -> None:
    """Raise ValueError if df is missing any of key_cols."""
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing key columns: {sorted(missing)}")


def _string_key_set(df: pl.DataFrame, key_cols: list[str]) -> set[tuple]:
    """Return df's key tuples cast to strings.

    Comparing keys in string space lets, e.g., an integer record_id in the base
    match a string record_id read from a patch CSV.
    """
    keys = df.select([pl.col(c).cast(pl.Utf8) for c in key_cols])
    return {tuple(row) for row in keys.iter_rows()}


def _resolve_cell_conflict(
    where: dict, col: str, old: str, new: str, mode: Mode
) -> None:
    """Handle a cell that a later patch sets to a value differing from an earlier one."""
    detail = f"for {where} column {col!r}: {old!r}"
    if mode is Mode.ERROR:
        raise ValueError(f"Conflicting values {detail} vs {new!r}")
    if mode is Mode.WARN:
        logger.warning(f"Conflicting values {detail} -> {new!r} (using latter)")


def _check_extra_cols(idx: int, extras: list[str], mode: Mode) -> None:
    """Handle patch columns absent from the base per mode (caller keeps them)."""
    if not extras:
        return
    detail = f"Patch {idx} has columns not in base CSV: {sorted(extras)}"
    if mode is Mode.ERROR:
        raise ValueError(detail)
    if mode is Mode.WARN:
        logger.warning(f"{detail} (including them)")


def collect_patches(
    base_keys: set[tuple],
    base_cols: list[str],
    patches: list[pl.DataFrame],
    key_cols: list[str],
    *,
    allow_new: bool,
    extra_cols: Mode,
    cell_conflicts: Mode,
) -> PatchSet:
    """Fold patches (applied left-to-right, last write wins) into a PatchSet.

    Validates key columns, classifies extra columns, detects cell conflicts, and
    flags unknown keys -- all without touching the base dataframe itself.
    """
    result = PatchSet()
    new_key_set: set[tuple] = set()

    for idx, patch in enumerate(patches):
        _require_key_cols(patch, key_cols, f"Patch {idx}")
        value_cols = [c for c in patch.columns if c not in key_cols]
        extras = [
            c for c in value_cols if c not in base_cols and c not in result.extra_cols
        ]
        _check_extra_cols(idx, extras, extra_cols)
        result.extra_cols.extend(extras)

        patch_s = patch.select([pl.col(c).cast(pl.Utf8) for c in patch.columns])
        for row in patch_s.iter_rows(named=True):
            kt = tuple(row[c] for c in key_cols)
            if kt not in base_keys and kt not in new_key_set:
                if not allow_new:
                    raise ValueError(
                        f"Patch {idx} references unknown key {dict(zip(key_cols, kt))}; "
                        "pass allow_new to add new rows"
                    )
                result.new_keys.append(kt)
                new_key_set.add(kt)
            for col in value_cols:
                _record_cell(result, key_cols, kt, col, row[col], cell_conflicts)

    return result


def _record_cell(
    patch_set: PatchSet, key_cols: list[str], kt: tuple, col: str, val: str, mode: Mode
) -> None:
    """Record one cell value, resolving any conflict with a prior write."""
    prior = patch_set.cell_values.setdefault(kt, {})
    if col in prior and prior[col] != val:
        _resolve_cell_conflict(dict(zip(key_cols, kt)), col, prior[col], val, mode)
    prior[col] = val


def _apply_cell_updates(
    work: pl.DataFrame, key_cols: list[str], rows: list[dict]
) -> pl.DataFrame:
    """Apply sparse updates to existing rows via a single keyed join + coalesce.

    rows is one dict per updated key (key columns plus only the changed columns);
    a column left out of a row keeps its existing value.
    """
    if not rows:
        return work
    map_df = pl.DataFrame(rows)
    map_df = map_df.select([pl.col(c).cast(pl.Utf8) for c in map_df.columns])
    update_cols = [c for c in map_df.columns if c not in key_cols]
    work = work.join(map_df, on=key_cols, how="left", suffix="__patch__")
    work = work.with_columns(
        [
            pl.when(pl.col(f"{c}__patch__").is_not_null())
            .then(pl.col(f"{c}__patch__"))
            .otherwise(pl.col(c))
            .alias(c)
            for c in update_cols
        ]
    )
    return work.drop([f"{c}__patch__" for c in update_cols])


def _new_rows_frame(
    out_cols: list[str], key_cols: list[str], patch_set: PatchSet
) -> pl.DataFrame | None:
    """Build a string frame of the brand-new rows (only reachable with allow_new)."""
    if not patch_set.new_keys:
        return None
    rows = []
    for kt in patch_set.new_keys:
        row = {c: "" for c in out_cols}
        row.update(dict(zip(key_cols, kt)))
        row.update(patch_set.cell_values.get(kt, {}))
        rows.append(row)
    return pl.DataFrame(rows).select([pl.col(c).cast(pl.Utf8) for c in out_cols])


def _materialize(
    base: pl.DataFrame,
    key_cols: list[str],
    base_keys: set[tuple],
    patch_set: PatchSet,
) -> pl.DataFrame:
    """Render patch_set onto base, returning a string-valued, key-sorted frame."""
    base_cols = list(base.columns)
    out_cols = base_cols + patch_set.extra_cols
    work = base.select([pl.col(c).cast(pl.Utf8) for c in base_cols])
    if patch_set.extra_cols:
        work = work.with_columns([pl.lit("").alias(c) for c in patch_set.extra_cols])

    existing_rows = [
        {**dict(zip(key_cols, kt)), **cols}
        for kt, cols in patch_set.cell_values.items()
        if kt in base_keys
    ]
    work = _apply_cell_updates(work, key_cols, existing_rows).select(out_cols)

    new_df = _new_rows_frame(out_cols, key_cols, patch_set)
    if new_df is not None:
        work = pl.concat([work, new_df], how="vertical")

    work = work.with_columns([pl.col(c).cast(base.schema[c]) for c in key_cols])
    return work.sort(key_cols)


def apply_patches(
    base: pl.DataFrame,
    patches: list[pl.DataFrame],
    key_cols: list[str],
    *,
    allow_new: bool = False,
    extra_cols: Mode = Mode.ERROR,
    cell_conflicts: Mode = Mode.WARN,
) -> pl.DataFrame:
    """Apply sparse patch dataframes to base and return the materialized result.

    The returned dataframe has the base's columns (in base order), followed by any
    extra patch columns that were kept (when extra_cols is not ERROR), and is
    sorted by key_cols. Key columns keep their original dtypes; all other
    columns are returned as strings.

    Raises ValueError for missing key columns, unknown keys (without allow_new),
    disallowed extra columns, or conflicting cells (with cell_conflicts=ERROR).
    """
    _require_key_cols(base, key_cols, "Base CSV")
    base_keys = _string_key_set(base, key_cols)
    patch_set = collect_patches(
        base_keys,
        list(base.columns),
        patches,
        key_cols,
        allow_new=allow_new,
        extra_cols=extra_cols,
        cell_conflicts=cell_conflicts,
    )
    return _materialize(base, key_cols, base_keys, patch_set)


def main() -> int:
    args = docopt.docopt(__doc__ or "")
    if args["--verbose"]:
        logger.setLevel(logging.DEBUG)
    logger.debug(args)

    try:
        extra_cols = Mode.from_str(args["--extra-cols"])
        cell_conflicts = Mode.from_str(args["--cell-conflicts"])
    except ValueError as e:
        logger.error(str(e))
        return EXIT_ERROR

    base = read_csv(args["<base_csv>"])
    key_cols = key_cols_for(base)
    logger.debug(f"Using key columns: {key_cols}")
    patches = [read_csv(p) for p in args["<patch_csv>"]]

    try:
        result = apply_patches(
            base,
            patches,
            key_cols,
            allow_new=args["--allow-new"],
            extra_cols=extra_cols,
            cell_conflicts=cell_conflicts,
        )
    except ValueError as e:
        logger.error(str(e))
        return EXIT_ERROR

    output = args["--output"]
    if output:
        result.write_csv(output)
    else:
        sys.stdout.write(result.write_csv())
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
