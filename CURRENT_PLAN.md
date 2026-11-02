# Plan: split diff/import, add `redcap` umbrella CLI, reorganize package

## Context

`redcap_toolbox` is a small set of CLI tools for syncing data with REDCap. Two
problems motivate this change:

1. **`update_redcap_diff` does too much.** It computes the minimal changeset
   *and* pushes it to REDCap in one step (`src/redcap_toolbox/update_redcap_diff.py`
   lines 63–93 compute, 95–118 import). There is no inspectable artifact between
   "what will change" and "change it," and the import-mechanics options
   (`--batch-size`, `--background`, `--max-records`) are tangled with the
   diff-computation options (`--allow-new`, `--strict-cols`).

2. **Inconsistent CLI surface and code organization.** Six flat console scripts
   each re-implement env handling three different ways (global `PROJ` +
   `sys.exit` in `download_redcap.py`; global + exit-codes in
   `update_redcap_diff.py`; none in the pure-transform tools). CLI parsing and
   library logic live in the same modules.

**Intended outcome:** a `redcap <subcommand>` umbrella CLI; the diff step writes
a JSON *changeset* artifact; two thin importers (`import_json` for changesets,
`import_csv` mirroring the REDCap Data Import Tool) apply data; and a clean
split between pure library logic and thin CLI entrypoints. Breaking changes are
acceptable (we are the primary users and pin to a version).

## Key design decisions (confirmed with user)

- **Changeset format is JSON**, not CSV — the changeset is sparse and ragged
  (each record carries only key cols + changed cells; a cleared cell is an empty
  string), which CSV cannot represent without losing the absent-vs-blank
  distinction. `minchange.transformation_dicts` already returns exactly this
  shape (list of record dicts).
- **Import is split into two sibling commands** with deliberately different
  blank-cell defaults:
  - `redcap import_json <changeset.json>` — applies a changeset; defaults to
    **overwrite** (`overwriteBehavior="overwrite"`) so cleared cells actually
    clear. This is required for the `diff → import_json` round-trip to sync.
  - `redcap import_csv <file.csv>` — mirrors the REDCap **Data Import Tool**,
    whose UI default is "Ignore blank values"; defaults to **ignore**
    (`overwriteBehavior="normal"`). Passes the CSV straight to the same import
    endpoint the Data Import Tool uses.
  - Both expose `--blank-values=ignore|overwrite` to switch (ignore→`normal`,
    overwrite→`overwrite`). The opposite defaults are intentional and will be
    documented at each command's `--help`.
- **Naming:** snake_case subcommands matching existing convention:
  `download`, `download_incremental`, `download_report`, `split`, `patch`,
  `diff`, `import_json`, `import_csv` — all under one `redcap` command. The
  redundant `_redcap` infix is dropped under the umbrella.

## Target package layout

```
src/redcap_toolbox/
  __init__.py
  csv_utils.py          # unchanged (pure: read_csv, key_cols_for)
  minchange.py          # unchanged (pure: transformation_dicts)
  changeset.py          # NEW: changeset read/write + schema
  redcap_api.py         # NEW: in-repo requests-based RedcapClient (replaces PyCap)
  ops/                  # pure operations, NO docopt / sys.exit / global state
    __init__.py
    download.py
    download_incremental.py
    download_report.py
    split.py
    patch.py            # apply_patches & helpers (moved from patch_redcap_csv.py)
    diff.py             # compute changeset from base/updated (top half of update_redcap_diff)
    importer.py         # import_json_records / import_csv_text wrappers + batching
  cli/                  # thin docopt entrypoints + dispatcher
    __init__.py         # `redcap` dispatcher: main()
    download.py
    download_incremental.py
    download_report.py
    split.py
    patch.py
    diff.py
    import_json.py
    import_csv.py
```

Pattern for every `cli/<cmd>.py`: a docopt usage string + a `run(argv)` that
parses, calls the matching `ops` function, and returns an int exit code. No
business logic. The pure logic already exists in most modules (`apply_patches`,
`transformation_dicts`, `download_redcap`, the split logic) — the work is moving
it under `ops/`, stripping the `main()`/global-`PROJ`/`sys.exit` parts, and
routing those through the shared helpers below.

## Changeset format (`changeset.py`)

Self-describing wrapper object (version + key columns + records):

```json
{
  "version": 1,
  "key_columns": ["record_id", "redcap_event_name"],
  "records": [
    {"record_id": "2", "redcap_event_name": "pre_arm_1", "field1": "g"}
  ]
}
```

- `records` is exactly `minchange.transformation_dicts(...)` output.
- `write_changeset(path|stdout, key_cols, records)` and
  `read_changeset(path|stdin) -> (key_cols, records)`.
- `import_json` tolerates a bare top-level JSON array too (treat as `records`),
  for ad-hoc use, but `redcap diff` always writes the wrapped form.

## In-repo REDCap client (`redcap_api.py`) — replaces PyCap

We use exactly three REDCap endpoints today (record export, record import, report
export) and will likely add `export_metadata` and Export Instrument-Event
Mappings. The REDCap API is a single URL, `POST` form-encoded with `token`,
`content`, `format`, `returnFormat`, plus content-specific params — so PyCap (a
maintained zip-URL **fork**, `uwmadison-chm/PyCap` `2.7.1-chm`) buys us little
for the surface we use. **Drop the PyCap dependency** and write a ~70-line client.

`RedcapClient(url, token)` with:
- `from_env() -> RedcapClient` reading `REDCAP_API_URL` / `REDCAP_API_TOKEN`,
  raising a clear error if unset. Replaces the three divergent env patterns; the
  importer commands tolerate missing env under `--dry-run` (preserving current
  `update_redcap_diff.py:150–157` behavior).
- `_post(content, **params) -> str`: injects `token`/`returnFormat`, encodes
  array params (`forms[0]=...`), `POST`s via `requests`, `raise_for_status`,
  returns text. One place for all REDCap quirks.
- Thin typed methods: `export_records(format="csv", forms=None,
  export_survey_fields=False, date_begin=None)` (formats `dateRangeBegin` as
  `%Y-%m-%d %H:%M:%S`, per the fork's records.py:234), `import_records(data,
  import_format="json", overwrite="normal", background_process=False)`,
  `export_report(report_id, format="csv")`. Future `export_metadata`
  (`content="metadata"`) and `export_form_event_mapping`
  (`content="formEventMapping"`) are ~3 lines each.

Dependency change in `pyproject.toml`: remove `pycap` and the
`[tool.uv.sources]` zip entry; add `requests`. (`urllib3` stays.)

## Command specs

- **`redcap diff <base_csv> <updated_csv>`** → writes changeset.
  Options: `-o/--output FILE` (default stdout), `--allow-new`, `--strict-cols`,
  `-v`. Pure: reads CSVs (`read_csv`), derives `key_cols` (`key_cols_for`),
  column-aligns, calls `transformation_dicts`, writes via `changeset.py`. No API
  access, no `--max-records` (that guard moves to the import side, where the
  dangerous action happens).

- **`redcap import_json <changeset.json>`** (`-` = stdin) → applies a changeset.
  Options: `--blank-values=ignore|overwrite` **[default: overwrite]**,
  `--batch-size N`, `--background`, `--max-records N [default: 1000]`,
  `--dry-run`, `-v`. Loads records via `changeset.py`, batches (reuse current
  batching logic from `update_redcap_diff.py:95–99`), calls
  `RedcapClient.import_records(batch, overwrite=..., background_process=...)`.

- **`redcap import_csv <file.csv>`** (`-` = stdin) → Data Import Tool semantics.
  Options: `--blank-values=ignore|overwrite` **[default: ignore]**,
  `--background`, `--max-records N [default: 1000]`, `--dry-run`, `-v`. Reads CSV
  text and passes through with `import_records(csv_text, import_format="csv",
  overwrite=...)` — faithful to the server's own tool. `--max-records` guards by
  row count.

- **`redcap download` / `download_incremental` / `download_report` / `split` /
  `patch`** — behavior unchanged; just relocated to `ops/` + `cli/` and switched
  to `RedcapClient.from_env()` where they touch the API.

## Dispatcher (`cli/__init__.py`)

`main()` uses docopt with `options_first=True` to read the subcommand, then
delegates to a `{name: run}` table, rewriting `argv` so each subcommand's docopt
sees `redcap <cmd> ...`. `redcap --help` lists subcommands; `redcap --version`
reports the package version. `pyproject.toml [project.scripts]` becomes a single
entry: `redcap = "redcap_toolbox.cli:main"` (the six flat scripts are removed —
breaking, accepted).

## Tests (red-green; `just test`)

- **Update import paths** in existing tests to the new `ops.*` locations
  (`tests/test_patch_redcap_csv.py` → `ops.patch`, the update/diff test → split
  across `ops.diff` and `ops.importer`). Keep `minchange`/`split`/download tests
  pointed at their moved modules.
- **New:** `changeset.py` round-trip (write→read identity; bare-array
  tolerance); `ops.diff` produces the expected changeset from base/updated
  fixtures (reuse `tests/dataframe_factory.py`); `ops.importer` with a mocked
  `RedcapClient.import_records` asserting `overwrite` value per `--blank-values`,
  batching, `--max-records` raises, and `--dry-run` makes no API call;
  dispatcher routes a sample argv to the right `run`.
- Rename test files to match new module names (project already prefers no
  `_pytest` suffix per recent commit history).

## README

Rewrite the command examples under one `redcap ...` surface and document the new
pipeline: `download → patch → diff (→ changeset.json) → import_json`, plus
`import_csv` as the ad-hoc "push this CSV like the Data Import Tool" path,
calling out the opposite `--blank-values` defaults.

## Verification

1. `just` (runs ty, ruff lint, ruff format, pytest) — all green.
2. Manual, no API needed:
   - `redcap diff base.csv updated.csv -o cs.json` then inspect `cs.json` shape.
   - `redcap import_json --dry-run cs.json` and `redcap import_csv --dry-run
     data.csv` — confirm counts/first-record output and zero API calls.
   - `redcap --help` lists all subcommands; `redcap diff --help` shows its usage.
3. With real `REDCAP_API_URL`/`REDCAP_API_TOKEN` against a test project: a small
   `import_json` that includes a cleared cell (confirm it clears with default
   overwrite) and an `import_csv` with a stray blank (confirm it is ignored with
   default), then flip `--blank-values` on each to confirm the switch.

## Decoupled follow-up: project rename

This may mark a fork/rename of the project (`redcap-toolbox` is a weak name).
The command stays `redcap` regardless, so this is purely a distribution/repo
identity choice and **does not gate the refactor** (git-pinned internal tool).

Name status (preference order), all pending external outcomes:
- **`redcap-cli`** — best fit with the `redcap` command. PyPI name held by an
  *archived* package from PyCap's author; user will email to ask for a transfer.
- **`redcap-tools`** — honest, plain. Also already taken; user will email the
  project admin.
- **`redcap-sync`** — fallback that is likely available and advertises the
  marquee minimal-diff feature; use if neither transfer lands.

Implementation can proceed under the current name and adopt the chosen name in
the mechanical rename pass once an email resolves.

Decision: **do the rename as a separate, near-mechanical pass after** the
functional work, so the refactor diff stays reviewable as a behavior change. The
rename touches `pyproject.toml` `name`, the package directory, all intra-package
imports, the repo URL, and the README. The console command stays `redcap`
regardless, so there is no CLI/muscle-memory cost. (If we'd rather not move the
package dir twice, the one alternative is to create it as `redcap_cli/` at the
very start of the functional work — otherwise this is strictly a follow-up.)

## Decoupled follow-up: vendor docopt-ng

`docopt-ng` is unmaintained. Its only inputs are our own usage strings and
`argv` — no untrusted/network input — so the runtime attack surface is nil; the
concern is purely a frozen-but-external install-time dependency that could be
yanked or tampered with. `uv.lock` hash-pinning already blocks tampered
re-uploads but not a yank/disappearance, and we never need the package to evolve.

It is an ideal vendoring candidate (pure Python, zero deps, small, MIT). Plan:
- Drop it into `src/redcap_toolbox/_vendor/docopt.py` with the upstream
  version/commit + MIT notice in the header for provenance.
- Add per-file ruff/ty ignores for `_vendor/` so vendored code is never linted or
  reformatted.
- Remove `docopt-ng` from `[project.dependencies]`; import as
  `from redcap_toolbox._vendor import docopt`.

Do this in **phase 2** (the `cli/` reorg), when every docopt call site is being
rewritten anyway, so the import-path change is free.

## Suggested sequencing (optional, can be one PR)

1. **Changeset + split:** add `changeset.py`, `redcap_api.py`, `ops/diff.py`,
   `ops/importer.py`; introduce `diff` / `import_json` / `import_csv`. Delivers
   the core value.
2. **Umbrella + reorg:** move remaining tools into `ops/` + `cli/`, add the
   dispatcher, collapse `[project.scripts]` to `redcap`, vendor `docopt-ng`,
   update README/tests.
