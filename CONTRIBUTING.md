# Contributing to redcap-toolbox

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and [just](https://just.systems/) as a task runner. It expects you to have a virtual environment in the `.venv` directory.

Install dependencies:

```console
$ uv venv
$ uv sync --group dev
```

## Common tasks

All common tasks are defined in the `justfile` and run via `just`:

| Command        | What it does                                      |
|----------------|---------------------------------------------------|
| `just`         | Run type checking, linting, formatting, and tests |
| `just test`    | Run the test suite with pytest                    |
| `just format`  | Auto-format code with ruff                        |
| `just lint`    | Lint code with ruff                               |
| `just ty`      | Run type checking with ty                         |
| `just publish` | Tag and publish a release (maintainers only)      |

## Code style

- CLI tools use [docopt-ng](https://github.com/jazzband/docopt-ng/) for argument parsing.
- Format code with `ruff format src/ tests/` before committing.
- Type check with `ty check`.

## Build scripts

The `build-scripts/` directory contains tooling used by `just publish`:

### `tag-release.py`

reads the version from `pyproject.toml`, creates an annotated git tag matching that version, and pushes it to the remote. If the tag already exists, pass `-f` to force-update it.

## Publishing a release

Publishing is handled by `just publish`, which:

1. Runs type checking, linting, and tests.
2. Verifies the git working tree is clean.
3. Runs `build-scripts/tag-release.py` to create and push the version tag.

Pushing a tag matching the pattern `YYYY.MM.DD*` triggers the GitHub Actions workflow at
`.github/workflows/publish.yml`, which:

- Builds the package with `uv build`.
- Publishes it to [PyPI](https://pypi.org/p/redcap_toolbox) via trusted publishing.
- Creates a GitHub release with the built artifacts attached.

To publish:

1. Update the `version` field in `pyproject.toml`.
2. Commit the change and make sure the working tree is clean.
3. Run `just publish`.
