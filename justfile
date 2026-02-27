default: ty lint format test

test:
    .venv/bin/pytest 2>&1

format:
    .venv/bin/ruff format 2>&1

lint:
    .venv/bin/ruff check 2>&1

ty:
    .venv/bin/ty check 2>&1

ensure-git-clean:
    #!/usr/bin/env bash
    set -euo pipefail
    status=$(git status --porcelain)
    if [ -n "$status" ]; then
        echo "Git repo is dirty:"
        echo "$status"
        exit 1
    fi

publish: ty lint test ensure-git-clean
    .venv/bin/python build-scripts/tag-release.py

