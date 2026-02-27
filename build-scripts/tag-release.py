#!/usr/bin/env python

"""
Get the version from pyproject.toml, add a tag with it, push it.

Raises an error if the tag already exists.
"""

# tomllib was added in 3.11, we support 3.10, you'll release with something new
import tomllib  # ty: ignore
import subprocess
import sys


def main():
    with open("pyproject.toml", "rb") as f:
        version = str(tomllib.load(f)["project"]["version"])

    tag = version

    # Check if tag already exists
    result = subprocess.run(["git", "tag", "-l", tag], capture_output=True, text=True)
    if tag in result.stdout.strip().splitlines():
        print(f"Tag {tag} already exists. Use -f to force update.")
        if "-f" not in sys.argv:
            sys.exit(1)
        subprocess.run(["git", "tag", "-fa", tag, "-m", f"Release {tag}"], check=True)
    else:
        subprocess.run(["git", "tag", "-a", tag, "-m", f"Release {tag}"], check=True)

    subprocess.run(["git", "push", "origin", tag, "--force"], check=True)
    print(f"✓ Tagged and pushed {tag}")


if __name__ == "__main__":
    main()
