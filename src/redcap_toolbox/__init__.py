# Copyright 2023 Board of Regents of the University of Wisconsin System
# Written by Nate Vack <njvack@wisc.edu>, Nicholas Vanhaute <nvanhaute@wisc.edu>, and
# Stuti Shrivastava <sshrivastav6@wisc.edu>
# Source is available at https://github.com/uwmadison-chm/redcap-tools

from importlib.metadata import metadata as _metadata

_meta = _metadata("redcap_toolbox")
__version__ = _meta["Version"]
__author__ = _meta["Author-email"]
__description__ = _meta["Summary"]
_project_urls = {
    label: url
    for entry in (_meta.get_all("Project-URL") or [])
    for label, url in [entry.split(", ", 1)]
}
__url__ = _project_urls.get("Homepage", "")
