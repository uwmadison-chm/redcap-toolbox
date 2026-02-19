#!/usr/bin/env python

import importlib

import src.redcap_toolbox.download_redcap_report as download_redcap_report_mod


def test_proj_none_at_module_level_without_env_vars(monkeypatch):
    """Module can be reloaded without REDCAP env vars; PROJ stays None."""
    monkeypatch.delenv("REDCAP_API_URL", raising=False)
    monkeypatch.delenv("REDCAP_API_TOKEN", raising=False)
    importlib.reload(download_redcap_report_mod)
    assert download_redcap_report_mod.PROJ is None
