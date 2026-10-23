#!/usr/bin/env python

import importlib
from unittest.mock import MagicMock

import pytest
from requests import RequestException

import src.redcap_toolbox.download_redcap_events as download_redcap_events_mod


def test_proj_none_at_module_level_without_env_vars(monkeypatch):
    """Module can be reloaded without REDCAP env vars; PROJ stays None."""
    monkeypatch.delenv("REDCAP_API_URL", raising=False)
    monkeypatch.delenv("REDCAP_API_TOKEN", raising=False)
    importlib.reload(download_redcap_events_mod)
    assert download_redcap_events_mod.PROJ is None


@pytest.fixture
def mock_proj(monkeypatch):
    proj = MagicMock()
    proj.export_events.return_value = "event_name,arm_num\nscr,1\n"
    proj.export_repeating_instruments_events.return_value = (
        "event_name,form_name\npre_arm_1,meds\n"
    )
    monkeypatch.setattr(download_redcap_events_mod, "PROJ", proj, raising=False)
    return proj


def test_writes_both_files_with_prefix(mock_proj, tmp_path):
    download_redcap_events_mod.download_redcap_events(
        tmp_path, "StudyName", verbose=False
    )

    event_file = tmp_path / "StudyName__event_map.csv"
    rep_file = tmp_path / "StudyName__repeating_event_map.csv"
    assert event_file.read_text() == "event_name,arm_num\nscr,1\n"
    assert rep_file.read_text() == "event_name,form_name\npre_arm_1,meds\n"


def test_first_export_failure_exits_1_and_writes_nothing(mock_proj, tmp_path):
    mock_proj.export_events.side_effect = RequestException("boom")
    with pytest.raises(SystemExit) as exc:
        download_redcap_events_mod.download_redcap_events(
            tmp_path, "StudyName", verbose=False
        )
    assert exc.value.code == 1
    assert list(tmp_path.iterdir()) == []


def test_second_export_failure_exits_1_keeps_first_file(mock_proj, tmp_path):
    mock_proj.export_repeating_instruments_events.side_effect = RequestException("boom")
    with pytest.raises(SystemExit) as exc:
        download_redcap_events_mod.download_redcap_events(
            tmp_path, "StudyName", verbose=False
        )
    assert exc.value.code == 1
    assert (tmp_path / "StudyName__event_map.csv").exists()
    assert not (tmp_path / "StudyName__repeating_event_map.csv").exists()


def test_main_default_prefix_and_creates_out_dir(monkeypatch, mock_proj, tmp_path):
    out_dir = tmp_path / "cfg"  # does not exist yet
    monkeypatch.setenv("REDCAP_API_URL", "http://example.test/api/")
    monkeypatch.setenv("REDCAP_API_TOKEN", "tok")
    monkeypatch.setattr(
        download_redcap_events_mod.redcap, "Project", lambda url, tok: mock_proj
    )
    monkeypatch.setattr("sys.argv", ["download_redcap_events", str(out_dir)])

    download_redcap_events_mod.main()

    assert (out_dir / "redcap__event_map.csv").exists()
    assert (out_dir / "redcap__repeating_event_map.csv").exists()
