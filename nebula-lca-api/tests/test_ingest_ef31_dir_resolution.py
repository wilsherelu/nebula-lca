"""The configured EF3.1 dir must be the resolver's first choice, not a hint it may ignore.

A public build ships its characterization under ``data/ef31-characterization`` and points
``NEBULA_LCA_EF31_DIR`` at it; the repo checkout keeps an ``data/EF3.1`` default. If the resolver
preferred the checkout default, the bootstrap would gate elementary flows against a different flow
index than the one the solve characterized with.
"""
from __future__ import annotations

from pathlib import Path

from app import ingest
from app.config import settings


def _write_flow_index(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "flow_index.csv"
    target.write_text("flow_index;FlowUUID;FlowName\n1;00000000-0000-0000-0000-000000000000;x\n",
                      encoding="utf-8")
    return target


def test_configured_ef31_dir_wins_over_the_checkout_default(monkeypatch, tmp_path) -> None:
    configured = _write_flow_index(tmp_path / "configured")
    monkeypatch.setattr(settings, "nebula_lca_ef31_dir", str(tmp_path / "configured"))
    assert ingest._resolve_default_ef31_flow_index_path() == configured


def test_resolver_falls_back_when_the_configured_dir_has_no_flow_index(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(settings, "nebula_lca_ef31_dir", str(tmp_path / "empty"))
    resolved = ingest._resolve_default_ef31_flow_index_path()
    assert resolved is None or resolved.name == "flow_index.csv"
    assert resolved is None or "empty" not in resolved.parts
