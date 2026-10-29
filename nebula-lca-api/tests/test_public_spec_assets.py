"""The public desktop payload may only carry license-verifiable characterization assets."""
from __future__ import annotations

import re
from pathlib import Path

SPEC = Path(__file__).resolve().parents[1] / "pyinstaller" / "nebula-lca-public.spec"


def _packed_files() -> list[str]:
    text = SPEC.read_text(encoding="utf-8")
    block = text.split("files = [", 1)[1].split("]", 1)[0]
    return re.findall(r'"([^"]+)"', block)


def test_public_spec_excludes_ecoinvent_derived_characterization() -> None:
    packed = _packed_files()
    assert "EF3.1/indicator_index.csv" in packed, "indicator/unit metadata table must ship"
    for forbidden in ("EF3.1/flow_index.csv", "EF3.1/lcia_factors.csv"):
        assert forbidden not in packed, f"{forbidden} is ecoinvent-derived and must not ship publicly"


def test_public_runtime_ignores_stale_local_characterization(monkeypatch, tmp_path):
    from app import solver_adapter
    configured = tmp_path / "verified-public"
    monkeypatch.setattr(solver_adapter.settings, "desktop_mode", True)
    monkeypatch.setattr(solver_adapter.settings, "public_characterization_only", True)
    monkeypatch.setattr(solver_adapter.settings, "nebula_lca_ef31_dir", str(configured))
    monkeypatch.setattr(solver_adapter.settings, "nebula_lca_runtime_root", str(tmp_path / "stale"))
    assert solver_adapter._embedded_ef31_candidates() == [configured]


def test_public_spec_packs_license_clean_characterization_trio() -> None:
    text = SPEC.read_text(encoding="utf-8")
    assert 'ef31_public = solver_root / "data" / "EF3.1-public"' in text
    assert 'Path("data") / "ef31-characterization"' in text
    assert 'for _name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv"):' in text
    assert "raise FileNotFoundError(f\"Public EF3.1 characterization asset missing" in text


def test_public_spec_never_packs_the_bundled_ef31_runtime() -> None:
    """The ecoinvent LCIA-implementation runtime must not ship, under any destination.

    ``desktop_entry._seed_runtime_if_needed`` copies ``_internal/runtime`` into the user's data
    dir on first boot, and ``solver_adapter._embedded_ef31_candidates`` puts that copy FIRST in
    the merged characterization order — so shipping it would let an unlicensed method package
    outrank the license-clean trio without any code change being visible in the payload layout.
    """
    text = SPEC.read_text(encoding="utf-8")
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    assert "release_assets" not in code, "public spec must not resolve the official EF3.1 runtime"
    assert "lcia-official" not in code
    assert 'Path("runtime")' not in code
    assert '"runtime/ef31"' not in code and "'runtime/ef31'" not in code
