# Explicit public desktop payload; never select a developer runtime or database.
from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules

api_root = Path(SPECPATH).parent
solver_root = api_root.parent / "nebula-lca-solver"
files = [
    # indicator_index.csv (LCIA_unit variant) is the indicator/unit metadata table required by
    # provider_ef31 and main.py; it carries no characterization factors.  The ecoinvent-derived
    # flow_index.csv / lcia_factors.csv must never ship in a public build: characterization is
    # served by the license-clean data/ef31-characterization trio packed further below.
    "EF3.1/indicator_index.csv",
    "Tiangong/tidas_reference_seed.json",
    "provider_profiles/steel_elementary_catalog.v1.csv",
    "provider_profiles/steel_elementary_catalog.v1.provenance.json",
    "provider_profiles/steel_tiangong_open_flows.v2.json",
    "provider_profiles/steel_tiangong_open_references.v1.json",
]
mapping = api_root / "data/flow_mappings/nebula-flow-mapping-v1"
datas = [(str(api_root / "data" / name), str(Path("data") / Path(name).parent)) for name in files]
datas += [(str(mapping / name), str(Path("data/flow_mappings/nebula-flow-mapping-v1") / Path(name).parent))
          for name in ("MANIFEST.json", "ACCEPTANCE.json", "LICENSE",
                       "data/unit-conversions.v1.json", "data/intermediate-flow-mappings.v1.jsonl",
                       "data/elementary-flow-mappings.v1.jsonl")]
datas += [(str(p), str(Path("solver-core") / p.parent.relative_to(solver_root / "app/core")))
          for p in (solver_root / "app/core").rglob("*.py")]
# Public EF3.1 characterization runtime (JRC ILCD source, license-clean).
# Provides the ecoinvent_category indicator_index + matching flow_index/lcia_factors
# required by the engine's characterization resolver. Replaces the ecoinvent-derived
# solver-data/EF3.1 that cannot ship in public builds.
ef31_public = solver_root / "data" / "EF3.1-public"
for _name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv"):
    _src = ef31_public / _name
    if not _src.is_file():
        raise FileNotFoundError(f"Public EF3.1 characterization asset missing: {_src}")
datas += [(str(ef31_public / _name), str(Path("data") / "ef31-characterization"))
          for _name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv")]
hidden = collect_submodules("app") + collect_submodules("uvicorn")
a = Analysis([str(api_root / "app/desktop_entry.py")], pathex=[str(api_root)],
             binaries=[], datas=datas, hiddenimports=hidden, hookspath=[],
             runtime_hooks=[], excludes=["pytest", "tkinter"], noarchive=False)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, [], exclude_binaries=True, name="nebula-lca-api",
          console=True, debug=False, strip=False, upx=False)
coll = COLLECT(exe, a.binaries, a.datas, name="nebula-lca-api", strip=False, upx=False)
