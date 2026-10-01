# Explicit public desktop payload; never select a developer runtime or database.
from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules

api_root = Path(SPECPATH).parent
solver_root = api_root.parent / "nebula-lca-solver"
files = [
    "EF3.1/flow_index.csv", "EF3.1/indicator_index.csv", "EF3.1/lcia_factors.csv",
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
hidden = collect_submodules("app") + collect_submodules("uvicorn")
a = Analysis([str(api_root / "app/desktop_entry.py")], pathex=[str(api_root)],
             binaries=[], datas=datas, hiddenimports=hidden, hookspath=[],
             runtime_hooks=[], excludes=["pytest", "tkinter"], noarchive=False)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, [], exclude_binaries=True, name="nebula-lca-api",
          console=True, debug=False, strip=False, upx=False)
coll = COLLECT(exe, a.binaries, a.datas, name="nebula-lca-api", strip=False, upx=False)
