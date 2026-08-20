# -*- mode: python ; coding: utf-8 -*-

import json
import shutil
import sys
from pathlib import Path

repo_root = Path.cwd().parent
api_root = repo_root / "nebula-lca-api"
solver_root = repo_root / "nebula-lca-solver"
sys.path.insert(0, str(api_root))

from app.release_assets import select_latest_public_mapping_root, select_official_ef31_runtime


mapping_parent = api_root / "data" / "flow_mappings"
public_mapping_root = select_latest_public_mapping_root(mapping_parent)
public_mapping_files = (
    "ACCEPTANCE.json",
    "LICENSE",
    "MANIFEST.json",
    "NOTICE.md",
    "data/elementary-flow-mappings.v1.jsonl",
    "data/intermediate-flow-mappings.v1.jsonl",
    "data/unit-conversions.v1.json",
)
api_data_files = (
    "EF3.1/flow_index.csv",
    "EF3.1/indicator_index.csv",
    "EF3.1/lcia_factors.csv",
    "Tiangong/ILCD_Unit_Groups.xlsx",
    "Tiangong/elementary_flows_sample.csv",
    "Tiangong/intermediate_flows_sample.csv",
    "Tiangong/tiangong_processes.zip",
    "Tiangong/tidas_reference_catalog.json",
    "Tiangong/tidas_reference_seed.json",
)

runtime_manifest, runtime_artifact = select_official_ef31_runtime(api_root / "runtime")
release_runtime_root = api_root / "build" / "release-runtime"
if release_runtime_root.exists():
    shutil.rmtree(release_runtime_root)
release_ef31_root = release_runtime_root / "ef31"
release_artifact = release_ef31_root / str(runtime_manifest["job_id"])
release_artifact.mkdir(parents=True)
for name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv"):
    shutil.copy2(runtime_artifact / name, release_artifact / name)
normalized_manifest = dict(runtime_manifest)
normalized_manifest["artifact_dir"] = str(runtime_manifest["job_id"])
normalized_manifest["output_dir"] = str(runtime_manifest["job_id"])
(release_ef31_root / "active_manifest.json").write_text(
    json.dumps(normalized_manifest, ensure_ascii=False),
    encoding="utf-8",
)

datas = [
    *(
        (str(api_root / "data" / relative), f"data/{Path(relative).parent}")
        for relative in api_data_files
    ),
    (str(mapping_parent / "ghg_ef31_v1.json"), "data/flow_mappings"),
    *(
        (str(public_mapping_root / relative), f"data/flow_mappings/{public_mapping_root.name}/{Path(relative).parent}")
        for relative in public_mapping_files
    ),
    (str(release_runtime_root), "runtime"),
]
if (solver_root / "data").exists():
    datas.extend(
        (str(solver_root / "data" / "EF3.1" / name), "solver-data/EF3.1")
        for name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv")
    )
if (solver_root / "app" / "core").exists():
    datas.extend(
        (str(path), f"solver-core/{path.parent.relative_to(solver_root / 'app' / 'core')}")
        for path in (solver_root / "app" / "core").rglob("*.py")
    )

pathex = [str(api_root), str(solver_root)]

a = Analysis(
    [str(api_root / "app" / "desktop_entry.py")],
    pathex=pathex,
    binaries=[],
    datas=datas,
    hiddenimports=[
        "app.main",
        "uvicorn",
        "uvicorn.logging",
        "uvicorn.loops",
        "uvicorn.loops.auto",
        "uvicorn.protocols",
        "uvicorn.protocols.http",
        "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets",
        "uvicorn.protocols.websockets.auto",
        "uvicorn.lifespan",
        "uvicorn.lifespan.on",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="nebula-lca-api",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="nebula-lca-api",
)
