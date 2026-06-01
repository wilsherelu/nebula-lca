# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

repo_root = Path.cwd().parent
api_root = repo_root / "nebula-lca-api"
solver_root = repo_root / "nebula-lca-solver"

datas = [
    (str(api_root / "data"), "data"),
]
if (api_root / "runtime").exists():
    datas.append((str(api_root / "runtime"), "runtime"))
if (solver_root / "data").exists():
    datas.append((str(solver_root / "data"), "solver-data"))
if (solver_root / "app" / "core").exists():
    datas.append((str(solver_root / "app" / "core"), "solver-core"))

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
