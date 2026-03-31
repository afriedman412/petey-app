# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for Petey Desktop (macOS + Windows).

Build from petey-web/:
    pyinstaller desktop/petey.spec
"""
import importlib
import platform
import shutil
import subprocess
from pathlib import Path

ROOT = Path(SPECPATH).parent  # petey-web/
IS_MAC = platform.system() == "Darwin"
IS_WIN = platform.system() == "Windows"

# Find pymupdf's layout resources (ONNX models)
pymupdf_path = Path(importlib.import_module("pymupdf").__path__[0])
pymupdf_datas = [(str(pymupdf_path / "layout" / "resources"), "pymupdf/layout/resources")]

# ---------------------------------------------------------------------------
# System binaries: tesseract + ghostscript
# ---------------------------------------------------------------------------
sys_datas = []

if IS_MAC:
    def brew_prefix(pkg):
        try:
            return subprocess.check_output(
                ["brew", "--prefix", pkg], text=True
            ).strip()
        except Exception:
            return None

    tesseract_prefix = brew_prefix("tesseract")
    ghostscript_prefix = brew_prefix("ghostscript")
    leptonica_prefix = brew_prefix("leptonica")

    if tesseract_prefix:
        sys_datas.append((f"{tesseract_prefix}/bin/tesseract", "bin"))
        sys_datas.append((f"{tesseract_prefix}/lib", "lib"))
        sys_datas.append((f"{tesseract_prefix}/share/tessdata", "share/tessdata"))
    if leptonica_prefix:
        sys_datas.append((f"{leptonica_prefix}/lib", "lib"))
    if ghostscript_prefix:
        sys_datas.append((f"{ghostscript_prefix}/bin/gs", "bin"))
        sys_datas.append((f"{ghostscript_prefix}/lib", "lib"))
        sys_datas.append((f"{ghostscript_prefix}/share/ghostscript", "share/ghostscript"))

    for pkg in ["libarchive", "jbig2dec", "libtiff", "libpng", "jpeg-turbo",
                "little-cms2", "libidn", "fontconfig", "freetype", "openjpeg"]:
        prefix = brew_prefix(pkg)
        if prefix:
            sys_datas.append((f"{prefix}/lib", "lib"))

elif IS_WIN:
    tess = shutil.which("tesseract")
    if tess:
        tess_dir = Path(tess).parent
        sys_datas.append((str(tess_dir), "bin"))
        tessdata = tess_dir / "tessdata"
        if not tessdata.exists():
            tessdata = tess_dir.parent / "tessdata"
        if tessdata.exists():
            sys_datas.append((str(tessdata), "share/tessdata"))

    gs = shutil.which("gswin64c") or shutil.which("gswin32c") or shutil.which("gs")
    if gs:
        gs_dir = Path(gs).parent
        sys_datas.append((str(gs_dir), "bin"))
        gs_lib = gs_dir.parent / "lib"
        if gs_lib.exists():
            sys_datas.append((str(gs_lib), "share/ghostscript"))

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
a = Analysis(
    [str(ROOT / "desktop" / "launch.py")],
    pathex=[str(ROOT)],
    datas=[
        (str(ROOT / "templates"), "templates"),
        (str(ROOT / "static"), "static"),
        (str(ROOT / "schemas"), "schemas"),
    ] + pymupdf_datas + sys_datas,
    hiddenimports=[
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
        "server",
        "server.app",
        "server.auth",
        "server.extract",
        "server.settings",
        "server.runs",
        "server.validate_keys",
        "server.parse_client",
        "server.par_extract",
        "petey",
        "petey.extract",
        "petey.schema",
        "petey.plugins",
        "petey.plugins.liteparse",
        "petey.plugins.unstructured",
        "petey.plugins.docling",
        "petey.plugins.azure_documentai",
        "petey.plugins.google_documentai",
        "petey.plugins.textract",
        "yaml",
        "multipart",
        "httpx",
        "pikepdf",
        "anthropic",
    ],
    excludes=[
        "firebase_admin",
        "google.cloud.firestore",
        "tkinter",
        "matplotlib",
        "scipy",
    ],
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Petey",
    console=True,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    name="Petey",
)
