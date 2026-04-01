#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
import venv


ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"
BUILD = ROOT / "build"
BUILD_VENV = ROOT / ".build-venv"
REQ = ROOT / "requirements.txt"
ICON_PNG = ROOT / "icon.png"
BUILD_ASSETS = ROOT / ".build-assets"


def run(cmd: list[str]) -> None:
    print("[build]", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)


def bin_name(base: str, is_windows: bool) -> str:
    return base + (".exe" if is_windows else "")


def venv_python(is_windows: bool) -> Path:
    if is_windows:
        return BUILD_VENV / "Scripts" / "python.exe"
    return BUILD_VENV / "bin" / "python"


def prepare_windows_ico(py: str) -> Path | None:
    if not ICON_PNG.is_file():
        return None
    BUILD_ASSETS.mkdir(exist_ok=True)
    ico_path = BUILD_ASSETS / "app.ico"
    code = (
        "from PIL import Image\n"
        f"img=Image.open(r'{ICON_PNG}')\n"
        f"img.save(r'{ico_path}', sizes=[(16,16),(24,24),(32,32),(48,48),(64,64),(128,128),(256,256)])\n"
    )
    run([py, "-c", code])
    return ico_path if ico_path.is_file() else None


def main() -> int:
    is_windows = os.name == "nt"
    if BUILD.exists():
        shutil.rmtree(BUILD, ignore_errors=True)
    if BUILD_ASSETS.exists():
        shutil.rmtree(BUILD_ASSETS, ignore_errors=True)
    DIST.mkdir(exist_ok=True)

    # Portable builder: create dedicated build venv automatically.
    if not BUILD_VENV.exists():
        print("[build] creating isolated build venv ...")
        venv.create(BUILD_VENV, with_pip=True)
    py = str(venv_python(is_windows))
    if not Path(py).exists():
        raise RuntimeError("Build venv python not found.")

    print("[build] installing build dependencies in .build-venv ...")
    run([py, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
    run([py, "-m", "pip", "install", "-r", str(REQ), "pyinstaller", "pillow"])
    pyinstaller = [py, "-m", "PyInstaller"]
    windows_ico = prepare_windows_ico(py) if is_windows else None
    icon_arg = str(windows_ico) if windows_ico else (str(ICON_PNG) if ICON_PNG.is_file() else "")

    # CLI single file
    cli_args = [
        "--noconfirm",
        "--clean",
        "--onefile",
        "--add-data",
        "config.defaults.json:.",
    ]
    if ICON_PNG.is_file():
        cli_args += ["--add-data", "icon.png:."]
    if icon_arg:
        cli_args += ["--icon", icon_arg]
    run(
        pyinstaller
        + cli_args
        + ["--name", "hdl-flux-cli", "serve.py"]
    )

    # GUI single file
    gui_args = [
        "--noconfirm",
        "--clean",
        "--onefile",
        "--add-data",
        "config.defaults.json:.",
    ]
    if ICON_PNG.is_file():
        gui_args += ["--add-data", "icon.png:."]
    if icon_arg:
        gui_args += ["--icon", icon_arg]
    gui_args += ["--name", "hdl-flux-gui"]
    if is_windows:
        gui_args.append("--windowed")
    run(pyinstaller + gui_args + ["gui.py"])

    cli_path = DIST / bin_name("hdl-flux-cli", is_windows)
    gui_path = DIST / bin_name("hdl-flux-gui", is_windows)
    print("[build] done")
    print(f"[build] cli: {cli_path}")
    print(f"[build] gui: {gui_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

