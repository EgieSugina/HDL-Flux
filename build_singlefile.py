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


def run(cmd: list[str]) -> None:
    print("[build]", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)


def bin_name(base: str, is_windows: bool) -> str:
    return base + (".exe" if is_windows else "")


def venv_python(is_windows: bool) -> Path:
    if is_windows:
        return BUILD_VENV / "Scripts" / "python.exe"
    return BUILD_VENV / "bin" / "python"


def main() -> int:
    is_windows = os.name == "nt"
    if BUILD.exists():
        shutil.rmtree(BUILD, ignore_errors=True)
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
    run([py, "-m", "pip", "install", "-r", str(REQ), "pyinstaller"])
    pyinstaller = [py, "-m", "PyInstaller"]

    # CLI single file
    run(
        pyinstaller
        + [
            "--noconfirm",
            "--clean",
            "--onefile",
            "--add-data",
            "config.defaults.json:.",
            "--name",
            "hdl-flux-cli",
            "serve.py",
        ]
    )

    # GUI single file
    gui_args = [
        "--noconfirm",
        "--clean",
        "--onefile",
        "--add-data",
        "config.defaults.json:.",
        "--name",
        "hdl-flux-gui",
    ]
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

