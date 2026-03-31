#!/usr/bin/env python3
"""
Unified downloader: hstream.moe (HLS/MPD + Selenium) and generic sites (yt-dlp).
Linux and Windows; uses the OS default browser for Selenium and cookie export when enabled.

Configuration (no hardcoded site/path defaults in code paths):
  - config.defaults.json — bundled template; copied to config.json on first run if missing.
  - config.json — your overrides (merged on top of defaults).
  - .env — optional KEY=value lines (only sets env vars that are not already set).
  - Environment HDL_* overrides from `hdl/config.py`; HDL_CONFIG = path to JSON file.
"""

from hdl.app import run_with_interrupt_handling


if __name__ == "__main__":
    run_with_interrupt_handling()
