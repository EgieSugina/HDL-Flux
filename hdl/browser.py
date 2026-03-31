from __future__ import annotations

import copy
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from hdl.config import AppConfig

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.edge.options import Options as EdgeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

try:
    import yt_dlp

    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False


def _match_browser_rules(haystack: str, rules: list) -> str | None:
    h = haystack.lower()
    for rule in rules:
        for sub in rule.get("haystack_contains", []):
            if sub.lower() in h:
                return str(rule["browser"])
    return None


def default_browser_ytdlp_name(cfg: AppConfig) -> str:
    bd = cfg._d["browser_detection"]
    if sys.platform == "win32":
        w = bd["windows"]
        try:
            import winreg

            hive = getattr(winreg, w["registry_hive"])
            with winreg.OpenKey(hive, w["subkey"]) as k:
                prog_id, _ = winreg.QueryValueEx(k, w["value_name"])
            hit = _match_browser_rules(prog_id, w["rules"])
            if hit:
                return hit
        except OSError:
            pass
        return str(w["fallback"])
    if sys.platform.startswith("linux"):
        lx = bd["linux"]
        try:
            r = subprocess.run(
                list(lx["command"]),
                capture_output=True,
                text=True,
                timeout=float(cfg.selenium_cfg["xdg_mime_timeout_sec"]),
            )
            d = (r.stdout or "").strip().lower()
            hit = _match_browser_rules(d, lx["rules"])
            if hit:
                return hit
        except (OSError, subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return str(lx["fallback"])
    if sys.platform == "darwin":
        dcfg = bd["darwin"]
        try:
            r = subprocess.run(
                list(dcfg["command"]),
                capture_output=True,
                text=True,
                timeout=float(cfg.selenium_cfg["launchservices_timeout_sec"]),
            )
            out = (r.stdout or "").lower()
            hit = _match_browser_rules(out, dcfg["rules"])
            if hit:
                return hit
        except (OSError, subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return str(dcfg["fallback"])
    return str(bd["fallback_other"])


def _selenium_browser_order(cfg: AppConfig) -> list[str]:
    primary = default_browser_ytdlp_name(cfg)
    all_browsers = list(cfg.selenium_cfg["browser_fallback_order"])
    order = [primary]
    for b in all_browsers:
        if b not in order:
            order.append(b)
    if primary == "safari" and SELENIUM_AVAILABLE:
        order = ["safari"] + [b for b in order if b != "safari"]
    return order


def _common_chromium_args(headless: bool, cfg: AppConfig) -> list[str]:
    sc = cfg.selenium_cfg
    args = list(sc["chromium_args"])
    args.append(sc["window_size_arg_prefix"] + sc["window_size"])
    if headless:
        args.insert(0, sc["headless_flag"])
    return args


def create_selenium_driver(headless: bool, cfg: AppConfig):
    if not SELENIUM_AVAILABLE:
        return None, cfg.h_msg("selenium_not_installed")

    sc = cfg.selenium_cfg
    ua = sc["user_agent_linux"] if sys.platform.startswith("linux") else sc["user_agent_windows"]
    err_max = int(cfg.g["error_message_max_len"])
    brave_names = sc["brave_binary_names"]
    ch_names = sc["chromium_binary_names"]

    last_err = ""
    for browser in _selenium_browser_order(cfg):
        try:
            if browser in ("chrome", "chromium"):
                opts = ChromeOptions()
                for a in _common_chromium_args(headless, cfg):
                    opts.add_argument(a)
                opts.add_argument(f"user-agent={ua}")
                if browser == "chromium":
                    ch_bin = None
                    for name in ch_names:
                        ch_bin = shutil.which(name)
                        if ch_bin:
                            break
                    if ch_bin:
                        opts.binary_location = ch_bin
                drv = webdriver.Chrome(options=opts)
                return drv, browser
            if browser == "edge":
                opts = EdgeOptions()
                for a in _common_chromium_args(headless, cfg):
                    opts.add_argument(a)
                opts.add_argument(f"user-agent={ua}")
                drv = webdriver.Edge(options=opts)
                return drv, browser
            if browser == "firefox":
                opts = FirefoxOptions()
                if headless:
                    opts.add_argument("-headless")
                drv = webdriver.Firefox(options=opts)
                return drv, browser
            if browser == "brave":
                opts = ChromeOptions()
                for a in _common_chromium_args(headless, cfg):
                    opts.add_argument(a)
                opts.add_argument(f"user-agent={ua}")
                brave = None
                for name in brave_names:
                    brave = shutil.which(name)
                    if brave:
                        break
                if brave:
                    opts.binary_location = brave
                drv = webdriver.Chrome(options=opts)
                return drv, browser
            if browser == "safari":
                drv = webdriver.Safari()
                return drv, browser
        except Exception as e:
            last_err = str(e)[:err_max]
            continue
    return None, last_err or cfg.h_msg("no_webdriver")


def kill_browser_for_cookies(browser_name: str, cfg: AppConfig) -> bool:
    bk = cfg.browser_kill_cfg
    killed = False
    tkill = float(bk["taskkill_timeout_sec"])
    pk = float(bk["pkill_timeout_sec"])
    if os.name == "nt":
        win_map = bk["windows"]
        for proc in win_map.get(browser_name, []):
            try:
                r = subprocess.run(
                    ["taskkill", "/F", "/IM", proc],
                    capture_output=True,
                    text=True,
                    timeout=tkill,
                )
                if r.returncode == 0:
                    killed = True
            except (OSError, subprocess.TimeoutExpired):
                pass
    else:
        unix_map = bk["unix"]
        for proc in unix_map.get(browser_name, []):
            try:
                r = subprocess.run(["pkill", "-x", proc], capture_output=True, timeout=pk)
                if r.returncode == 0:
                    killed = True
            except (OSError, subprocess.TimeoutExpired):
                pass
            try:
                r = subprocess.run(["pkill", "-f", proc], capture_output=True, timeout=pk)
                if r.returncode == 0:
                    killed = True
            except (OSError, subprocess.TimeoutExpired):
                pass
    if killed:
        time.sleep(float(bk["after_kill_sleep_sec"]))
    return killed


def export_cookies_ytdlp(browser_name: str, out_path: Path, cfg: AppConfig) -> bool:
    if not YTDLP_AVAILABLE:
        return False
    urls = cfg.g["cookie_export_probe_urls"]
    ydl_opts = {
        "cookiesfrombrowser": (browser_name,),
        "cookiefile": str(out_path),
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
    }
    ex = cfg.g.get("extractor_args")
    if isinstance(ex, dict) and ex:
        ydl_opts["extractor_args"] = copy.deepcopy(ex)
    for url in urls:
        if not isinstance(url, str) or not url.strip():
            continue
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(url.strip(), download=False)
            if out_path.is_file() and out_path.stat().st_size > 0:
                return True
        except Exception:
            continue
    return False

