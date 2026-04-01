from __future__ import annotations

import copy
import http.cookiejar
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.utils import dict_from_cookiejar

from hdl import cookiefile as cookiefile_util
from hdl import page_media
from hdl.config import AppConfig

try:
    import yt_dlp
except ImportError:  # pragma: no cover
    yt_dlp = None


def detect_site(url: str, cfg: AppConfig) -> str:
    host = (urlparse(url).hostname or "").lower()
    for site, keywords in cfg.g["site_host_keywords"].items():
        for kw in keywords:
            if kw in host:
                return site
    return "generic"


def _generic_http_headers(cfg: AppConfig) -> dict:
    g = cfg.g
    common = dict(g["headers_common"])
    if os.name == "nt":
        common["User-Agent"] = g["user_agent_windows"]
    else:
        common["User-Agent"] = g["user_agent_linux"]
    return common


def _fetch_generic_page_html(page_url: str, cfg: AppConfig, cookiefile: str | None) -> str | None:
    """GET the page HTML with generic headers, optional Netscape cookie file."""
    headers = _generic_http_headers(cfg)
    site = detect_site(page_url, cfg)
    referers = cfg.g["site_referers"]
    if site in referers:
        headers["Referer"] = referers[site]
    try:
        sess = requests.Session()
        sess.headers.update(headers)
        if cookiefile and os.path.isfile(cookiefile):
            p = Path(cookiefile)
            if cookiefile_util.netscape_cookie_file_loads(p):
                try:
                    jar = http.cookiejar.MozillaCookieJar(cookiefile)
                    jar.load(ignore_discard=True, ignore_expires=True)
                    sess.cookies.update(dict_from_cookiejar(jar))
                except Exception:
                    pass
        r = sess.get(page_url, timeout=int(cfg.g["socket_timeout"]))
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def build_generic_ydl_opts(
    cfg: AppConfig,
    url: str,
    output_dir: str,
    progress_hook,
    *,
    proxy: str | None,
    cookiefile: str | None,
    cookies_browser: str | None,
    max_retries: int,
    format_str: str | None = None,
):
    g = cfg.g
    site = detect_site(url, cfg)
    headers = _generic_http_headers(cfg)
    referers = g["site_referers"]
    if site in referers:
        headers["Referer"] = referers[site]
    fmt = format_str if format_str is not None else str(g["format"])
    ydl_opts = {
        "outtmpl": os.path.join(output_dir, g["outtmpl"]),
        "format": fmt,
        "progress_hooks": [progress_hook],
        "no_warnings": True,
        "extract_flat": False,
        "ignoreerrors": False,
        "retries": max_retries,
        "fragment_retries": max_retries,
        "socket_timeout": int(g["socket_timeout"]),
        "geo_bypass": bool(g["ydl_geo_bypass"]),
        "nocheckcertificate": bool(g["ydl_nocheckcertificate"]),
        "http_headers": headers,
        "extractor_retries": int(g["extractor_retries"]),
        "sleep_interval": int(g["sleep_interval"]),
        "max_sleep_interval": int(g["max_sleep_interval"]),
    }
    if cookiefile and os.path.isfile(cookiefile):
        if cookiefile_util.netscape_cookie_file_loads(Path(cookiefile)):
            ydl_opts["cookiefile"] = cookiefile
    elif cookies_browser:
        ydl_opts["cookiesfrombrowser"] = (cookies_browser,)
    if proxy:
        ydl_opts["proxy"] = proxy
    if site == "pornhub":
        ydl_opts["sleep_interval"] = int(g["pornhub_sleep_interval"])
        ydl_opts["max_sleep_interval"] = int(g["pornhub_max_sleep_interval"])
    ex = g.get("extractor_args")
    if isinstance(ex, dict) and ex:
        ydl_opts["extractor_args"] = copy.deepcopy(ex)
    mof = g.get("merge_output_format")
    if isinstance(mof, str) and mof.strip():
        ydl_opts["merge_output_format"] = mof.strip()
    return ydl_opts


def download_generic_video(
    cfg: AppConfig,
    url: str,
    output_dir: str,
    on_progress,
    *,
    proxy: str | None,
    cookiefile: str | None,
    cookies_browser: str | None,
    max_retries: int | None = None,
    retry_delay: int | None = None,
    on_status=None,
    on_transfer=None,
) -> tuple[bool, str]:
    """Returns (ok, title_or_error)."""
    if yt_dlp is None:
        return False, "yt-dlp is not installed"
    os.makedirs(output_dir, exist_ok=True)

    def progress_hook(d):
        if d["status"] == "downloading":
            tot = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            got = d.get("downloaded_bytes", 0)
            pct = (got / tot * 100) if tot > 0 else 0
            on_progress(pct)
            if on_transfer:
                on_transfer(got, tot, d.get("speed"))
        elif d["status"] == "finished":
            on_progress(100)
            if on_transfer:
                on_transfer(
                    d.get("total_bytes") or d.get("downloaded_bytes", 0),
                    d.get("total_bytes") or d.get("downloaded_bytes", 0),
                    d.get("speed"),
                )

    g = cfg.g
    mr = int(max_retries if max_retries is not None else g["max_retries"])
    rd = int(retry_delay if retry_delay is not None else g["retry_delay_sec"])
    err_max = int(g["error_message_max_len"])
    fneedle = str(g.get("format_error_substring", "format")).lower()
    primary = str(g["format"])
    fmt_chain = [primary]
    for x in g.get("format_fallbacks", []):
        s = str(x).strip()
        if s and s not in fmt_chain:
            fmt_chain.append(s)

    ext_list = list(
        g.get("fallback_media_extensions") or page_media.DEFAULT_FALLBACK_MEDIA_EXTENSIONS
    )
    extra_rx = list(g.get("fallback_media_extra_regexes") or [])

    def try_ytdlp_on_target(target_url: str) -> tuple[bool, str]:
        title = str(g["fallback_title"])
        for fmt_try in fmt_chain:
            try:
                if on_status:
                    on_status(f"generic try format: {fmt_try}")
                ydl_opts = build_generic_ydl_opts(
                    cfg,
                    target_url,
                    output_dir,
                    progress_hook,
                    proxy=proxy,
                    cookiefile=cookiefile,
                    cookies_browser=cookies_browser,
                    max_retries=mr,
                    format_str=fmt_try,
                )
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    if on_status:
                        on_status("generic extract info")
                    info = ydl.extract_info(target_url, download=False)
                    unk = str(g["unknown_title"])
                    fb = str(g["fallback_title"])
                    raw = info.get("title")
                    title = unk if raw is None else raw
                    title = str(title).strip() or fb
                    on_progress(0)
                    if on_status:
                        on_status("generic downloading")
                    ydl.download([target_url])
                on_progress(100)
                return True, title
            except Exception as e:
                err_s = str(e)[:err_max]
                if fmt_try != fmt_chain[-1] and fneedle in err_s.lower():
                    continue
                return False, err_s
        return False, "all formats failed"

    candidates: list[str] = [url]
    scraped = False
    retry_count = 0
    last_err = ""

    while retry_count <= mr:
        for cu in candidates:
            ok, msg = try_ytdlp_on_target(cu)
            if ok:
                return True, msg
            last_err = msg

        if not scraped and bool(g.get("use_page_media_fallback", True)):
            scraped = True
            if on_status:
                on_status("generic: scan page for stream urls")
            html = _fetch_generic_page_html(url, cfg, cookiefile)
            if html:
                found = page_media.extract_fallback_media_urls(
                    html,
                    url,
                    extensions=ext_list,
                    extra_regexes=extra_rx,
                )
                new_only = [u for u in found if u != url]
                if new_only:
                    candidates = list(dict.fromkeys(new_only + [url]))
                    continue

        retry_count += 1
        if retry_count <= mr:
            if on_status:
                on_status(f"retry {retry_count}/{mr}")
            time.sleep(rd)

    return False, last_err or "download failed"

