from __future__ import annotations

import copy
import http.cookiejar
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.utils import dict_from_cookiejar

from hdl import cookiefile as cookiefile_util
from hdl import embed_resolver, page_media
from hdl.config import AppConfig

try:
    import yt_dlp
except ImportError:  # pragma: no cover
    yt_dlp = None

_UNSUPPORTED_URL_NEEDLE = "unsupported url"


def detect_site(url: str, cfg: AppConfig) -> str:
    host = (urlparse(url).hostname or "").lower()
    for site, keywords in cfg.g["site_host_keywords"].items():
        for kw in keywords:
            if kw in host:
                return site
    return "generic"


def _valid_cookiefile(cookiefile: str | None) -> str | None:
    if not cookiefile or not os.path.isfile(cookiefile):
        return None
    if cookiefile_util.netscape_cookie_file_loads(Path(cookiefile)):
        return cookiefile
    return None


def _generic_http_headers(cfg: AppConfig, *, referer: str | None = None) -> dict:
    g = cfg.g
    common = dict(g["headers_common"])
    if os.name == "nt":
        common["User-Agent"] = g["user_agent_windows"]
    else:
        common["User-Agent"] = g["user_agent_linux"]
    if referer:
        common["Referer"] = referer
    return common


def _fetch_generic_page_html(
    page_url: str,
    cfg: AppConfig,
    cookiefile: str | None,
    *,
    referer: str | None = None,
) -> str | None:
    """GET the page HTML with generic headers, optional Netscape cookie file."""
    headers = _generic_http_headers(cfg, referer=referer)
    site = detect_site(page_url, cfg)
    if referer is None:
        referers = cfg.g["site_referers"]
        if site in referers:
            headers["Referer"] = referers[site]
    valid_cookie = _valid_cookiefile(cookiefile)
    try:
        sess = requests.Session()
        sess.headers.update(headers)
        if valid_cookie:
            try:
                jar = http.cookiejar.MozillaCookieJar(valid_cookie)
                jar.load(ignore_discard=True, ignore_expires=True)
                sess.cookies.update(dict_from_cookiejar(jar))
            except Exception:
                pass
        r = sess.get(page_url, timeout=int(cfg.g["socket_timeout"]))
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def _embed_first_hosts(cfg: AppConfig) -> tuple[str, ...]:
    raw = cfg.g.get("embed_first_hosts")
    if isinstance(raw, list) and raw:
        return tuple(str(x).lower() for x in raw)
    return embed_resolver._EMBED_FIRST_HOSTS_DEFAULT


def _collect_stream_targets(
    page_url: str,
    cfg: AppConfig,
    cookiefile: str | None,
) -> list[tuple[str, str]]:
    """Return (media_url, referer_for_headers) from page + embed players."""
    g = cfg.g
    ext_list = list(
        g.get("fallback_media_extensions") or page_media.DEFAULT_FALLBACK_MEDIA_EXTENSIONS
    )
    extra_rx = list(g.get("fallback_media_extra_regexes") or [])
    bait = tuple(g.get("jwplayer_bait_host_substrings") or [])
    valid_cookie = _valid_cookiefile(cookiefile)

    def fetch(u: str) -> str | None:
        return _fetch_generic_page_html(u, cfg, valid_cookie, referer=page_url)

    html = fetch(page_url)
    if not html:
        return []

    if not bool(g.get("use_page_media_fallback", True)):
        return []

    scrape_embeds = bool(g.get("jwplayer_follow_embeds", True))
    needs_embed = embed_resolver.page_needs_embed_resolve(
        html, page_url, embed_first_hosts=_embed_first_hosts(cfg)
    )

    if needs_embed or scrape_embeds:
        return embed_resolver.resolve_stream_targets(
            page_url,
            html,
            fetch,
            extensions=ext_list,
            extra_regexes=extra_rx,
            follow_embeds=scrape_embeds,
            max_embed_depth=int(g.get("jwplayer_embed_max_depth", 1)),
            bait_substrings=bait if bait else None,
        )

    urls = page_media.extract_fallback_media_urls(
        html, page_url, extensions=ext_list, extra_regexes=extra_rx
    )
    return [(u, page_url) for u in urls]


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
    referer: str | None = None,
    outtmpl_override: str | None = None,
):
    g = cfg.g
    site = detect_site(url, cfg)
    headers = _generic_http_headers(cfg, referer=referer)
    referers = g["site_referers"]
    if referer is None and site in referers:
        headers["Referer"] = referers[site]
    fmt = format_str if format_str is not None else str(g["format"])
    outtmpl = outtmpl_override if outtmpl_override else str(g["outtmpl"])
    ydl_opts = {
        "outtmpl": os.path.join(output_dir, outtmpl),
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
        "concurrent_fragment_downloads": int(g.get("concurrent_fragment_downloads", 1)),
    }
    valid_cookie = _valid_cookiefile(cookiefile)
    if valid_cookie:
        ydl_opts["cookiefile"] = valid_cookie
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
    fmt_sort = g.get("format_sort")
    if isinstance(fmt_sort, list) and fmt_sort:
        ydl_opts["format_sort"] = [str(x) for x in fmt_sort if str(x).strip()]
    return ydl_opts


def _is_unsupported_page_url(err: str) -> bool:
    return _UNSUPPORTED_URL_NEEDLE in (err or "").lower()


def _slug_from_page_url(page_url: str) -> str:
    """Filesystem-safe basename from the source page URL path (last segment)."""
    path = urlparse(page_url).path.strip("/")
    slug = path.rsplit("/", 1)[-1] if path else "video"
    slug = re.sub(r"[^\w.-]+", "_", slug)[:120].strip("._")
    return slug or "video"


def _outtmpl_for_source_slug(file_base: str) -> str:
    """yt-dlp output template: fixed name from page URL, never HLS 'master'."""
    safe = re.sub(r"[^\w.-]+", "_", file_base)[:120].strip("._") or "video"
    return f"{safe}.%(ext)s"


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
    valid_cookie = _valid_cookiefile(cookiefile)

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
    primary = str(g.get("format_best") or g["format"])
    fmt_chain = [primary]
    for x in g.get("format_fallbacks", []):
        s = str(x).strip()
        if s and s not in fmt_chain:
            fmt_chain.append(s)
    fb_default = str(g.get("format_fallback_chain", "")).strip()
    if fb_default and fb_default not in fmt_chain:
        fmt_chain.append(fb_default)
    best_single = str(g.get("format_best_single", "best")).strip()
    if best_single and best_single not in fmt_chain:
        fmt_chain.append(best_single)

    source_slug = _slug_from_page_url(url)
    use_source_name = bool(g.get("use_source_url_filename", True))

    targets: list[tuple[str, str]] = []
    if bool(g.get("embed_resolve_before_ytdlp", True)):
        if on_status:
            on_status("generic: resolve embed streams")
        targets = _collect_stream_targets(url, cfg, valid_cookie)

    # Stream URLs first; page URL only when no embed streams were resolved.
    # file_base: when set, output is {slug-from-page-url}.ext (not master.mp4).
    candidates: list[tuple[str, str | None, str | None]] = []
    for media_url, referer in targets:
        base = source_slug if use_source_name else None
        candidates.append((media_url, referer, base))
    if not targets:
        candidates.append((url, None, None))

    def try_ytdlp_on_target(
        target_url: str,
        *,
        referer: str | None,
        file_base: str | None,
    ) -> tuple[bool, str]:
        if file_base:
            out_tmpl = _outtmpl_for_source_slug(file_base)
            title = file_base
        else:
            out_tmpl = None
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
                    cookiefile=valid_cookie,
                    cookies_browser=cookies_browser,
                    max_retries=mr,
                    format_str=fmt_try,
                    referer=referer,
                    outtmpl_override=out_tmpl,
                )
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    if on_status:
                        on_status("generic extract info")
                    if not file_base:
                        info = ydl.extract_info(target_url, download=False)
                        unk = str(g["unknown_title"])
                        fb = source_slug or str(g["fallback_title"])
                        raw = info.get("title")
                        title = unk if raw is None else raw
                        title = str(title).strip() or fb
                        if title.lower() in (
                            "master",
                            "index",
                            "playlist",
                            "video",
                            "hls",
                        ):
                            title = source_slug or fb
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

    retry_count = 0
    last_err = ""

    while retry_count <= mr:
        for target_url, referer, file_base in candidates:
            ok, msg = try_ytdlp_on_target(
                target_url, referer=referer, file_base=file_base
            )
            if ok:
                return True, msg
            last_err = msg
            if targets and _is_unsupported_page_url(msg):
                continue

        if not targets and bool(g.get("use_page_media_fallback", True)):
            if on_status:
                on_status("generic: scan page for stream urls")
            targets = _collect_stream_targets(url, cfg, valid_cookie)
            if targets:
                base = source_slug if use_source_name else None
                candidates = [(u, ref, base) for u, ref in targets]
                continue

        retry_count += 1
        if retry_count <= mr:
            if on_status:
                on_status(f"retry {retry_count}/{mr}")
            time.sleep(rd)

    return False, last_err or "download failed"
