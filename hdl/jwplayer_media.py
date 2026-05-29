"""
JW Player page scraping: setup configs, VOE-style obfuscated JSON, embed discovery.
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from hdl import page_media

JWPLAYER_SCRIPT_RE = re.compile(r"/jwplayer(?:/\d+(?:\.\d+)*)?/jwplayer\.js", re.I)
JWPLAYER_CALL_RE = re.compile(r"\bjwplayer\s*\(", re.I)

# Obfuscation markers used by VOE / vickisaveworker-style hosts (see voe-dl method 8).
_OBFUSCATION_MARKERS = ("@$", "^^", "~@", "%?", "*~", "!!", "#&")

# Decoy `var source=` values on JW embed pages.
_BAIT_HOST_SUBSTRINGS = (
    "test-videos.co.uk",
    "sample-videos.com",
    "commondatastorage.googleapis.com/gtv-videos-bucket",
)

_FILE_IN_SETUP_RE = re.compile(
    r"""(?P<q>['"])(?P<url>https?://[^'"]+\.(?:m3u8|m3u|mpd|mp4|mkv|webm)(?:\?[^'"]*)?)(?P=q)""",
    re.I,
)
_VAR_SOURCE_RE = re.compile(
    r"""var\s+source\s*=\s*(?P<q>['"])(?P<url>https?://[^'"]+)(?P=q)""",
    re.I,
)
_APP_JSON_SCRIPT_RE = re.compile(
    r'<script[^>]+type\s*=\s*["\']application/json["\'][^>]*>(?P<body>.*?)</script>',
    re.I | re.S,
)
_EMBED_URL_RE = re.compile(
    r"""(?:data-embed|data-src|data-url)\s*=\s*["'](?P<url>[^"']+)["']""",
    re.I,
)
_IFRAME_SRC_RE = re.compile(
    r"""<iframe\b[^>]*\bsrc\s*=\s*["'](?P<url>[^"']+)["']""",
    re.I,
)


def page_uses_jwplayer(html: str) -> bool:
    if not html:
        return False
    return bool(JWPLAYER_SCRIPT_RE.search(html) or JWPLAYER_CALL_RE.search(html))


def _is_bait_url(url: str, bait_substrings: tuple[str, ...] | None = None) -> bool:
    low = (url or "").lower()
    needles = bait_substrings if bait_substrings is not None else _BAIT_HOST_SUBSTRINGS
    return any(n in low for n in needles)


def _rot13(text: str) -> str:
    out: list[str] = []
    for ch in text:
        o = ord(ch)
        if 65 <= o <= 90:
            out.append(chr(((o - 65 + 13) % 26) + 65))
        elif 97 <= o <= 122:
            out.append(chr(((o - 97 + 13) % 26) + 97))
        else:
            out.append(ch)
    return "".join(out)


def _strip_obfuscation_markers(txt: str) -> str:
    for pat in _OBFUSCATION_MARKERS:
        txt = txt.replace(pat, "")
    return txt


def _shift_chars(text: str, shift: int) -> str:
    return "".join(chr(ord(c) - shift) for c in text)


def _safe_b64_decode(s: str) -> str:
    pad = len(s) % 4
    if pad:
        s += "=" * (4 - pad)
    return base64.b64decode(s).decode("utf-8", errors="replace")


def deobfuscate_embedded_json(raw_json: str) -> dict[str, Any] | str | None:
    """Decode VOE-style obfuscated JSON from <script type=\"application/json\">."""
    try:
        arr = json.loads(raw_json)
        if not (isinstance(arr, list) and arr and isinstance(arr[0], str)):
            return None
        obf = arr[0]
    except (json.JSONDecodeError, TypeError, IndexError):
        return None

    try:
        step1 = _rot13(obf)
        step2 = _strip_obfuscation_markers(step1)
        step3 = _safe_b64_decode(step2)
        step4 = _shift_chars(step3, 3)
        step5 = step4[::-1]
        step6 = _safe_b64_decode(step5)
        try:
            parsed = json.loads(step6)
            return parsed if isinstance(parsed, dict) else step6
        except json.JSONDecodeError:
            return step6
    except Exception:
        return None


def _urls_from_decoded_payload(
    payload: dict[str, Any] | str,
    *,
    bait_substrings: tuple[str, ...] | None = None,
) -> list[str]:
    found: list[str] = []

    def add(raw: str | None) -> None:
        if not raw or not isinstance(raw, str):
            return
        u = raw.strip()
        if not u.startswith(("http://", "https://")):
            return
        if _is_bait_url(u, bait_substrings):
            return
        if u not in found:
            found.append(u)

    if isinstance(payload, dict):
        for key in ("source", "hls"):
            val = payload.get(key)
            if isinstance(val, str):
                add(val)
        fallback = payload.get("fallback")
        if isinstance(fallback, list):
            labeled: list[tuple[int, str]] = []
            for item in fallback:
                if not isinstance(item, dict):
                    continue
                file_url = item.get("file")
                if not isinstance(file_url, str):
                    continue
                label = item.get("label")
                rank = 0
                if label is not None:
                    s = str(label).strip().rstrip("pP")
                    if s.isdigit():
                        rank = int(s)
                labeled.append((rank, file_url))
            for _, file_url in sorted(labeled, key=lambda t: (-t[0], t[1])):
                add(file_url)
        for key in ("direct_access_url", "mp4", "file"):
            val = payload.get(key)
            if isinstance(val, str):
                add(val)
        sources = payload.get("sources")
        if isinstance(sources, list):
            for item in sources:
                if isinstance(item, dict):
                    add(item.get("file"))
        playlist = payload.get("playlist")
        if isinstance(playlist, list):
            for item in playlist:
                if isinstance(item, dict):
                    add(item.get("file"))
                    nested = item.get("sources")
                    if isinstance(nested, list):
                        for sub in nested:
                            if isinstance(sub, dict):
                                add(sub.get("file"))
    elif isinstance(payload, str):
        for m in re.finditer(
            r"https?://[^\s\"']+\.(?:m3u8|m3u|mpd|mp4|mkv|webm)(?:\?[^\s\"']*)?",
            payload,
            re.I,
        ):
            add(m.group(0))
    return found


def extract_embed_page_urls(html: str, page_url: str) -> list[str]:
    """Collect external player/embed URLs from data-embed attributes and iframes."""
    if not html:
        return []
    seen: set[str] = set()
    out: list[str] = []

    def consider(raw: str) -> None:
        raw = (raw or "").strip()
        if not raw or "${" in raw or "javascript:" in raw.lower():
            return
        u = page_media.join_media_url(page_url, raw)
        if not u or u in seen:
            return
        host = (urlparse(u).hostname or "").lower()
        if host.endswith(("a-ads.com", "doubleclick.net", "googlesyndication.com")):
            return
        seen.add(u)
        out.append(u)

    for m in _EMBED_URL_RE.finditer(html):
        consider(m.group("url"))
    for m in _IFRAME_SRC_RE.finditer(html):
        consider(m.group("url"))
    return out


def extract_jwplayer_media_urls(
    html: str,
    page_url: str,
    *,
    extensions: list[str] | None = None,
    bait_substrings: tuple[str, ...] | None = None,
) -> list[str]:
    """
    Return direct stream/file URLs from JW Player setup or VOE obfuscated JSON.
    """
    if not html:
        return []
    exts = (
        extensions if extensions else list(page_media.DEFAULT_FALLBACK_MEDIA_EXTENSIONS)
    )
    raw_candidates: list[str] = []

    if page_uses_jwplayer(html) or _APP_JSON_SCRIPT_RE.search(html):
        for m in _FILE_IN_SETUP_RE.finditer(html):
            raw_candidates.append(m.group("url"))
        for m in _VAR_SOURCE_RE.finditer(html):
            raw_candidates.append(m.group("url"))
        for m in _APP_JSON_SCRIPT_RE.finditer(html):
            body = (m.group("body") or "").strip()
            if not body:
                continue
            decoded = deobfuscate_embedded_json(body)
            if decoded is not None:
                raw_candidates.extend(
                    _urls_from_decoded_payload(decoded, bait_substrings=bait_substrings)
                )

    seen: set[str] = set()
    scored: list[tuple[int, int, str]] = []
    seq = 0
    for raw in raw_candidates:
        if _is_bait_url(raw, bait_substrings):
            continue
        u = page_media.join_media_url(page_url, raw)
        if not u or u in seen:
            continue
        pr = page_media.media_priority(u, exts)
        if pr is None:
            continue
        seen.add(u)
        scored.append((pr, seq, u))
        seq += 1

    scored.sort(key=lambda t: (t[0], t[1]))
    out: list[str] = []
    seen.clear()
    for _, __, u in scored:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return page_media.sort_media_urls_by_quality(out)


def extract_jwplayer_with_embeds(
    html: str,
    page_url: str,
    fetch_html,
    *,
    extensions: list[str] | None = None,
    follow_embeds: bool = True,
    max_embed_depth: int = 1,
    bait_substrings: tuple[str, ...] | None = None,
) -> list[str]:
    """
    Extract JW Player URLs from page and optionally one level of embed pages.

    fetch_html(url) -> str | None
    """
    merged: list[str] = []
    seen: set[str] = set()

    def add_many(urls: list[str]) -> None:
        for u in urls:
            if u not in seen:
                seen.add(u)
                merged.append(u)

    add_many(
        extract_jwplayer_media_urls(
            html,
            page_url,
            extensions=extensions,
            bait_substrings=bait_substrings,
        )
    )
    if not follow_embeds or max_embed_depth < 1:
        return page_media.sort_media_urls_by_quality(merged)

    for embed_url in extract_embed_page_urls(html, page_url):
        if embed_url == page_url:
            continue
        host = (urlparse(embed_url).hostname or "").lower()
        if not host:
            continue
        child_html = fetch_html(embed_url)
        if not child_html:
            continue
        add_many(
            extract_jwplayer_media_urls(
                child_html,
                embed_url,
                extensions=extensions,
                bait_substrings=bait_substrings,
            )
        )
    return page_media.sort_media_urls_by_quality(merged)
