"""
Shared HTML scraping for direct stream / file URLs (m3u8, mpd, mp4, …).
Used by hstream and generic download paths.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

DEFAULT_FALLBACK_MEDIA_EXTENSIONS = ["m3u8", "m3u", "mpd", "mp4", "mkv", "webm"]


def join_media_url(page_url: str, raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw or raw.startswith(("#", "javascript:", "data:", "about:")):
        return None
    if raw.startswith("//"):
        scheme = urlparse(page_url).scheme or "https"
        raw = f"{scheme}:{raw}"
    joined = urljoin(page_url, raw)
    if not joined.startswith(("http://", "https://")):
        return None
    return joined


def media_priority(url: str, extensions: list[str]) -> int | None:
    """Lower sorts earlier: HLS/DASH playlists first, then direct files."""
    path = url.split("?", 1)[0].lower()
    dotted = {"." + str(x).lower().lstrip(".") for x in extensions}
    streamish = ({".m3u8", ".m3u", ".mpd"} & dotted) or {".m3u8", ".m3u"}
    for ext in sorted(streamish, key=len, reverse=True):
        if path.endswith(ext):
            return 0
    direct = dotted - {".m3u8", ".m3u", ".mpd"}
    for ext in sorted(direct, key=len, reverse=True):
        if path.endswith(ext):
            return 1
    return None


def extract_fallback_media_urls(
    html: str,
    page_url: str,
    *,
    extensions: list[str] | None = None,
    extra_regexes: list[str] | None = None,
) -> list[str]:
    if not html:
        return []
    exts = extensions if extensions else list(DEFAULT_FALLBACK_MEDIA_EXTENSIONS)
    extras = extra_regexes if extra_regexes is not None else []
    seen: set[str] = set()
    scored: list[tuple[int, int, str]] = []
    seq = 0

    def consider(raw: str) -> None:
        nonlocal seq
        u = join_media_url(page_url, raw)
        if not u or u in seen:
            return
        pr = media_priority(u, exts)
        if pr is None:
            return
        seen.add(u)
        scored.append((pr, seq, u))
        seq += 1

    for m in re.finditer(
        r'<(?:video|source)\b[^>]*\bsrc\s*=\s*["\']([^"\']+)["\']',
        html,
        re.I,
    ):
        consider(m.group(1))
    for m in re.finditer(r'(?:\bsrc|\bhref)\s*=\s*["\']([^"\']+)["\']', html, re.I):
        consider(m.group(1))
    ext_re = "|".join(re.escape(str(x).lstrip(".").lower()) for x in exts)
    if ext_re:
        for m in re.finditer(
            rf'https?://[^\s"\'<>]+?\.(?:{ext_re})(?:\?[^\s"\'<>]*)?',
            html,
            re.I,
        ):
            consider(m.group(0))
    for pat in extras:
        if not isinstance(pat, str) or not pat.strip():
            continue
        try:
            for m in re.finditer(pat, html):
                if not m.groups():
                    continue
                consider(m.group(1))
        except re.error:
            continue
    scored.sort(key=lambda t: (t[0], t[1]))
    out: list[str] = []
    seen.clear()
    for _, __, u in scored:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out
