"""
Resolve direct stream URLs from pages that use external embed players (not JW-only).
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from hdl import jwplayer_media, page_media

_EMBED_HINT_RE = re.compile(r"data-embed\s*=|change-video|lazy-v", re.I)

# Hosts that usually need an embed hop (page HTML has no direct stream).
_EMBED_FIRST_HOSTS_DEFAULT = ("sakuhentai.net",)

# Prefer these embed CDNs when multiple players are listed (hglink is JS-only).
_EMBED_HOST_PRIORITY = (
    "natsumi.fun",
    "voe.sx",
    "vickisaveworker.com",
    "cloudwindow-route.com",
    "hglink.to",
)


def _host_of(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def is_plausible_embed_url(url: str, page_url: str) -> bool:
    raw = (url or "").strip()
    if not raw or "${" in raw or "javascript:" in raw.lower():
        return False
    if not raw.startswith(("http://", "https://")):
        return False
    host = _host_of(url)
    if not host:
        return False
    page_host = _host_of(page_url)
    if page_host and host == page_host and "/e/" not in raw and "embed" not in raw.lower():
        return False
    if host.endswith(("a-ads.com", "doubleclick.net", "googlesyndication.com")):
        return False
    return True


def prioritize_embed_urls(embeds: list[str]) -> list[str]:
    def rank(u: str) -> tuple[int, str]:
        h = _host_of(u)
        for i, pref in enumerate(_EMBED_HOST_PRIORITY):
            if h == pref or h.endswith("." + pref):
                return (i, u)
        return (len(_EMBED_HOST_PRIORITY), u)

    return sorted(dict.fromkeys(embeds), key=rank)


def page_needs_embed_resolve(html: str, page_url: str, *, embed_first_hosts: tuple[str, ...]) -> bool:
    if not html:
        return False
    if _EMBED_HINT_RE.search(html):
        return True
    host = _host_of(page_url)
    return any(host == h or host.endswith("." + h) for h in embed_first_hosts)


def resolve_stream_targets(
    page_url: str,
    html: str,
    fetch_html,
    *,
    extensions: list[str] | None = None,
    extra_regexes: list[str] | None = None,
    follow_embeds: bool = True,
    max_embed_depth: int = 1,
    bait_substrings: tuple[str, ...] | None = None,
) -> list[tuple[str, str]]:
    """
    Return (stream_url, referer) pairs sorted best-quality first.
    referer is the embed page URL when streams came from an external player.
    """
    if not html:
        return []

    exts = extensions if extensions else list(page_media.DEFAULT_FALLBACK_MEDIA_EXTENSIONS)
    extras = extra_regexes if extra_regexes is not None else []
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add_streams(urls: list[str], referer: str) -> None:
        for u in page_media.sort_media_urls_by_quality(urls):
            if u not in seen:
                seen.add(u)
                out.append((u, referer))

    # Direct streams on the page (rare for embed sites).
    direct = page_media.extract_fallback_media_urls(
        html, page_url, extensions=exts, extra_regexes=extras
    )
    add_streams(direct, page_url)

    embeds = [
        u
        for u in jwplayer_media.extract_embed_page_urls(html, page_url)
        if is_plausible_embed_url(u, page_url)
    ]
    embeds = prioritize_embed_urls(embeds)

    if follow_embeds and max_embed_depth >= 1:
        for embed_url in embeds:
            child_html = fetch_html(embed_url)
            if not child_html:
                continue
            jw_urls = jwplayer_media.extract_jwplayer_media_urls(
                child_html,
                embed_url,
                extensions=exts,
                bait_substrings=bait_substrings,
            )
            if jw_urls:
                add_streams(jw_urls, embed_url)
                continue
            # Non-JW embed page: generic scrape on embed HTML only.
            generic = page_media.extract_fallback_media_urls(
                child_html, embed_url, extensions=exts, extra_regexes=extras
            )
            if generic:
                add_streams(generic, embed_url)
    elif embeds:
        for embed_url in embeds:
            if embed_url not in seen:
                seen.add(embed_url)
                out.append((embed_url, page_url))

    return out
