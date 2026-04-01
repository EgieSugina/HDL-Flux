"""
Netscape-format cookie files for yt-dlp / requests (MozillaCookieJar).
"""
from __future__ import annotations

import time
from http.cookiejar import Cookie, MozillaCookieJar
from pathlib import Path


def netscape_cookie_file_loads(path: Path) -> bool:
    """Return True if path looks like a valid Netscape cookie jar yt-dlp can read."""
    if not path.is_file() or path.stat().st_size == 0:
        return False
    try:
        jar = MozillaCookieJar(str(path))
        jar.load(ignore_discard=True, ignore_expires=True)
        return True
    except Exception:
        return False


def write_netscape_from_browser_cookies(cookies: list[dict], path: Path) -> None:
    """
    Write Selenium-style cookie dicts to a Netscape cookie file (yt-dlp compatible).
    Sanitizes tab/newline in values.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    jar = MozillaCookieJar(str(path))
    now = int(time.time())
    for c in cookies:
        domain = str(c.get("domain") or "").strip()
        if not domain:
            continue
        name = str(c.get("name", ""))
        raw_val = str(c.get("value", ""))
        value = raw_val.replace("\n", "").replace("\r", "").replace("\t", " ")
        path_s = str(c.get("path") or "/").strip() or "/"
        secure = bool(c.get("secure", False))
        exp = c.get("expiry")
        if exp is None:
            expires = now + 86400 * 365
        else:
            try:
                expires = int(float(exp))
            except (TypeError, ValueError):
                expires = now + 86400 * 365
        domain_initial_dot = domain.startswith(".")
        ck = Cookie(
            0,
            name,
            value,
            None,
            False,
            domain,
            True,
            domain_initial_dot,
            path_s,
            True,
            secure,
            expires,
            False,
            None,
            None,
            {},
            False,
        )
        jar.set_cookie(ck)
    jar.save(ignore_discard=True, ignore_expires=True)
