from __future__ import annotations

import re
from pathlib import Path
from hdl.config import AppConfig


def is_hstream_url(url: str, cfg: AppConfig) -> bool:
    u = url.lower().strip()
    for marker in cfg.h["url_markers"]:
        if marker.lower() in u:
            return True
    return False


def hstream_video_name_from_url(url: str, cfg: AppConfig) -> str:
    name = url.rstrip("/").split("/")[-1]
    name = re.sub(r'[<>:"/\\|?*]', "_", name).strip()
    return name or str(cfg.h["fallback_video_name"])


def find_existing_hstream_video(
    cfg: AppConfig,
    video_name: str,
    output_format: str = "mp4",
) -> Path | None:
    exts = [output_format, *[e.lstrip(".") for e in cfg.h["video_extensions"]]]
    seen: set[str] = set()
    ordered_exts: list[str] = []
    for ext in exts:
        clean = str(ext).strip().lstrip(".").lower()
        if clean and clean not in seen:
            seen.add(clean)
            ordered_exts.append(clean)
    for base_dir in cfg.skip_if_exists_dirs:
        for ext in ordered_exts:
            candidate = base_dir / f"{video_name}.{ext}"
            if candidate.exists() and candidate.stat().st_size > 0:
                return candidate
    return None


def load_urls_from_list(
    cfg: AppConfig,
    console,
    path: Path | None = None,
    *,
    hstream_only: bool = False,
) -> list[str]:
    path = path or cfg.list_file
    if not path.exists():
        console.print(cfg.text("urls", "list_missing", path=str(path)))
        return []
    sub = cfg.h["list_filter_substring"]
    comment_p = str(cfg.ui.get("comment_prefix", "#"))
    prefixes = list(cfg.ui.get("url_line_prefixes", ("http://", "https://")))
    seen: set[str] = set()
    out: list[str] = []
    for line in path.read_text("utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith(comment_p):
            continue
        if not any(line.startswith(p) for p in prefixes):
            continue
        if hstream_only and sub not in line:
            continue
        if line not in seen:
            out.append(line)
            seen.add(line)
    return out

