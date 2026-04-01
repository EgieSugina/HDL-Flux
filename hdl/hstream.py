from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import unquote, urljoin

import requests

from hdl import cookiefile as cookiefile_util
from hdl import page_media, routing
from hdl.browser import SELENIUM_AVAILABLE, YTDLP_AVAILABLE, create_selenium_driver
from hdl.config import AppConfig

try:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
except ImportError:  # pragma: no cover
    By = None
    WebDriverWait = None
    EC = None
    TimeoutException = Exception

try:
    import yt_dlp
except ImportError:  # pragma: no cover
    yt_dlp = None


class HStreamDownloader:
    def __init__(
        self,
        cfg: AppConfig,
        console,
        *,
        quality: str,
        fmt: str,
        framerate: int | None = None,
        use_browser: bool = True,
        headless: bool = True,
        prefer_ytdlp: bool = True,
        max_retries: int | None = None,
        delete_chunks: bool = True,
        ffmpeg_cuda_mode: str | None = None,
        proxy=None,
    ):
        self.cfg = cfg
        self.console = console
        h = cfg.h
        self.quality = quality
        self.fmt = fmt
        self.framerate = int(framerate if framerate is not None else h["framerate"])
        self.use_browser = use_browser and SELENIUM_AVAILABLE
        self.headless = headless
        self.prefer_ytdlp = prefer_ytdlp and YTDLP_AVAILABLE
        self.max_retries = int(max_retries if max_retries is not None else h["max_retries"])
        self.delete_chunks = delete_chunks
        self.ffmpeg_cuda_mode = ffmpeg_cuda_mode
        self.proxy = proxy
        self._err_max = int(cfg.g["error_message_max_len"])

        self.session = requests.Session()
        ua = h["user_agent_linux"] if sys.platform.startswith("linux") else h["user_agent_windows"]
        self.session.headers.update({"User-Agent": ua, "Referer": h["referer"], "Origin": h["origin"]})
        if self.proxy:
            self.session.proxies.update({"http": self.proxy, "https": self.proxy})
        self._driver = None
        self._browser_lock = threading.Lock()
        self._browser_kind: str | None = None
        self._cuda_encoder_available: bool | None = None

    def _quality_res(self) -> dict[str, tuple[int, int]]:
        return self.cfg.hstream_quality_res()

    def _init_browser(self) -> bool:
        if not SELENIUM_AVAILABLE or not self.use_browser:
            return False
        if self._driver is None:
            drv, used = create_selenium_driver(self.headless, self.cfg)
            if drv is None:
                self.use_browser = False
                return False
            self._driver = drv
            self._browser_kind = used
            return True
        return True

    def close_browser(self):
        with self._browser_lock:
            if self._driver:
                try:
                    self._driver.quit()
                except Exception:
                    pass
                self._driver = None
                self._browser_kind = None

    @staticmethod
    def load_credentials(cfg: AppConfig) -> tuple[str, str] | None:
        path = cfg.credentials_file
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text("utf-8"))
            email = data.get("email", "").strip()
            pw = data.get("password", "").strip()
            if email and pw:
                return email, pw
        except (json.JSONDecodeError, OSError):
            pass
        return None

    def login(self, email: str, password: str) -> bool:
        h = self.cfg.h
        login_contains = h["login_url_path_contains"]
        sel = h["selenium_selectors"]
        if not SELENIUM_AVAILABLE:
            self.console.print(f"  [red]{self.cfg.h_msg('selenium_required_login')}[/]")
            return False
        with self._browser_lock:
            if not self._init_browser():
                self.console.print(f"  [red]{self.cfg.h_msg('browser_start_failed')}[/]")
                return False
            try:
                base = h["base_url"].rstrip("/")
                self._driver.get(base + h["login_path"])
                time.sleep(float(h["page_load_wait_sec"]))
                wait_sec = float(h["login_field_timeout_sec"])
                email_input = WebDriverWait(self._driver, wait_sec).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, sel["email"]))
                )
                email_input.clear()
                email_input.send_keys(email)
                pw_input = self._driver.find_element(By.CSS_SELECTOR, sel["password"])
                pw_input.clear()
                pw_input.send_keys(password)
                try:
                    btn = self._driver.find_element(By.CSS_SELECTOR, sel["submit"])
                    btn.click()
                except Exception:
                    from selenium.webdriver.common.keys import Keys

                    pw_input.send_keys(Keys.RETURN)
                n_iter = int(h["login_post_submit_max_iterations"])
                poll = float(h["login_post_submit_poll_sec"])
                for _ in range(n_iter):
                    time.sleep(poll)
                    if login_contains not in self._driver.current_url:
                        break
                if login_contains in self._driver.current_url:
                    self.console.print(f"  [red]{self.cfg.h_msg('login_still_on_page')}[/]")
                    return False
                self._sync_cookies()
                return True
            except TimeoutException:
                try:
                    self._driver.execute_script("window.stop();")
                except Exception:
                    pass
                self.console.print(f"  [red]{self.cfg.h_msg('login_timeout')}[/]")
                return False
            except Exception as exc:
                self.console.print(f"  [red]{self.cfg.h_msg('login_error', detail=exc)}[/]")
                return False

    def _sync_cookies(self):
        if not self._driver:
            return
        browser_cookies = self._driver.get_cookies()
        for c in browser_cookies:
            self.session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
        try:
            cookiefile_util.write_netscape_from_browser_cookies(
                browser_cookies, self.cfg.cookies_file
            )
        except OSError:
            pass

    def _quality_from_url(self, url: str) -> str:
        m = re.search(r"/(\d{3,4})/manifest\.mpd(?:[?#]|$)", url)
        if m:
            return m.group(1)
        return "0"

    def _extract_mpd_urls(self, html: str) -> dict[str, str]:
        """
        Extract MPD URLs from page source.
        Uses primary configured regex, then broader fallbacks so pages that
        don't include `size=` still work.
        """
        out: dict[str, str] = {}
        primary = str(self.cfg.h.get("mpd_source_regex", "")).strip()
        if primary:
            for url, size in re.findall(primary, html):
                u = (url or "").strip()
                s = str(size).strip() or self._quality_from_url(u)
                if u:
                    out[s] = u
        # Fallback 1: any src/href ending with manifest.mpd
        for m in re.finditer(
            r'(?:\bsrc|\bhref)\s*=\s*["\']([^"\']+manifest\.mpd(?:\?[^"\']*)?)["\']',
            html,
            re.I,
        ):
            u = m.group(1).strip()
            if not u:
                continue
            q = self._quality_from_url(u)
            out.setdefault(q, u)
        # Fallback 2: plain absolute URL in inline scripts
        for m in re.finditer(
            r'https?://[^\s"\'<>]+manifest\.mpd(?:\?[^\s"\'<>]*)?',
            html,
            re.I,
        ):
            u = m.group(0).strip()
            if not u:
                continue
            q = self._quality_from_url(u)
            out.setdefault(q, u)
        return out

    def _extract_subtitle_urls(self, html: str, page_url: str) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for m in re.finditer(
            r'(?:\bsrc|\bhref)\s*=\s*["\']([^"\']+\.(?:ass|srt|vtt)(?:\?[^"\']*)?)["\']',
            html,
            re.I,
        ):
            u = urljoin(page_url, m.group(1).strip())
            if u.startswith(("http://", "https://")) and u not in seen:
                seen.add(u)
                out.append(u)
        for m in re.finditer(
            r'https?://[^\s"\'<>]+?\.(?:ass|srt|vtt)(?:\?[^\s"\'<>]*)?',
            html,
            re.I,
        ):
            u = m.group(0).strip()
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    def _extract_episode_id(self, html: str) -> str | None:
        m = re.search(
            r'<input[^>]*\bid\s*=\s*["\']e_id["\'][^>]*\bvalue\s*=\s*["\'](\d+)["\']',
            html,
            re.I,
        )
        if not m:
            return None
        v = m.group(1).strip()
        return v or None

    def _resolve_mpd_urls_via_api(
        self, html: str, page_url: str, on_status=None
    ) -> tuple[dict[str, str], list[str], list[str]]:
        """
        Resolve MPD candidates from /player/api using hidden input #e_id.
        Returns:
          - map quality->mpd_url
          - ordered mpd candidates (multi-domain)
          - subtitle urls from api payload
        """
        h = self.cfg.h
        eid = self._extract_episode_id(html)
        if not eid:
            return {}, [], []
        api_url = h["base_url"].rstrip("/") + "/player/api"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": page_url,
            "Origin": h["origin"],
        }
        xsrf = self.session.cookies.get("XSRF-TOKEN", "")
        if xsrf:
            headers["X-XSRF-TOKEN"] = unquote(xsrf)
        try:
            if on_status:
                on_status("player api resolve")
            with self.session.post(
                api_url,
                headers=headers,
                json={"episode_id": str(eid)},
                timeout=float(h["request_timeout_sec"]),
            ) as resp:
                resp.raise_for_status()
                data = resp.json()
            if not isinstance(data, dict):
                return {}, [], []
            stream_url = str(data.get("stream_url", "")).strip().strip("/")
            domains: list[str] = []
            for key in ("stream_domains", "asia_stream_domains"):
                vals = data.get(key) or []
                if isinstance(vals, list):
                    for d in vals:
                        dom = str(d).strip().rstrip("/")
                        if dom and dom not in domains:
                            domains.append(dom)
            qkeys = sorted(
                [str(k) for k in self._quality_res().keys()],
                key=lambda z: int(z),
                reverse=True,
            )
            mpd_candidates: list[str] = []
            mpd_map: dict[str, str] = {}
            if stream_url and domains and qkeys:
                for q in qkeys:
                    q_candidates = [
                        f"{dom}/{stream_url}/{q}/manifest.mpd" for dom in domains
                    ]
                    for u in q_candidates:
                        if u not in mpd_candidates:
                            mpd_candidates.append(u)
                    if q_candidates and q not in mpd_map:
                        mpd_map[q] = q_candidates[0]
            sub_urls: list[str] = []
            extras = data.get("extra_subtitles") or []
            if isinstance(extras, list):
                for raw in extras:
                    s = str(raw).strip()
                    if not s:
                        continue
                    u = s if s.startswith(("http://", "https://")) else urljoin(page_url, s)
                    if u not in sub_urls:
                        sub_urls.append(u)
            if on_status and mpd_candidates:
                on_status(f"player api found {len(mpd_candidates)} mpd candidate(s)")
            return mpd_map, mpd_candidates, sub_urls
        except Exception:
            return {}, [], []

    def _rank_mpd_candidates(self, candidates: list[str], on_status=None) -> list[str]:
        """
        Probe MPD candidates quickly and sort by latency.
        Unreachable candidates are kept at the end (still usable as fallback).
        """
        if not candidates:
            return []
        req_t = max(1.0, min(float(self.cfg.h["request_timeout_sec"]), 6.0))
        scored: list[tuple[float, str]] = []
        if on_status and len(candidates) > 1:
            on_status(f"mpd health-check {len(candidates)} candidate(s)")
        for u in candidates:
            t0 = time.monotonic()
            ok = False
            try:
                with self.session.get(u, timeout=req_t, stream=True) as resp:
                    ok = resp.status_code == 200
            except Exception:
                ok = False
            dt = time.monotonic() - t0
            scored.append((dt if ok else 9999.0, u))
        scored.sort(key=lambda x: x[0])
        ranked = [u for _, u in scored]
        if on_status and ranked:
            on_status("mpd health-check done")
        return ranked

    def _get_page_html(self, page_url: str, on_status=None) -> tuple[str | None, str]:
        """
        Load the episode page and return (html, source) where source is
        \"browser\", \"http\", or \"\" if both paths failed.
        """
        h = self.cfg.h
        vtag = h["selenium_video_tag"]
        tmo = float(h["video_wait_timeout_sec"])
        aw = float(h["after_video_element_wait_sec"])
        req_t = float(h["request_timeout_sec"])
        if self.use_browser:
            with self._browser_lock:
                if self._init_browser():
                    try:
                        if on_status:
                            on_status("browser fetch page")
                        self._driver.get(page_url)
                        try:
                            WebDriverWait(self._driver, tmo).until(EC.presence_of_element_located((By.TAG_NAME, vtag)))
                            time.sleep(aw)
                        except TimeoutException:
                            pass
                        try:
                            self._sync_cookies()
                        except Exception:
                            pass
                        return (self._driver.page_source, "browser")
                    except TimeoutException:
                        if on_status:
                            on_status("browser timeout, fallback http")
                        try:
                            self._driver.execute_script("window.stop();")
                        except Exception:
                            pass
                    except Exception:
                        if on_status:
                            on_status("browser failed, fallback http")
        try:
            if on_status:
                on_status("http fetch page")
            with self.session.get(page_url, timeout=req_t) as resp:
                resp.raise_for_status()
                return (resp.text, "http")
        except Exception:
            return (None, "")

    def get_mpd_urls(self, page_url: str, on_status=None) -> dict[str, str] | None:
        html, src = self._get_page_html(page_url, on_status)
        if not html:
            return None
        mpd = self._extract_mpd_urls(html)
        if mpd and on_status and src:
            on_status(f"mpd found via {src}")
        elif mpd and on_status:
            on_status("mpd found")
        return mpd or None

    def _ytdlp_fmt(self, fallback=False) -> str:
        yd = self.cfg.ytdlp_cfg()
        if fallback:
            return str(yd["format_fallback_chain"])
        qmap = self._quality_res()
        lab = self.quality if self.quality in qmap else max(qmap.keys(), key=lambda z: int(z))
        return str(yd["format_height_template"]).format(h=lab)

    def _download_ytdlp(
        self,
        page_url,
        media_candidates: list[str],
        name,
        workdir,
        on_progress,
        on_status=None,
        on_transfer=None,
    ):
        h = self.cfg.h
        # Try each manifest/stream URL first, then the episode page (often unsupported alone).
        seen: set[str] = set()
        urls_to_try: list[str] = []
        for u in media_candidates:
            s = (u or "").strip()
            if s and s not in seen:
                seen.add(s)
                urls_to_try.append(s)
        pu = (page_url or "").strip()
        if pu and pu not in seen and bool(h.get("ytdlp_append_episode_page", False)):
            urls_to_try.append(pu)
        last_err = ""
        min_out = int(h["min_output_bytes"])
        yd = self.cfg.ytdlp_cfg()
        ybest = str(yd["format_best_single"])
        usub = str(yd["unsupported_url_substring"]).lower()
        fsub = str(yd["format_error_substring"]).lower()
        for target_url in urls_to_try:
            for attempt in range(1, self.max_retries + 1):
                if attempt > 1:
                    if on_status:
                        on_status(f"yt-dlp retry {attempt}/{self.max_retries}")
                    self._clean_partials(workdir)
                    mult = int(h["ydl_retry_backoff_multiplier_sec"])
                    cap = int(h["ydl_retry_backoff_cap_sec"])
                    time.sleep(min(mult * attempt, cap))
                try:
                    ydl_opts = {
                        "outtmpl": str(Path(workdir) / name) + ".%(ext)s",
                        "quiet": True,
                        "no_warnings": True,
                        "nocheckcertificate": True,
                        "http_headers": dict(self.session.headers),
                        "http_chunk_size": int(h["ydl_http_chunk_size"]),
                        "noplaylist": True,
                        "socket_timeout": int(h["ydl_socket_timeout"]),
                        "retries": int(h["ydl_inner_retries"]),
                        "fragment_retries": int(h["ydl_fragment_retries"]),
                        "file_access_retries": int(h["ydl_file_access_retries"]),
                    }
                    if self.proxy:
                        ydl_opts["proxy"] = self.proxy
                    cf = self.cfg.cookies_file
                    if cf.exists() and cookiefile_util.netscape_cookie_file_loads(cf):
                        ydl_opts["cookiefile"] = str(cf)
                    if on_progress:

                        def _hook(d, _cb=on_progress):
                            if d["status"] == "downloading":
                                tot = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                                got = d.get("downloaded_bytes", 0)
                                if tot > 0:
                                    _cb(got / tot * 100)
                                if on_transfer:
                                    on_transfer(got, tot, d.get("speed"))
                            elif d["status"] == "finished":
                                _cb(100)
                                if on_transfer:
                                    fin = d.get("total_bytes") or d.get("downloaded_bytes", 0)
                                    on_transfer(fin, fin, d.get("speed"))

                        ydl_opts["progress_hooks"] = [_hook]
                    if self.fmt in self.cfg.h["container_formats_postprocess"]:
                        ydl_opts["postprocessors"] = [{"key": str(yd["ffmpeg_convertor_key"]), "preferedformat": self.fmt}]
                    for fmt_str in (self._ytdlp_fmt(), self._ytdlp_fmt(True), ybest):
                        try:
                            ydl_opts["format"] = fmt_str
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                ydl.download([target_url])
                            break
                        except Exception as exc:
                            el = str(exc).lower()
                            if fsub in el and fmt_str != ybest:
                                continue
                            raise
                    out = self._find_video(workdir, name)
                    if out and os.path.getsize(out) > min_out:
                        return True, ""
                    if attempt >= self.max_retries:
                        last_err = self.cfg.h_msg("ytdlp_output_too_small")
                        break
                except Exception as exc:
                    last_err = str(exc)[: self._err_max]
                    if usub in last_err.lower():
                        # Unsupported URL: stop yt-dlp path and fallback to chunk flow.
                        return False, last_err
                    if attempt >= self.max_retries:
                        break
        return False, last_err

    def _parse_mpd(self, mpd_url):
        try:
            with self.session.get(
                mpd_url,
                timeout=float(self.cfg.h["request_timeout_sec"]),
            ) as resp:
                resp.raise_for_status()
                content = resp.content
            root = ET.fromstring(content)
            base = mpd_url.rsplit("/", 1)[0] + "/"
            info = {"video": [], "audio": [], "base": base}
            for ada in root.iter():
                if not ada.tag.endswith("AdaptationSet"):
                    continue
                ctype = (ada.get("contentType", "") + ada.get("mimeType", "")).lower()
                is_vid = "video" in ctype
                is_aud = "audio" in ctype
                for rep in ada:
                    if not rep.tag.endswith("Representation"):
                        continue
                    seg = None
                    for child in rep:
                        if child.tag.endswith("SegmentTemplate"):
                            seg = child
                            break
                    if seg is None:
                        for child in ada:
                            if child.tag.endswith("SegmentTemplate"):
                                seg = child
                                break
                    if seg is None:
                        continue
                    media = seg.get("media", "")
                    start = int(seg.get("startNumber", 1))
                    rep_id = str(rep.get("id", ""))
                    try:
                        rep_bw = int(rep.get("bandwidth", 0))
                    except (TypeError, ValueError):
                        rep_bw = 0
                    try:
                        rep_h = int(rep.get("height", 0))
                    except (TypeError, ValueError):
                        rep_h = 0
                    timeline = None
                    for ch in seg:
                        if ch.tag.endswith("SegmentTimeline"):
                            timeline = ch
                            break
                    if timeline is None:
                        continue
                    count = 0
                    for s_el in timeline:
                        if s_el.tag.endswith("S"):
                            count += int(s_el.get("r", 0)) + 1
                    entry = {
                        "media": media,
                        "start": start,
                        "count": count,
                        "rep_id": rep_id,
                        "bandwidth": rep_bw,
                        "height": rep_h,
                    }
                    if is_vid:
                        info["video"].append(entry)
                    elif is_aud:
                        info["audio"].append(entry)
            if info["video"]:
                info["video"].sort(
                    key=lambda x: (
                        int(x.get("height", 0)),
                        int(x.get("bandwidth", 0)),
                    ),
                    reverse=True,
                )
            return info
        except Exception:
            return None

    def _expand_segment_template(self, template: str, number: int, rep_id: str) -> str:
        s = template
        # Common MPD placeholders
        s = s.replace("$RepresentationID$", rep_id)
        s = re.sub(r"\$RepresentationID%0\d+d\$", rep_id, s)
        s = s.replace("$Number$", str(number))
        s = s.replace("$Number%05d$", f"{number:05d}")
        s = re.sub(r"\$Number%0(\d+)d\$", lambda m: f"{number:0{int(m.group(1))}d}", s)
        return s

    def _download_chunk(self, url, filepath, retries=None) -> bool:
        h = self.cfg.h
        if retries is None:
            retries = int(h["chunk_retry_default"])
        req_t = float(h["request_timeout_sec"])
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return True
        for _ in range(retries):
            try:
                with self.session.get(url, timeout=req_t) as resp:
                    resp.raise_for_status()
                    content = resp.content
                if not content:
                    continue
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                Path(filepath).write_bytes(content)
                if os.path.getsize(filepath) > 0:
                    return True
            except Exception:
                time.sleep(float(h["chunk_http_fail_sleep_sec"]))
        return False

    def _download_chunks(self, mpd_url, workdir, name, on_progress, on_status=None):
        h = self.cfg.h
        if on_status:
            on_status("chunks parse mpd")
        mpd = self._parse_mpd(mpd_url)
        if not mpd or not mpd.get("video"):
            return False, self.cfg.h_msg("mpd_parse_failed")
        vi = mpd["video"][0]
        base = mpd["base"]
        tmpl = h["chunk_filename_template"]
        workers = int(h["chunk_download_workers"])
        rfail = int(h["chunk_retry_failed"])
        ratio = float(h["chunk_min_complete_ratio"])
        chunk_list = []
        for i in range(vi["start"], vi["start"] + vi["count"]):
            path = self._expand_segment_template(vi["media"], i, str(vi.get("rep_id", "")))
            chunk_list.append((i, path))
        total = len(chunk_list)
        if on_status:
            on_status(f"chunks downloading {total} segments")
        done_count = 0
        processed_count = 0
        failed = []
        status_step = max(1, total // 10)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {}
            for num, path in chunk_list:
                url = urljoin(base, path)
                fp = os.path.join(workdir, tmpl.format(num=num))
                futs[pool.submit(self._download_chunk, url, fp)] = (num, url, fp)
            for fut in as_completed(futs):
                num, url, fp = futs[fut]
                processed_count += 1
                if fut.result():
                    done_count += 1
                else:
                    failed.append((num, url, fp))
                if on_progress and total:
                    # 0..70%: progress over primary chunk processing.
                    on_progress(processed_count / total * 70)
                if on_status and processed_count % status_step == 0:
                    on_status(f"chunks processed {processed_count}/{total} (ok {done_count})")
        retried = 0
        failed_total = len(failed)
        for num, url, fp in failed:
            if self._download_chunk(url, fp, retries=rfail):
                done_count += 1
            retried += 1
            if on_progress and failed_total:
                # 70..90%: progress over retry pass.
                on_progress(70 + (retried / failed_total * 20))
        if on_status and failed:
            on_status(f"chunks recovered {done_count}/{total}")
        if done_count < total * ratio:
            return False, self.cfg.h_msg("chunks_missing_template", missing=total - done_count, total=total)
        if on_status:
            on_status("chunks converting with ffmpeg")
        if on_progress:
            on_progress(92)
        ok, result = self._convert_chunks(workdir, name)
        if on_progress:
            on_progress(100)
        return ok, result

    def _download_subtitle(self, subtitle_url: str, workdir: str, name: str) -> str | None:
        try:
            ext = subtitle_url.split("?", 1)[0].rsplit(".", 1)[-1].lower()
            if ext not in {"ass", "srt", "vtt"}:
                ext = "ass"
            out_path = os.path.join(workdir, f"{name}.{ext}")
            with self.session.get(subtitle_url, timeout=float(self.cfg.h["request_timeout_sec"])) as resp:
                resp.raise_for_status()
                data = resp.content
            if not data:
                return None
            Path(out_path).write_bytes(data)
            return out_path if os.path.getsize(out_path) > 0 else None
        except Exception:
            return None

    def _embed_subtitle(self, video_path: str, subtitle_path: str) -> tuple[bool, str]:
        hx = self.cfg.h
        ext = Path(video_path).suffix.lower()
        if ext not in {".mp4", ".mkv"}:
            return False, "unsupported container for subtitle embed"
        tmp_out = str(Path(video_path).with_name(Path(video_path).stem + ".subtmp" + ext))
        cmd = [
            hx["ffmpeg_binary"],
            "-y",
            "-i",
            video_path,
            "-i",
            subtitle_path,
            "-map",
            "0",
            "-map",
            "1:0",
            "-c:v",
            "copy",
            "-c:a",
            "copy",
        ]
        if ext == ".mp4":
            cmd += ["-c:s", "mov_text"]
        else:
            # mkv can keep ASS/SRT directly.
            cmd += ["-c:s", "copy"]
        cmd += ["-metadata:s:s:0", "language=eng", tmp_out]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            return False, proc.stderr[-int(hx["stderr_tail_chars"]):]
        try:
            os.replace(tmp_out, video_path)
        except OSError as exc:
            return False, str(exc)
        return True, video_path

    def _convert_chunks(self, workdir, name):
        hx = self.cfg.h
        if not self._ffmpeg_available():
            return False, self.cfg.h_msg("ffmpeg_not_found")
        chunks = sorted(glob.glob(os.path.join(workdir, hx["chunk_glob_pattern"])))
        if not chunks:
            return False, self.cfg.h_msg("no_chunk_files")
        m = re.search(hx["chunk_filename_regex"], os.path.basename(chunks[0]))
        start = int(m.group(1)) if m else 0
        qres = self._quality_res()
        w, hdim = qres.get(self.quality, qres[max(qres.keys(), key=lambda z: int(z))])
        output = os.path.join(workdir, f"{name}.{self.fmt}")
        seg_in = os.path.join(workdir, hx["ffmpeg_input_segment_pattern"])
        cmd = [
            hx["ffmpeg_binary"],
            "-y",
            "-framerate",
            str(self.framerate),
            "-start_number",
            str(start),
            "-i",
            seg_in,
            "-vf",
            f"scale={w}:{hdim}:flags={hx['ffmpeg_scale_filter']}",
        ]
        use_cuda = self._should_use_cuda()
        if use_cuda:
            cmd += [
                "-c:v",
                str(hx.get("ffmpeg_cuda_encoder", "h264_nvenc")),
                "-preset",
                str(hx.get("ffmpeg_cuda_preset", "p5")),
                "-cq",
                str(hx.get("ffmpeg_cuda_cq", hx["ffmpeg_crf"])),
                "-rc:v",
                str(hx.get("ffmpeg_cuda_rc", "vbr")),
                "-b:v",
                str(hx.get("ffmpeg_cuda_bitrate", "0")),
                "-pix_fmt",
                hx["pix_fmt"],
                "-r",
                str(self.framerate),
            ]
        else:
            cmd += [
                "-c:v",
                hx["video_codec"],
                "-preset",
                hx["ffmpeg_preset"],
                "-crf",
                str(hx["ffmpeg_crf"]),
                "-pix_fmt",
                hx["pix_fmt"],
                "-r",
                str(self.framerate),
            ]
        if self.fmt == "mp4":
            cmd += ["-movflags", "+faststart"]
        cmd.append(output)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0 and use_cuda:
            # Fallback to software encoder when NVENC path fails at runtime.
            cpu_cmd = [
                hx["ffmpeg_binary"],
                "-y",
                "-framerate",
                str(self.framerate),
                "-start_number",
                str(start),
                "-i",
                seg_in,
                "-vf",
                f"scale={w}:{hdim}:flags={hx['ffmpeg_scale_filter']}",
                "-c:v",
                hx["video_codec"],
                "-preset",
                hx["ffmpeg_preset"],
                "-crf",
                str(hx["ffmpeg_crf"]),
                "-pix_fmt",
                hx["pix_fmt"],
                "-r",
                str(self.framerate),
            ]
            if self.fmt == "mp4":
                cpu_cmd += ["-movflags", "+faststart"]
            cpu_cmd.append(output)
            proc = subprocess.run(cpu_cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            tail = int(hx["stderr_tail_chars"])
            return False, proc.stderr[-tail:]
        if self.delete_chunks:
            for c in chunks:
                try:
                    os.remove(c)
                except OSError:
                    pass
        return True, output

    def _ffmpeg_available(self) -> bool:
        hx = self.cfg.h
        try:
            return (
                subprocess.run(
                    [hx["ffmpeg_binary"], "-version"],
                    capture_output=True,
                    timeout=float(hx["ffmpeg_version_timeout_sec"]),
                ).returncode
                == 0
            )
        except Exception:
            return False

    def _cuda_encoder_ok(self) -> bool:
        if self._cuda_encoder_available is not None:
            return self._cuda_encoder_available
        hx = self.cfg.h
        enc = str(hx.get("ffmpeg_cuda_encoder", "h264_nvenc"))
        try:
            proc = subprocess.run(
                [hx["ffmpeg_binary"], "-hide_banner", "-encoders"],
                capture_output=True,
                text=True,
                timeout=float(hx["ffmpeg_version_timeout_sec"]),
            )
            txt = f"{proc.stdout}\n{proc.stderr}".lower()
            self._cuda_encoder_available = proc.returncode == 0 and enc.lower() in txt
        except Exception:
            self._cuda_encoder_available = False
        return self._cuda_encoder_available

    def _should_use_cuda(self) -> bool:
        hx = self.cfg.h
        mode = str(self.ffmpeg_cuda_mode if self.ffmpeg_cuda_mode is not None else hx.get("ffmpeg_use_cuda", "auto")).lower()
        if mode in ("0", "false", "no", "off", "disabled"):
            return False
        if mode in ("1", "true", "yes", "on", "enabled"):
            return self._cuda_encoder_ok()
        return self._cuda_encoder_ok()

    def _find_video(self, workdir, name) -> str | None:
        vext = self.cfg.h["video_extensions"]
        expected = os.path.join(workdir, f"{name}.{self.fmt}")
        if os.path.exists(expected) and os.path.getsize(expected) > 0:
            return expected
        for ext in vext:
            for f in glob.glob(os.path.join(workdir, f"*{ext}")):
                if os.path.isfile(f) and os.path.getsize(f) > 0:
                    try:
                        os.rename(f, expected)
                        return expected
                    except OSError:
                        return f
        return None

    def _clean_partials(self, workdir):
        hx = self.cfg.h
        max_b = int(hx["partial_max_bytes"])
        for ext in hx["partial_extensions"]:
            for f in glob.glob(os.path.join(workdir, f"*{ext}")):
                try:
                    if os.path.getsize(f) < max_b:
                        os.remove(f)
                except OSError:
                    pass

    def download_one(self, page_url, on_progress=None, on_status=None, on_transfer=None):
        name = routing.hstream_video_name_from_url(page_url, self.cfg)
        workdir = str(self.cfg.downloads_dir / name)
        os.makedirs(workdir, exist_ok=True)
        if on_status:
            on_status("resolve media")
        html, page_src = self._get_page_html(page_url, on_status=on_status)
        if not html:
            if on_status:
                on_status("page load failed")
            return False, name, self.cfg.h_msg("page_load_failed")
        mpd_urls = self._extract_mpd_urls(html)
        api_mpd_map, api_mpd_candidates, api_subtitles = self._resolve_mpd_urls_via_api(
            html, page_url, on_status=on_status
        )
        for q, u in api_mpd_map.items():
            mpd_urls.setdefault(q, u)
        subtitle_urls = self._extract_subtitle_urls(html, page_url)
        for su in api_subtitles:
            if su not in subtitle_urls:
                subtitle_urls.append(su)
        mpd_all_candidates: list[str] = []
        for u in api_mpd_candidates:
            if u not in mpd_all_candidates:
                mpd_all_candidates.append(u)
        for u in mpd_urls.values():
            if u not in mpd_all_candidates:
                mpd_all_candidates.append(u)
        mpd_all_candidates = self._rank_mpd_candidates(mpd_all_candidates, on_status=on_status)
        if mpd_urls and on_status and page_src:
            on_status(f"mpd found via {page_src}")
        elif mpd_urls and on_status:
            on_status("mpd found")
        hx = self.cfg.h
        fallback_urls = page_media.extract_fallback_media_urls(
            html,
            page_url,
            extensions=list(
                hx.get("fallback_media_extensions") or page_media.DEFAULT_FALLBACK_MEDIA_EXTENSIONS
            ),
            extra_regexes=list(hx.get("fallback_media_extra_regexes") or []),
        )
        if mpd_urls:
            keys = list(mpd_urls.keys())
            selected = mpd_urls.get(self.quality) or mpd_urls[max(keys, key=lambda z: int(z))]
            ytdl_candidates = [selected] + [u for u in mpd_all_candidates if u != selected]
            have_mpd = True
        elif fallback_urls:
            selected = None
            ytdl_candidates = list(fallback_urls)
            have_mpd = False
            if on_status:
                on_status(f"no mpd; trying {len(fallback_urls)} alternate url(s)")
        else:
            if on_status:
                on_status("no mpd or stream url")
            return False, name, self.cfg.h_msg("no_playable_url")
        if self.prefer_ytdlp:
            if on_status:
                on_status("trying yt-dlp")
            ok, err = self._download_ytdlp(
                page_url,
                ytdl_candidates,
                name,
                workdir,
                on_progress,
                on_status=on_status,
                on_transfer=on_transfer,
            )
            if ok:
                out = self._find_video(workdir, name)
                if out and subtitle_urls:
                    if on_status:
                        on_status("embedding subtitle")
                    sub_file = self._download_subtitle(subtitle_urls[0], workdir, name)
                    if sub_file:
                        self._embed_subtitle(out, sub_file)
                return True, name, out or workdir
            if on_status:
                on_status("yt-dlp failed")
            # When yt-dlp mode is selected, do not fallback to chunks.
            return False, name, err or self.cfg.h_msg("ytdlp_output_too_small")
        if not have_mpd:
            if not YTDLP_AVAILABLE:
                return False, name, self.cfg.h_msg("no_playable_url")
            if on_status:
                on_status("chunks need mpd; trying yt-dlp on alternate url")
            ok, err = self._download_ytdlp(
                page_url,
                ytdl_candidates,
                name,
                workdir,
                on_progress,
                on_status=on_status,
                on_transfer=on_transfer,
            )
            if ok:
                out = self._find_video(workdir, name)
                return True, name, out or workdir
            return False, name, err or self.cfg.h_msg("ytdlp_output_too_small")
        last_err = ""
        for attempt in range(1, self.max_retries + 1):
            if on_status and attempt == 1:
                on_status("trying chunks")
            if attempt > 1 and on_status:
                on_status(f"chunks retry {attempt}/{self.max_retries}")
            mpd_try = selected
            if mpd_all_candidates:
                mpd_try = mpd_all_candidates[(attempt - 1) % len(mpd_all_candidates)]
                if on_status and len(mpd_all_candidates) > 1:
                    on_status(
                        f"chunks using mpd {((attempt - 1) % len(mpd_all_candidates)) + 1}/{len(mpd_all_candidates)}"
                    )
            ok, result = self._download_chunks(
                mpd_try,
                workdir,
                name,
                on_progress,
                on_status=on_status,
            )
            if ok:
                out = self._find_video(workdir, name)
                if out and subtitle_urls:
                    if on_status:
                        on_status("embedding subtitle")
                    sub_file = self._download_subtitle(subtitle_urls[0], workdir, name)
                    if sub_file:
                        self._embed_subtitle(out, sub_file)
                return True, name, out or result
            last_err = str(result)
            if attempt < self.max_retries:
                # Small backoff before retrying full chunk pipeline.
                mult = int(self.cfg.h["ydl_retry_backoff_multiplier_sec"])
                cap = int(self.cfg.h["ydl_retry_backoff_cap_sec"])
                time.sleep(min(mult * attempt, cap))
        return False, name, last_err


def move_to_videos(cfg: AppConfig, video_name: str, output_format: str = "mp4") -> Path | None:
    vdir = cfg.videos_dir
    ddir = cfg.downloads_dir
    vext = cfg.h["video_extensions"]
    vext_suffixes = {e.lower() for e in vext}
    vdir.mkdir(exist_ok=True)
    src_dir = ddir / video_name
    src_file = None
    for ext in (output_format, *[e.lstrip(".") for e in vext]):
        candidate = src_dir / f"{video_name}.{ext}"
        if candidate.exists() and candidate.stat().st_size > 0:
            src_file = candidate
            break
    if src_file is None and src_dir.is_dir():
        for f in src_dir.iterdir():
            if f.suffix.lower() in vext_suffixes and f.stat().st_size > 0:
                src_file = f
                break
    if src_file is None:
        return None
    dst = vdir / src_file.name
    if dst.exists():
        stem = dst.stem
        dst = vdir / f"{stem}_{int(time.time())}{dst.suffix}"
    shutil.move(str(src_file), str(dst))
    if src_dir.is_dir():
        remaining_videos = [f for f in src_dir.iterdir() if f.suffix.lower() in vext_suffixes]
        if not remaining_videos:
            shutil.rmtree(str(src_dir), ignore_errors=True)
    return dst

