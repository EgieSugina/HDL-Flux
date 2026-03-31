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
from urllib.parse import urljoin

import requests

from hdl import routing
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
        h = self.cfg.h
        browser_cookies = self._driver.get_cookies()
        for c in browser_cookies:
            self.session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
        lines = list(h["cookies_netscape_header_lines"])
        for c in browser_cookies:
            domain = c.get("domain", "")
            flag = "TRUE" if domain.startswith(".") else "FALSE"
            path = c.get("path", "/")
            secure = "TRUE" if c.get("secure") else "FALSE"
            expiry = str(int(c.get("expiry", 0)))
            lines.append(f"{domain}\t{flag}\t{path}\t{secure}\t{expiry}\t{c['name']}\t{c['value']}")
        self.cfg.cookies_file.write_text("\n".join(lines), "utf-8")

    def _extract_mpd_urls(self, html: str) -> dict[str, str]:
        pat = self.cfg.h["mpd_source_regex"]
        return {size: url for url, size in re.findall(pat, html)}

    def get_mpd_urls(self, page_url: str, on_status=None) -> dict[str, str] | None:
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
                        mpd = self._extract_mpd_urls(self._driver.page_source)
                        if mpd:
                            if on_status:
                                on_status("mpd found via browser")
                            return mpd
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
                        pass
        try:
            if on_status:
                on_status("http fetch page")
            with self.session.get(page_url, timeout=req_t) as resp:
                resp.raise_for_status()
                mpd = self._extract_mpd_urls(resp.text)
            if mpd and on_status:
                on_status("mpd found via http")
            return mpd or None
        except Exception:
            return None

    def _ytdlp_fmt(self, fallback=False) -> str:
        yd = self.cfg.ytdlp_cfg()
        if fallback:
            return str(yd["format_fallback_chain"])
        qmap = self._quality_res()
        lab = self.quality if self.quality in qmap else max(qmap.keys(), key=lambda z: int(z))
        return str(yd["format_height_template"]).format(h=lab)

    def _download_ytdlp(self, page_url, mpd_url, name, workdir, on_progress, on_status=None):
        h = self.cfg.h
        # Prefer MPD directly for hstream; page URLs are often unsupported by yt-dlp.
        urls_to_try = [u for u in (mpd_url, page_url) if u]
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
                    if cf.exists():
                        ydl_opts["cookiefile"] = str(cf)
                    if on_progress:

                        def _hook(d, _cb=on_progress):
                            if d["status"] == "downloading":
                                tot = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                                got = d.get("downloaded_bytes", 0)
                                if tot > 0:
                                    _cb(got / tot * 100)
                            elif d["status"] == "finished":
                                _cb(100)

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
                    entry = {"media": media, "start": start, "count": count}
                    if is_vid:
                        info["video"].append(entry)
                    elif is_aud:
                        info["audio"].append(entry)
            return info
        except Exception:
            return None

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
            path = vi["media"].replace("$Number$", str(i))
            path = path.replace("$Number%05d$", f"{i:05d}")
            chunk_list.append((i, path))
        total = len(chunk_list)
        if on_status:
            on_status(f"chunks downloading {total} segments")
        done_count = 0
        failed = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {}
            for num, path in chunk_list:
                url = urljoin(base, path)
                fp = os.path.join(workdir, tmpl.format(num=num))
                futs[pool.submit(self._download_chunk, url, fp)] = (num, url, fp)
            for fut in as_completed(futs):
                num, url, fp = futs[fut]
                if fut.result():
                    done_count += 1
                else:
                    failed.append((num, url, fp))
                if on_progress and total:
                    on_progress(done_count / total * 80)
        for num, url, fp in failed:
            if self._download_chunk(url, fp, retries=rfail):
                done_count += 1
        if on_status and failed:
            on_status(f"chunks recovered {done_count}/{total}")
        if done_count < total * ratio:
            return False, self.cfg.h_msg("chunks_missing_template", missing=total - done_count, total=total)
        if on_status:
            on_status("chunks converting with ffmpeg")
        ok, result = self._convert_chunks(workdir, name)
        if on_progress:
            on_progress(100)
        return ok, result

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

    def download_one(self, page_url, on_progress=None, on_status=None):
        name = routing.hstream_video_name_from_url(page_url, self.cfg)
        workdir = str(self.cfg.downloads_dir / name)
        os.makedirs(workdir, exist_ok=True)
        if on_status:
            on_status("resolve mpd")
        mpd_urls = self.get_mpd_urls(page_url, on_status=on_status)
        if not mpd_urls:
            if on_status:
                on_status("mpd not found")
            return False, name, self.cfg.h_msg("mpd_not_found")
        keys = list(mpd_urls.keys())
        selected = mpd_urls.get(self.quality) or mpd_urls[max(keys, key=lambda z: int(z))]
        if self.prefer_ytdlp:
            if on_status:
                on_status("trying yt-dlp")
            ok, err = self._download_ytdlp(
                page_url,
                selected,
                name,
                workdir,
                on_progress,
                on_status=on_status,
            )
            if ok:
                out = self._find_video(workdir, name)
                return True, name, out or workdir
        last_err = ""
        for attempt in range(1, self.max_retries + 1):
            if on_status and attempt == 1:
                on_status("trying chunks")
            if attempt > 1 and on_status:
                on_status(f"chunks retry {attempt}/{self.max_retries}")
            ok, result = self._download_chunks(
                selected,
                workdir,
                name,
                on_progress,
                on_status=on_status,
            )
            if ok:
                out = self._find_video(workdir, name)
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

