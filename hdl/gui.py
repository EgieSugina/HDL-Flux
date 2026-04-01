from __future__ import annotations

import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from hdl import routing
from hdl.config import SCRIPT_DIR, AppConfig, apply_phantomjs_path, load_config
from hdl.generic_downloader import download_generic_video
from hdl.hstream import HStreamDownloader, move_to_videos
from hdl.state_manager import StateManager

try:
    from PySide6.QtCore import QThread, Signal
    from PySide6.QtGui import QIcon
    from PySide6.QtWidgets import (
        QApplication,
        QCheckBox,
        QComboBox,
        QFileDialog,
        QGridLayout,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMessageBox,
        QPushButton,
        QPlainTextEdit,
        QProgressBar,
        QSpinBox,
        QTabWidget,
        QTableWidget,
        QTableWidgetItem,
        QTextEdit,
        QVBoxLayout,
        QWidget,
    )
except Exception as exc:  # pragma: no cover
    raise SystemExit("PySide6 is required. Install with: pip install PySide6") from exc


class _GuiConsole:
    def __init__(self, sink):
        self._sink = sink

    def print(self, *args, **kwargs):
        msg = " ".join(str(x) for x in args).strip()
        if msg:
            self._sink(msg)


@dataclass
class RunOptions:
    mode: str
    input_source: str
    workers: int
    list_file: str
    single_url: str
    use_proxy: bool
    proxy_url: str
    output_dir: str
    cookiefile: str
    cookies_browser: str
    skip_completed_hstream: bool
    h_quality: str
    h_format: str
    h_method: str
    h_use_browser: bool
    h_headless: bool
    h_delete_chunks: bool
    h_cuda_mode: str


def _atomic_write(path: Path, lines: list[str], merge: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: list[str] = []
    if merge and path.exists():
        try:
            existing = [x.strip() for x in path.read_text("utf-8").splitlines() if x.strip()]
        except OSError:
            existing = []
    seen = set(existing)
    out = list(existing)
    for line in lines:
        if line not in seen:
            seen.add(line)
            out.append(line)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(("\n".join(out) + ("\n" if out else "")), "utf-8")
    os.replace(tmp, path)


def _load_urls_from_file(cfg: AppConfig, path: Path) -> list[str]:
    if not path.exists():
        return []
    comment_p = str(cfg.ui.get("comment_prefix", "#"))
    prefixes = list(cfg.ui.get("url_line_prefixes", ("http://", "https://")))
    seen: set[str] = set()
    out: list[str] = []
    for line in path.read_text("utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith(comment_p):
            continue
        if not any(s.startswith(p) for p in prefixes):
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _platform_downloads_dir() -> Path:
    p = Path.home() / "Downloads"
    return p if p.exists() else (SCRIPT_DIR / "downloads")


def _fmt_bytes(n: float | int | None) -> str:
    if not n:
        return "0 B"
    v = float(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while v >= 1024 and i < len(units) - 1:
        v /= 1024.0
        i += 1
    return f"{v:.1f} {units[i]}"


class DownloadWorker(QThread):
    log = Signal(str)
    overall = Signal(int, int)
    item = Signal(object)
    finished_ok = Signal()
    failed = Signal(str)

    def __init__(self, options: RunOptions):
        super().__init__()
        self.options = options
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        try:
            self._run_impl()
            self.finished_ok.emit()
        except Exception as exc:
            self.failed.emit(str(exc))

    def _run_impl(self):
        cfg = load_config()
        apply_phantomjs_path(cfg)
        dummy_console = _GuiConsole(self.log.emit)
        opts = self.options
        proxy = opts.proxy_url.strip() if opts.use_proxy else None
        out_dir = Path(opts.output_dir).expanduser().resolve()

        urls: list[str] = []
        input_source = (opts.input_source or "single").strip().lower()
        if input_source not in {"single", "list"}:
            input_source = "single"
        lf = Path(opts.list_file).expanduser() if opts.list_file.strip() else cfg.list_file
        if input_source == "list":
            urls.extend(_load_urls_from_file(cfg, lf))
        su = opts.single_url.strip()
        if input_source == "single" and su and su not in urls:
            urls.append(su)
        if not urls:
            raise RuntimeError("No URL found (list file empty and single URL blank).")

        if opts.mode == "generic":
            urls = [u for u in urls if not routing.is_hstream_url(u, cfg)]
        elif opts.mode == "hstream":
            urls = [u for u in urls if routing.is_hstream_url(u, cfg)]
        if not urls:
            raise RuntimeError("No URL matches selected mode.")

        hstream_urls = [u for u in urls if routing.is_hstream_url(u, cfg)]
        generic_urls = [u for u in urls if not routing.is_hstream_url(u, cfg)]

        state = StateManager(cfg.state_file, error_max_len=int(cfg.h["state_error_max_len"]))
        if hstream_urls and opts.skip_completed_hstream:
            pending = state.pending(hstream_urls)
            urls = pending + generic_urls
            hstream_urls = pending
            self.log.emit(f"Skip completed hstream: {len(hstream_urls)} pending")

        cfg.downloads_dir.mkdir(exist_ok=True)
        cfg.videos_dir = out_dir
        if cfg.videos_dir not in cfg.skip_if_exists_dirs:
            cfg.skip_if_exists_dirs.append(cfg.videos_dir)
        cfg.videos_dir.mkdir(parents=True, exist_ok=True)
        os.makedirs(str(out_dir), exist_ok=True)

        hs = None
        if hstream_urls:
            hs = HStreamDownloader(
                cfg,
                dummy_console,
                quality=opts.h_quality,
                fmt=opts.h_format,
                use_browser=opts.h_use_browser,
                headless=opts.h_headless,
                prefer_ytdlp=(opts.h_method == "ytdlp"),
                delete_chunks=opts.h_delete_chunks,
                ffmpeg_cuda_mode=opts.h_cuda_mode,
                proxy=proxy,
            )
            creds = HStreamDownloader.load_credentials(cfg)
            if creds:
                hs.login(*creds)

        failed_urls: list[str] = []
        success_urls: list[str] = []
        lock = threading.Lock()
        total = len(urls)
        done = 0
        self.overall.emit(done, total)

        def run_one(url: str):
            if self._stop.is_set():
                return
            short = url.rstrip("/").split("/")[-1]
            self.item.emit({"name": short, "kind": "start", "pct": 0.0, "status": "start"})

            def on_progress(pct):
                self.item.emit(
                    {
                        "name": short,
                        "kind": "progress",
                        "pct": float(pct),
                        "status": "progress",
                    }
                )

            def on_status(msg):
                self.item.emit(
                    {
                        "name": short,
                        "kind": "status",
                        "pct": 0.0,
                        "status": str(msg),
                    }
                )

            def on_transfer(downloaded, total_bytes, speed=None):
                self.item.emit(
                    {
                        "name": short,
                        "kind": "transfer",
                        "pct": 0.0,
                        "status": "",
                        "downloaded": float(downloaded or 0),
                        "total": float(total_bytes or 0),
                        "speed": float(speed or 0),
                    }
                )

            is_hstream = routing.is_hstream_url(url, cfg)
            try:
                if is_hstream and hs:
                    name = routing.hstream_video_name_from_url(url, cfg)
                    existing = routing.find_existing_hstream_video(cfg, name, opts.h_format)
                    if existing is not None:
                        state.mark_done(url, str(existing))
                        with lock:
                            success_urls.append(url)
                        self.item.emit(
                            {
                                "name": short,
                                "kind": "done",
                                "pct": 100.0,
                                "status": f"already downloaded: {existing.name}",
                            }
                        )
                        return
                    state.mark_start(url, name)
                    ok, vname, result = hs.download_one(
                        url,
                        on_progress=on_progress,
                        on_status=on_status,
                        on_transfer=on_transfer,
                    )
                    if ok:
                        dst = move_to_videos(cfg, vname, opts.h_format)
                        state.mark_done(url, str(dst or result))
                        with lock:
                            success_urls.append(url)
                        self.item.emit({"name": short, "kind": "done", "pct": 100.0, "status": "ok"})
                    else:
                        state.mark_failed(url, str(result))
                        with lock:
                            failed_urls.append(url)
                        self.item.emit({"name": short, "kind": "done", "pct": 0.0, "status": f"fail: {result}"})
                else:
                    ok, msg = download_generic_video(
                        cfg,
                        url,
                        str(out_dir),
                        on_progress,
                        proxy=proxy,
                        cookiefile=opts.cookiefile.strip() or None,
                        cookies_browser=opts.cookies_browser.strip() or None,
                        on_status=on_status,
                        on_transfer=on_transfer,
                    )
                    if ok:
                        with lock:
                            success_urls.append(url)
                        self.item.emit({"name": short, "kind": "done", "pct": 100.0, "status": "ok"})
                    else:
                        with lock:
                            failed_urls.append(url)
                        self.item.emit({"name": short, "kind": "done", "pct": 0.0, "status": f"fail: {msg}"})
            except Exception as exc:
                with lock:
                    failed_urls.append(url)
                if is_hstream:
                    state.mark_failed(url, str(exc))
                self.item.emit({"name": short, "kind": "done", "pct": 0.0, "status": f"error: {exc}"})
            finally:
                nonlocal done
                with lock:
                    done += 1
                    self.overall.emit(done, total)

        with ThreadPoolExecutor(max_workers=max(1, opts.workers)) as pool:
            futs = [pool.submit(run_one, u) for u in urls]
            for fut in as_completed(futs):
                if self._stop.is_set():
                    break
                fut.result()

        if hs:
            hs.close_browser()

        _atomic_write(cfg.failed_links_file, failed_urls, cfg.link_files_merge)
        _atomic_write(cfg.success_links_file, success_urls, cfg.link_files_merge)
        self.log.emit(f"Failed links: {cfg.failed_links_file}")
        self.log.emit(f"Success links: {cfg.success_links_file}")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("HDL Flux GUI")
        self.resize(1000, 720)
        icon_path = SCRIPT_DIR / "icon.png"
        if not icon_path.is_file() and getattr(sys, "frozen", False):
            icon_path = Path(getattr(sys, "_MEIPASS", "")) / "icon.png"
        if icon_path.is_file():
            self.setWindowIcon(QIcon(str(icon_path)))
        self.worker: DownloadWorker | None = None
        self._rows: dict[str, int] = {}
        self.cfg = load_config()
        self._build_ui()
        self._load_config_text()

    def _build_ui(self):
        tabs = QTabWidget(self)
        self.setCentralWidget(tabs)

        run_tab = QWidget()
        tabs.addTab(run_tab, "Run")
        root = QVBoxLayout(run_tab)

        form = QGridLayout()
        row = 0

        self.mode = QComboBox()
        self.mode.addItems(["auto", "hstream", "generic"])
        form.addWidget(QLabel("Mode"), row, 0)
        form.addWidget(self.mode, row, 1)
        row += 1

        self.input_source = QComboBox()
        self.input_source.addItems(["single", "list"])
        self.input_source.setCurrentText("single")
        self.input_source.currentTextChanged.connect(self._sync_input_source_visibility)
        form.addWidget(QLabel("URL source"), row, 0)
        form.addWidget(self.input_source, row, 1)
        row += 1

        self.list_file_label = QLabel("List file")
        self.list_path = QLineEdit(str(self.cfg.list_file))
        pick_list = QPushButton("Browse list.txt")
        pick_list.clicked.connect(self._pick_list)
        self.list_browse_btn = pick_list
        form.addWidget(self.list_file_label, row, 0)
        form.addWidget(self.list_path, row, 1)
        form.addWidget(pick_list, row, 2)
        row += 1

        self.single_url_label = QLabel("Single URL")
        self.single_url = QLineEdit()
        self.single_url.setPlaceholderText("https://... (optional single URL)")
        form.addWidget(self.single_url_label, row, 0)
        form.addWidget(self.single_url, row, 1, 1, 2)
        row += 1

        self.workers = QSpinBox()
        self.workers.setRange(1, 64)
        self.workers.setValue(int(self.cfg.ui.get("workers_default", 3)))
        form.addWidget(QLabel("Workers"), row, 0)
        form.addWidget(self.workers, row, 1)
        row += 1

        self.use_proxy = QCheckBox("Use proxy")
        self.proxy_url = QLineEdit(self.cfg.proxy_default_url)
        form.addWidget(self.use_proxy, row, 0)
        form.addWidget(self.proxy_url, row, 1, 1, 2)
        row += 1

        self.output_dir = QLineEdit(str(_platform_downloads_dir()))
        pick_out = QPushButton("Browse output folder")
        pick_out.clicked.connect(self._pick_output_dir)
        form.addWidget(QLabel("Output folder"), row, 0)
        form.addWidget(self.output_dir, row, 1)
        form.addWidget(pick_out, row, 2)
        row += 1

        self.cookiefile = QLineEdit()
        pick_cookie = QPushButton("Browse cookies.txt")
        pick_cookie.clicked.connect(self._pick_cookie)
        form.addWidget(QLabel("Cookie file"), row, 0)
        form.addWidget(self.cookiefile, row, 1)
        form.addWidget(pick_cookie, row, 2)
        row += 1

        self.cookies_browser = QLineEdit()
        self.cookies_browser.setPlaceholderText("chrome / firefox / edge (optional)")
        form.addWidget(QLabel("cookiesfrombrowser"), row, 0)
        form.addWidget(self.cookies_browser, row, 1, 1, 2)
        row += 1

        self.skip_done = QCheckBox("Skip completed hstream from state")
        self.skip_done.setChecked(True)
        form.addWidget(self.skip_done, row, 0, 1, 3)
        row += 1

        hbox = QGroupBox("HStream options")
        hgrid = QGridLayout(hbox)
        self.h_quality = QComboBox()
        self.h_quality.addItems([str(x) for x in self.cfg.ui.get("quality_choices", ["720", "1080", "2160"])])
        self.h_quality.setCurrentText(str(self.cfg.ui.get("quality_default", "1080")))
        self.h_format = QComboBox()
        self.h_format.addItems([str(x) for x in self.cfg.ui.get("format_choices", ["mp4", "mkv"])])
        self.h_format.setCurrentText(str(self.cfg.ui.get("format_default", "mp4")))
        self.h_method = QComboBox()
        self.h_method.addItems(["ytdlp", "chunks"])
        self.h_method.setCurrentText(str(self.cfg.ui.get("hstream_method_default", "ytdlp")))
        self.h_browser = QCheckBox("Use browser")
        self.h_browser.setChecked(bool(self.cfg.h.get("use_browser_default", True)))
        self.h_headless = QCheckBox("Headless")
        self.h_headless.setChecked(bool(self.cfg.h.get("headless_default", True)))
        self.h_delete_chunks = QCheckBox("Delete chunks")
        self.h_delete_chunks.setChecked(bool(self.cfg.h.get("delete_chunks_default", True)))
        self.h_cuda = QComboBox()
        self.h_cuda.addItems([str(x) for x in self.cfg.ui.get("hstream_cuda_choices", ["auto", "cpu", "cuda"])])
        self.h_cuda.setCurrentText(str(self.cfg.ui.get("hstream_cuda_default", "auto")))
        hgrid.addWidget(QLabel("Quality"), 0, 0)
        hgrid.addWidget(self.h_quality, 0, 1)
        hgrid.addWidget(QLabel("Format"), 0, 2)
        hgrid.addWidget(self.h_format, 0, 3)
        hgrid.addWidget(QLabel("Method"), 1, 0)
        hgrid.addWidget(self.h_method, 1, 1)
        hgrid.addWidget(QLabel("CUDA"), 1, 2)
        hgrid.addWidget(self.h_cuda, 1, 3)
        hgrid.addWidget(self.h_browser, 2, 0)
        hgrid.addWidget(self.h_headless, 2, 1)
        hgrid.addWidget(self.h_delete_chunks, 2, 2)
        root.addLayout(form)
        root.addWidget(hbox)

        btns = QHBoxLayout()
        self.start_btn = QPushButton("Start")
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setEnabled(False)
        self.start_btn.clicked.connect(self._start)
        self.stop_btn.clicked.connect(self._stop)
        btns.addWidget(self.start_btn)
        btns.addWidget(self.stop_btn)
        root.addLayout(btns)

        self.pbar = QProgressBar()
        self.pbar.setRange(0, 100)
        self.pbar.setValue(0)
        root.addWidget(self.pbar)

        self.items_table = QTableWidget(0, 5)
        self.items_table.setHorizontalHeaderLabels(["Item", "Progress", "Size", "Speed", "Status"])
        self.items_table.verticalHeader().setVisible(False)
        self.items_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.items_table.setSelectionMode(QTableWidget.NoSelection)
        self.items_table.setColumnWidth(0, 280)
        self.items_table.setColumnWidth(2, 180)
        self.items_table.setColumnWidth(3, 140)
        self.items_table.setColumnWidth(4, 300)
        root.addWidget(self.items_table)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        root.addWidget(self.log)

        cfg_tab = QWidget()
        tabs.addTab(cfg_tab, "Config JSON")
        cfg_root = QVBoxLayout(cfg_tab)
        self.config_editor = QPlainTextEdit()
        cfg_root.addWidget(self.config_editor)
        cfg_btns = QHBoxLayout()
        b_reload = QPushButton("Reload from config.json")
        b_save = QPushButton("Save config.json")
        b_reload.clicked.connect(self._load_config_text)
        b_save.clicked.connect(self._save_config_text)
        cfg_btns.addWidget(b_reload)
        cfg_btns.addWidget(b_save)
        cfg_root.addLayout(cfg_btns)

        cred_tab = QWidget()
        tabs.addTab(cred_tab, "Credentials")
        cred_root = QVBoxLayout(cred_tab)
        cred_form = QGridLayout()
        self.creds_email = QLineEdit()
        self.creds_password = QLineEdit()
        self.creds_password.setEchoMode(QLineEdit.Password)
        cred_form.addWidget(QLabel("Email"), 0, 0)
        cred_form.addWidget(self.creds_email, 0, 1)
        cred_form.addWidget(QLabel("Password"), 1, 0)
        cred_form.addWidget(self.creds_password, 1, 1)
        cred_root.addLayout(cred_form)
        cred_btns = QHBoxLayout()
        b_creds_reload = QPushButton("Load .credentials.json")
        b_creds_save = QPushButton("Save .credentials.json")
        b_creds_reload.clicked.connect(self._load_credentials_file)
        b_creds_save.clicked.connect(self._save_credentials_file)
        cred_btns.addWidget(b_creds_reload)
        cred_btns.addWidget(b_creds_save)
        cred_root.addLayout(cred_btns)
        cred_root.addWidget(QLabel("Used for HStream login. Do not commit real credentials."))

        self._load_credentials_file()
        self._sync_input_source_visibility()

    def _pick_list(self):
        p, _ = QFileDialog.getOpenFileName(self, "Choose list file", str(SCRIPT_DIR), "Text files (*.txt);;All files (*)")
        if p:
            self.list_path.setText(p)

    def _pick_cookie(self):
        p, _ = QFileDialog.getOpenFileName(self, "Choose cookies file", str(SCRIPT_DIR), "Text files (*.txt);;All files (*)")
        if p:
            self.cookiefile.setText(p)

    def _sync_input_source_visibility(self):
        src = self.input_source.currentText().strip().lower()
        use_single = src == "single"
        self.single_url_label.setVisible(use_single)
        self.single_url.setVisible(use_single)
        self.list_file_label.setVisible(not use_single)
        self.list_path.setVisible(not use_single)
        self.list_browse_btn.setVisible(not use_single)

    def _pick_output_dir(self):
        p = QFileDialog.getExistingDirectory(self, "Choose output folder", self.output_dir.text().strip() or str(_platform_downloads_dir()))
        if p:
            self.output_dir.setText(p)

    def _load_config_text(self):
        p = SCRIPT_DIR / "config.json"
        if p.exists():
            self.config_editor.setPlainText(p.read_text("utf-8"))
        else:
            self.config_editor.setPlainText("{}\n")

    def _save_config_text(self):
        raw = self.config_editor.toPlainText().strip() or "{}"
        try:
            obj = json.loads(raw)
            txt = json.dumps(obj, indent=2, ensure_ascii=False) + "\n"
            (SCRIPT_DIR / "config.json").write_text(txt, "utf-8")
            self.cfg = load_config()
            QMessageBox.information(self, "Saved", "config.json updated.")
        except Exception as exc:
            QMessageBox.critical(self, "Invalid JSON", str(exc))

    def _load_credentials_file(self):
        path = self.cfg.credentials_file
        if not path.exists():
            self.creds_email.setText("")
            self.creds_password.setText("")
            return
        try:
            data = json.loads(path.read_text("utf-8"))
            self.creds_email.setText(str(data.get("email", "")))
            self.creds_password.setText(str(data.get("password", "")))
        except Exception as exc:
            QMessageBox.critical(self, "Credentials error", f"Failed reading {path}: {exc}")

    def _save_credentials_file(self):
        path = self.cfg.credentials_file
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "email": self.creds_email.text().strip(),
                "password": self.creds_password.text(),
            }
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", "utf-8")
            os.replace(tmp, path)
            QMessageBox.information(self, "Saved", f"Credentials saved to {path}")
        except Exception as exc:
            QMessageBox.critical(self, "Credentials error", f"Failed writing {path}: {exc}")

    def _append_log(self, msg: str):
        self.log.append(msg)

    def _ensure_row(self, name: str) -> int:
        row = self._rows.get(name)
        if row is not None:
            return row
        row = self.items_table.rowCount()
        self.items_table.insertRow(row)
        self._rows[name] = row
        self.items_table.setItem(row, 0, QTableWidgetItem(name))
        bar = QProgressBar()
        bar.setRange(0, 100)
        bar.setValue(0)
        self.items_table.setCellWidget(row, 1, bar)
        self.items_table.setItem(row, 2, QTableWidgetItem("-"))
        self.items_table.setItem(row, 3, QTableWidgetItem("-"))
        self.items_table.setItem(row, 4, QTableWidgetItem("start"))
        return row

    def _on_item_event(self, ev: object):
        if not isinstance(ev, dict):
            return
        name = str(ev.get("name", "item"))
        row = self._ensure_row(name)
        kind = str(ev.get("kind", "status"))
        pct = float(ev.get("pct", 0.0))
        bar = self.items_table.cellWidget(row, 1)
        if isinstance(bar, QProgressBar):
            if kind in {"progress", "done"}:
                bar.setValue(max(0, min(100, int(pct))))
        if kind == "transfer":
            d = float(ev.get("downloaded", 0.0))
            t = float(ev.get("total", 0.0))
            s = float(ev.get("speed", 0.0))
            size_txt = f"{_fmt_bytes(d)}/{_fmt_bytes(t)}" if t > 0 else f"{_fmt_bytes(d)}/?"
            speed_txt = f"{_fmt_bytes(s)}/s" if s > 0 else "-"
            self.items_table.item(row, 2).setText(size_txt)
            self.items_table.item(row, 3).setText(speed_txt)
        status = str(ev.get("status", ""))
        if status:
            self.items_table.item(row, 4).setText(status)
            if kind in {"done", "status"}:
                self._append_log(f"{name}: {status}")

    def _start(self):
        if self.worker and self.worker.isRunning():
            return
        opts = RunOptions(
            mode=self.mode.currentText(),
            input_source=self.input_source.currentText(),
            workers=int(self.workers.value()),
            list_file=self.list_path.text().strip(),
            single_url=self.single_url.text().strip(),
            use_proxy=self.use_proxy.isChecked(),
            proxy_url=self.proxy_url.text().strip(),
            output_dir=self.output_dir.text().strip() or str(_platform_downloads_dir()),
            cookiefile=self.cookiefile.text().strip(),
            cookies_browser=self.cookies_browser.text().strip(),
            skip_completed_hstream=self.skip_done.isChecked(),
            h_quality=self.h_quality.currentText(),
            h_format=self.h_format.currentText(),
            h_method=self.h_method.currentText(),
            h_use_browser=self.h_browser.isChecked(),
            h_headless=self.h_headless.isChecked(),
            h_delete_chunks=self.h_delete_chunks.isChecked(),
            h_cuda_mode=self.h_cuda.currentText(),
        )
        self.worker = DownloadWorker(opts)
        self.worker.log.connect(self._append_log)
        self.worker.item.connect(self._on_item_event)
        self.worker.overall.connect(self._on_overall)
        self.worker.finished_ok.connect(self._on_done)
        self.worker.failed.connect(self._on_failed)
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.pbar.setValue(0)
        self._rows.clear()
        self.items_table.setRowCount(0)
        self.log.clear()
        self.worker.start()

    def _stop(self):
        if self.worker:
            self.worker.stop()
            self._append_log("Stop requested...")

    def _on_overall(self, done: int, total: int):
        if total <= 0:
            self.pbar.setValue(0)
            return
        self.pbar.setValue(int(done * 100 / total))

    def _on_done(self):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._append_log("Finished.")

    def _on_failed(self, msg: str):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        QMessageBox.critical(self, "Run failed", msg)


def run_gui():
    app = QApplication([])
    w = MainWindow()
    w.show()
    app.exec()

