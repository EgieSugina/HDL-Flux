from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from hdl import browser as browser_utils
from hdl import generic_downloader, routing
from hdl.config import SCRIPT_DIR, apply_phantomjs_path, load_config, load_json_file
from hdl.hstream import HStreamDownloader, move_to_videos
from hdl.state_manager import StateManager

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.prompt import Prompt, IntPrompt, Confirm
    from rich.text import Text
    from rich.padding import Padding
    from rich import box
except ImportError:
    try:
        _rm = load_json_file(SCRIPT_DIR / "config.defaults.json")["strings"]["rich_required"]
    except Exception:
        _rm = "Package 'rich' is required: pip install rich"
    print(_rm)
    raise SystemExit(1)

console = Console()


def _make_console(cfg) -> Console:
    r = cfg.ui.get("rich", {})
    return Console(
        highlight=bool(r.get("console_highlight", False)),
        soft_wrap=bool(r.get("console_soft_wrap", True)),
    )


def _box_named(name: str):
    return getattr(box, str(name).upper().replace("-", "_"), box.ROUNDED)


def _load_last_session(cfg) -> dict | None:
    path = cfg.last_session_file
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text("utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _save_last_session(cfg, data: dict) -> None:
    try:
        cfg.last_session_file.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            "utf-8",
        )
    except OSError:
        pass


def show_banner(cfg):
    r = cfg.ui.get("rich", {})
    b = r.get("banner", {})
    title = Text()
    title.append(str(b.get("title_primary", "UNIFIED")), style=str(b.get("title_primary_style", "bold cyan")))
    title.append(str(b.get("title_secondary", " DOWNLOADER")), style=str(b.get("title_secondary_style", "bold white")))
    db = browser_utils.default_browser_ytdlp_name(cfg)
    sub_t = str(b.get("subtitle_template", "[dim]default browser: [cyan]{browser}[/][/dim]")).format(browser=db)
    pad = b.get("padding", [1, 4])
    pt = tuple(pad) if isinstance(pad, list) else (1, 4)
    console.print(
        Panel(
            title,
            box=_box_named(str(b.get("box_style", "HEAVY"))),
            border_style=str(b.get("border_style", "cyan")),
            padding=pt,
            subtitle=sub_t,
        )
    )


def show_deps(cfg):
    r = cfg.ui.get("rich", {})
    d = r.get("dependencies", {})
    tbl = Table(box=_box_named(str(d.get("table_box", "SIMPLE"))), show_header=False, padding=(0, 2))
    tbl.add_column("dep", style="bold")
    tbl.add_column("status")
    hx = cfg.h
    ffmpeg_ok = False
    try:
        ffmpeg_ok = (
            subprocess.run([hx["ffmpeg_binary"], "-version"], capture_output=True, timeout=float(hx["ffmpeg_version_timeout_sec"])).returncode
            == 0
        )
    except Exception:
        pass
    checks = {
        "ytdlp": browser_utils.YTDLP_AVAILABLE,
        "selenium": browser_utils.SELENIUM_AVAILABLE,
        "ffmpeg": ffmpeg_ok,
    }
    ok_s = str(d.get("status_ready", "[green]ready[/]"))
    bad_s = str(d.get("status_missing", "[yellow]not installed[/]"))
    for row in d.get("rows", []):
        key = str(row.get("key", ""))
        label = str(row.get("label", key))
        tbl.add_row(label, ok_s if checks.get(key) else bad_s)
    ptitle = str(d.get("panel_title", "Dependencies"))
    console.print(
        Panel(
            tbl,
            title=f"[bold]{ptitle}[/]",
            border_style=str(d.get("border_style", "dim")),
            box=_box_named(str(d.get("panel_box", "ROUNDED"))),
        )
    )


def run():
    global console
    try:
        cfg = load_config()
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/]")
        return
    console = _make_console(cfg)
    apply_phantomjs_path(cfg)
    show_banner(cfg)
    show_deps(cfg)

    if not browser_utils.YTDLP_AVAILABLE:
        console.print(cfg.text("deps", "yt_dlp_required"))
        return

    ui = cfg.ui
    r = ui.get("rich", {})
    rules = r.get("rules", {})
    pr = ui.get("progress", {})

    console.print()
    console.rule(rules.get("setup", "[bold cyan]Setup[/]"))
    console.print()
    last_session = _load_last_session(cfg)
    use_prev = bool(last_session) and Confirm.ask(
        cfg.prompt_label("use_previous_session"),
        default=True,
    )
    mode = str(ui["mode_default"])
    if use_prev and last_session:
        mode = str(last_session.get("mode", mode))
        if mode not in set(ui["mode_choices"]):
            mode = str(ui["mode_default"])
    else:
        mode = Prompt.ask(cfg.prompt_label("mode"), choices=list(ui["mode_choices"]), default=str(ui["mode_default"]))
    hstream_only = mode == "hstream"
    generic_only = mode == "generic"

    clean_list_path = routing.rebuild_clean_url_file(cfg, console, cfg.list_file)
    urls = routing.load_urls_from_list(
        cfg,
        console,
        path=clean_list_path,
        hstream_only=hstream_only,
    )
    if generic_only:
        urls = [u for u in urls if not routing.is_hstream_url(u, cfg)]
    elif mode != "auto":
        urls = [u for u in urls if routing.is_hstream_url(u, cfg)]

    if not urls:
        console.print(cfg.text("urls", "none_to_process", list_file=cfg.list_file.name))
        return

    hstream_urls = [u for u in urls if routing.is_hstream_url(u, cfg)]
    generic_urls = [u for u in urls if not routing.is_hstream_url(u, cfg)]

    state = StateManager(cfg.state_file, error_max_len=int(cfg.h["state_error_max_len"]))
    if hstream_urls:
        done_n, _, _ = state.stats()
        console.print()
        console.print(cfg.text("session", "counts_line", total=len(urls), hstream_n=len(hstream_urls), generic_n=len(generic_urls)))
        if done_n > 0:
            pending = state.pending(hstream_urls)
            if not generic_urls:
                console.print(cfg.text("session", "resume_line", done=done_n, pending=len(pending)))
                action = Prompt.ask(cfg.prompt_label("action"), choices=list(ui["resume_action_choices"]), default=str(ui["resume_action_default"]))
                if action == "quit":
                    return
                if action == "restart":
                    state.clear()
                else:
                    hstream_urls = pending
            elif pending != hstream_urls and Confirm.ask(cfg.prompt_label("skip_completed_hstream"), default=True):
                hstream_urls = pending
            urls = hstream_urls + generic_urls
    else:
        console.print()
        console.print(cfg.text("session", "generic_only_line", n=len(urls)))

    if not urls:
        console.print()
        console.print(cfg.text("urls", "nothing_todo"))
        console.print()
        return

    console.print()
    console.rule(rules.get("session", "[bold cyan]Session[/]"))
    console.print()

    proxy = None
    purl = cfg.proxy_default_url
    if use_prev and last_session:
        if bool(last_session.get("use_proxy", False)):
            proxy = purl
    elif Confirm.ask(cfg.prompt_label("proxy", url=purl), default=False):
        proxy = purl

    cookiefile: str | None = None
    cookies_browser: str | None = None
    if generic_urls:
        if use_prev and last_session:
            prev_cookiefile = str(last_session.get("cookiefile", "")).strip()
            prev_browser = str(last_session.get("cookies_browser", "")).strip()
            if prev_cookiefile and os.path.isfile(prev_cookiefile):
                cookiefile = prev_cookiefile
            elif prev_browser:
                cookies_browser = prev_browser
            else:
                cookies_browser = None
        else:
            console.print(cfg.text("cookies", "hint_line", browser=browser_utils.default_browser_ytdlp_name(cfg)))
            csrc = Prompt.ask(cfg.prompt_label("cookie_source"), choices=list(ui["cookie_source_choices"]), default=str(ui["cookie_source_default"]))
            if csrc == "default":
                bn = browser_utils.default_browser_ytdlp_name(cfg)
                if Confirm.ask(cfg.prompt_label("cookie_close_default", browser=bn), default=False):
                    browser_utils.kill_browser_for_cookies(bn, cfg)
                cache_path = cfg.cookies_cache_file
                if browser_utils.export_cookies_ytdlp(bn, cache_path, cfg):
                    cookiefile = str(cache_path)
                    console.print(cfg.text("cookies", "exported", name=cache_path.name))
                else:
                    cookies_browser = bn
                    console.print(cfg.text("cookies", "live_browser"))
            elif csrc == "file":
                p = Prompt.ask(cfg.prompt_label("cookie_path"))
                if p and os.path.isfile(p):
                    cookiefile = p
            elif csrc != "none":
                if Confirm.ask(cfg.prompt_label("cookie_close_named", browser=csrc), default=False):
                    browser_utils.kill_browser_for_cookies(csrc, cfg)
                cache_path = cfg.cookies_cache_file
                if browser_utils.export_cookies_ytdlp(csrc, cache_path, cfg):
                    cookiefile = str(cache_path)
                else:
                    cookies_browser = csrc

    if use_prev and last_session:
        workers = int(last_session.get("workers", ui["workers_default"]))
    else:
        workers = IntPrompt.ask(cfg.prompt_label("workers"), default=int(ui["workers_default"]))
    workers = max(int(ui["workers_min"]), min(workers, int(ui["workers_max"])))

    hx = cfg.h
    quality = str(ui["quality_default"])
    fmt = str(ui["format_default"])
    prefer_ytdlp = bool(hx["prefer_ytdlp_default"])
    use_browser = bool(hx["use_browser_default"])
    delete_chunks = bool(hx["delete_chunks_default"])
    ffmpeg_cuda_mode = str(hx.get("ffmpeg_use_cuda", "auto"))
    if hstream_urls:
        if use_prev and last_session:
            quality = str(last_session.get("quality", ui["quality_default"]))
            if quality not in set(ui["quality_choices"]):
                quality = str(ui["quality_default"])
            fmt = str(last_session.get("format", ui["format_default"]))
            if fmt not in set(ui["format_choices"]):
                fmt = str(ui["format_default"])
            prefer_ytdlp = bool(last_session.get("prefer_ytdlp", prefer_ytdlp))
            use_browser = browser_utils.SELENIUM_AVAILABLE and bool(last_session.get("use_browser", use_browser))
            delete_chunks = bool(last_session.get("delete_chunks", delete_chunks))
            ffmpeg_cuda_mode = str(last_session.get("ffmpeg_cuda_mode", ffmpeg_cuda_mode))
        else:
            quality = Prompt.ask(cfg.prompt_label("hstream_quality"), choices=list(ui["quality_choices"]), default=str(ui["quality_default"]))
            fmt = Prompt.ask(cfg.prompt_label("hstream_format"), choices=list(ui["format_choices"]), default=str(ui["format_default"]))
            if browser_utils.YTDLP_AVAILABLE:
                method = Prompt.ask(cfg.prompt_label("hstream_method"), choices=list(ui["hstream_method_choices"]), default=str(ui["hstream_method_default"]))
                prefer_ytdlp = method == "ytdlp"
            use_browser = browser_utils.SELENIUM_AVAILABLE and Confirm.ask(cfg.prompt_label("hstream_browser"), default=bool(hx["use_browser_default"]))
            delete_chunks = Confirm.ask(cfg.prompt_label("delete_chunks"), default=bool(hx["delete_chunks_default"]))
            ffmpeg_cuda_mode = Prompt.ask(
                cfg.prompt_label("hstream_cuda"),
                choices=list(ui.get("hstream_cuda_choices", ["auto", "cpu", "cuda"])),
                default=str(ui.get("hstream_cuda_default", ffmpeg_cuda_mode)),
            )

    if use_prev and last_session:
        generic_out = str(last_session.get("generic_out", cfg.g["output_dir_default"]))
    else:
        generic_out = str(Prompt.ask(cfg.prompt_label("generic_output_dir"), default=str(cfg.g["output_dir_default"])))
    sp = r.get("setup_panel", {})
    rl = ui.get("recap_labels", {})
    rv = ui.get("recap_values", {})

    def _rl(key: str) -> str:
        return str(rl.get(key, key))

    recap = Table(box=_box_named(str(sp.get("inner_table_box", "SIMPLE"))), show_header=False, padding=(0, 2))
    recap.add_column(style="dim", no_wrap=True)
    recap.add_column(style="bold")
    qsuf = str(ui.get("recap_quality_suffix", "p"))
    recap.add_row(_rl("proxy"), purl if proxy else str(rv["proxy_off"]))
    recap.add_row(_rl("workers"), str(workers))
    if hstream_urls:
        recap.add_row(_rl("quality"), f"{quality}{qsuf}")
        recap.add_row(_rl("format"), fmt.upper() if ui.get("recap_format_uppercase") else fmt)
        recap.add_row(_rl("method"), str(rv["method_ytdlp" if prefer_ytdlp else "method_chunks"]))
        recap.add_row(_rl("browser"), str(rv["yes" if use_browser else "no"]))
        recap.add_row(_rl("chunks"), str(rv["yes" if delete_chunks else "no"]))
    recap.add_row(_rl("output"), generic_out)
    recap.add_row(_rl("urls"), str(len(urls)))
    console.print()
    console.print(Panel(recap, title=f"[bold]{sp.get('title', 'Ready')}[/]", border_style=str(sp.get("border_style", "blue")), box=_box_named(str(sp.get("box_style", "ROUNDED")))))
    console.print()

    cfg.downloads_dir.mkdir(exist_ok=True)
    cfg.videos_dir.mkdir(exist_ok=True)
    os.makedirs(generic_out, exist_ok=True)

    hs = None
    if hstream_urls:
        hs = HStreamDownloader(
            cfg,
            console,
            quality=quality,
            fmt=fmt,
            use_browser=use_browser,
            headless=bool(hx["headless_default"]),
            prefer_ytdlp=prefer_ytdlp,
            delete_chunks=delete_chunks,
            ffmpeg_cuda_mode=ffmpeg_cuda_mode,
            proxy=proxy,
        )
        creds = HStreamDownloader.load_credentials(cfg)
        nmask = int(ui.get("auth_mask_chars", 4))
        if creds:
            masked = creds[0][:nmask] + cfg.text("auth", "mask_suffix")
            console.print(cfg.text("auth", "logging_in", masked=masked))
            st = cfg.text("auth", "status")
            spin = str(r.get("auth_spinner", "dots"))
            with console.status(f"[dim]{st}[/]", spinner=spin):
                logged = hs.login(*creds)
            console.print(cfg.text("auth", "ok") if logged else cfg.text("auth", "fail_guest"))
        else:
            console.print(cfg.text("auth", "no_creds", creds_name=cfg.credentials_file.name))

    if not Confirm.ask(cfg.prompt_label("start_confirm"), default=True):
        if hs:
            hs.close_browser()
        return

    ok_list: list[str] = []
    fail_list: list[tuple[str, str]] = []
    failed_urls: list[str] = []
    success_urls: list[str] = []
    results_lock = threading.Lock()
    t0 = time.time()

    console.print()
    console.rule(rules.get("download", "[bold cyan]Download[/]"))
    console.print()

    bw = int(pr.get("bar_width", 30))
    ov_lbl = str(pr.get("overall_label", "Overall"))
    cap_pct = float(pr.get("progress_cap_pct", 99.9))
    tmax = int(pr.get("task_label_max_len", 40))
    err_show = min(int(pr.get("summary_error_display_max", 80)), int(cfg.g["error_message_max_len"]))
    ok_tmax = int(pr.get("summary_ok_title_max", 60))
    bt_sleep = float(pr.get("between_tasks_sleep_sec", 0.2))
    ok_pfx = str(pr.get("ok_prefix", "[green]✓[/]"))
    fail_pfx = str(pr.get("fail_prefix", "[red]✗[/]"))

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=bw),
        TextColumn("[progress.percentage]{task.percentage:>5.1f}%"),
        TimeElapsedColumn(),
        console=console,
        expand=False,
    ) as progress:
        overall = progress.add_task(f"[cyan]{ov_lbl}  [0/{len(urls)}]", total=len(urls))

        def run_one(url: str):
            short = url.rstrip("/").split("/")[-1][:tmax]
            task = progress.add_task(f"  {short}", total=100)
            is_hstream = routing.is_hstream_url(url, cfg)
            if is_hstream:
                vname = routing.hstream_video_name_from_url(url, cfg)
                existing = routing.find_existing_hstream_video(cfg, vname, fmt)
                if existing is not None:
                    with results_lock:
                        state.mark_done(url, str(existing))
                        ok_list.append(vname[:ok_tmax])
                        success_urls.append(url)
                        progress.update(task, completed=100, description=f"  {ok_pfx} {short}")
                    return
                state.mark_start(url, short)

            def on_progress(pct):
                progress.update(task, completed=min(pct, cap_pct))

            try:
                if is_hstream and hs:
                    ok, vname, result = hs.download_one(url, on_progress)
                    with results_lock:
                        if ok:
                            dst = move_to_videos(cfg, vname, fmt)
                            state.mark_done(url, str(dst or result))
                            ok_list.append(vname)
                            success_urls.append(url)
                            progress.update(task, completed=100, description=f"  {ok_pfx} {short}")
                        else:
                            state.mark_failed(url, str(result))
                            fail_list.append((short, str(result)[:err_show]))
                            failed_urls.append(url)
                            progress.update(task, description=f"  {fail_pfx} {short}")
                else:
                    gok, gmsg = generic_downloader.download_generic_video(
                        cfg,
                        url,
                        generic_out,
                        on_progress,
                        proxy=proxy,
                        cookiefile=cookiefile,
                        cookies_browser=cookies_browser,
                    )
                    with results_lock:
                        if gok:
                            ok_list.append(gmsg[:ok_tmax])
                            success_urls.append(url)
                            progress.update(task, completed=100, description=f"  {ok_pfx} {short}")
                        else:
                            fail_list.append((short, gmsg))
                            failed_urls.append(url)
                            progress.update(task, description=f"  {fail_pfx} {short}")
            except Exception as exc:
                with results_lock:
                    if is_hstream:
                        state.mark_failed(url, str(exc))
                    fail_list.append((short, str(exc)[:err_show]))
                    failed_urls.append(url)
                    progress.update(task, description=f"  {fail_pfx} {short}")
            finally:
                with results_lock:
                    n = len(ok_list) + len(fail_list)
                progress.update(overall, completed=n, description=f"[cyan]{ov_lbl}  [{n}/{len(urls)}]")
                time.sleep(bt_sleep)
                progress.remove_task(task)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(run_one, u): u for u in urls}
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception:
                    pass

    if hs:
        hs.close_browser()

    elapsed = time.time() - t0
    console.print()
    console.rule(rules.get("results", "[bold cyan]Results[/]"))
    console.print()
    st = cfg._d.get("strings", {}).get("summary", {})
    tbl = Table(title=st.get("table_title", "Summary"), box=box.ROUNDED, border_style="cyan")
    tbl.add_column(st.get("col_status", "Status"), justify="center")
    tbl.add_column(st.get("col_count", "Count"), justify="center")
    tbl.add_row(st.get("ok", "[green]OK[/]"), str(len(ok_list)))
    tbl.add_row(st.get("fail", "[red]Fail[/]"), str(len(fail_list)))
    m, s = divmod(int(elapsed), 60)
    h, m = divmod(m, 60)
    tbl.add_row(st.get("time", "[dim]Time[/]"), f"{h:02d}:{m:02d}:{s:02d}")
    console.print(Padding(tbl, (0, 0, 0, 2)))
    if fail_list:
        ft = r.get("failures_panel", {})
        ftbl = Table(box=_box_named(str(ft.get("table_box", "SIMPLE"))), show_header=True, header_style="bold red")
        ftbl.add_column(st.get("failures_col_item", "Item"), style="bold", no_wrap=True, max_width=36)
        ftbl.add_column(st.get("failures_col_error", "Error"), style="dim")
        for name, err in fail_list:
            ftbl.add_row(name, err)
        console.print(
            Panel(
                ftbl,
                title=f"[bold]{ft.get('title', 'Failed items')}[/]",
                border_style=str(ft.get("border_style", "red")),
                box=_box_named(str(ft.get("box_style", "ROUNDED"))),
            )
        )
        console.print()
    # Always refresh failed links file with failures from this run.
    seen: set[str] = set()
    unique_failed = [u for u in failed_urls if not (u in seen or seen.add(u))]
    cfg.failed_links_file.write_text("\n".join(unique_failed), "utf-8")
    if unique_failed:
        console.print(f"[yellow]Failed links saved:[/] [bold]{cfg.failed_links_file}[/]")
    seen_ok: set[str] = set()
    unique_success = [u for u in success_urls if not (u in seen_ok or seen_ok.add(u))]
    cfg.success_links_file.write_text("\n".join(unique_success), "utf-8")
    if unique_success:
        console.print(f"[green]Success links saved:[/] [bold]{cfg.success_links_file}[/]")
    if ok_list and hstream_urls:
        console.print(cfg.text("summary", "hstream_saved", path=str(cfg.videos_dir)))
    if ok_list and generic_urls:
        console.print(cfg.text("summary", "generic_saved", path=generic_out))
    console.print()
    _save_last_session(
        cfg,
        {
            "mode": mode,
            "use_proxy": bool(proxy),
            "cookiefile": cookiefile or "",
            "cookies_browser": cookies_browser or "",
            "workers": workers,
            "quality": quality,
            "format": fmt,
            "prefer_ytdlp": bool(prefer_ytdlp),
            "use_browser": bool(use_browser),
            "delete_chunks": bool(delete_chunks),
            "ffmpeg_cuda_mode": ffmpeg_cuda_mode,
            "generic_out": generic_out,
        },
    )


def run_with_interrupt_handling():
    try:
        run()
    except KeyboardInterrupt:
        try:
            imsg = load_json_file(SCRIPT_DIR / "config.defaults.json")["strings"]["interrupt"]
        except Exception:
            imsg = "[yellow]Interrupted.[/]"
        console.print(Padding(imsg, (1, 2)))

