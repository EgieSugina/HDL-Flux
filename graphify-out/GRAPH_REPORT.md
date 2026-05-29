# Graph Report - hls-downloader  (2026-05-29)

## Corpus Check
- 25 files · ~22,774 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 699 nodes · 1032 edges · 35 communities (32 shown, 3 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 62 edges (avg confidence: 0.5)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `81638eec`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 32|Community 32]]
- [[_COMMUNITY_Community 33|Community 33]]
- [[_COMMUNITY_Community 37|Community 37]]

## God Nodes (most connected - your core abstractions)
1. `hstream` - 70 edges
2. `HStreamDownloader` - 53 edges
3. `AppConfig` - 49 edges
4. `generic` - 42 edges
5. `StateManager` - 31 edges
6. `ui` - 28 edges
7. `MainWindow` - 24 edges
8. `str` - 21 edges
9. `prompts` - 20 edges
10. `messages` - 16 edges

## Surprising Connections (you probably didn't know these)
- `Path` --uses--> `AppConfig`  [INFERRED]
  hdl/browser.py → hdl/config.py
- `main()` --calls--> `load_config()`  [EXTRACTED]
  clean_urls.py → hdl/config.py
- `Console` --uses--> `StateManager`  [INFERRED]
  hdl/app.py → hdl/state_manager.py
- `float` --uses--> `StateManager`  [INFERRED]
  hdl/app.py → hdl/state_manager.py
- `int` --uses--> `StateManager`  [INFERRED]
  hdl/app.py → hdl/state_manager.py

## Communities (35 total, 3 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.03
Nodes (66): hstream, after_video_element_wait_sec, base_url, chunk_download_workers, chunk_filename_regex, chunk_filename_template, chunk_glob_pattern, chunk_http_fail_sleep_sec (+58 more)

### Community 1 - "Community 1"
Cohesion: 0.07
Nodes (22): _atomic_write(), DownloadWorker, _fmt_bytes(), _GuiConsole, _load_urls_from_file(), MainWindow, _platform_downloads_dir(), AppConfig (+14 more)

### Community 2 - "Community 2"
Cohesion: 0.09
Nodes (35): AppConfig, int, str, build_generic_ydl_opts(), _collect_stream_targets(), detect_site(), download_generic_video(), _embed_first_hosts() (+27 more)

### Community 3 - "Community 3"
Cohesion: 0.04
Nodes (45): browser_kill, after_kill_sleep_sec, pkill_timeout_sec, taskkill_timeout_sec, unix, windows, paths, base_dir (+37 more)

### Community 4 - "Community 4"
Cohesion: 0.12
Nodes (12): HStreamDownloader, AppConfig, bool, int, Path, str, Extract MPD URLs from page source.         Uses primary configured regex, then b, Resolve MPD candidates from /player/api using hidden input #e_id.         Return (+4 more)

### Community 5 - "Community 5"
Cohesion: 0.09
Nodes (33): Console, _atomic_write_utf8(), _box_named(), _fmt_bytes(), _load_last_session(), _make_console(), _merge_with_file_lines(), _persist_session_link_files() (+25 more)

### Community 6 - "Community 6"
Cohesion: 0.06
Nodes (36): fail_guest, logging_in, mask_suffix, no_creds, ok, status, exported, hint_line (+28 more)

### Community 7 - "Community 7"
Cohesion: 0.06
Nodes (36): border_style, box_style, padding, subtitle_template, title_primary, title_primary_style, title_secondary, title_secondary_style (+28 more)

### Community 8 - "Community 8"
Cohesion: 0.06
Nodes (33): subtitle_template, title_primary, title_secondary, generic, cookie_export_probe_urls, format, format_fallbacks, output_dir_default (+25 more)

### Community 9 - "Community 9"
Cohesion: 0.05
Nodes (38): generic, concurrent_fragment_downloads, cookie_export_probe_urls, embed_first_hosts, embed_resolve_before_ytdlp, error_message_max_len, extractor_retries, fallback_media_extensions (+30 more)

### Community 10 - "Community 10"
Cohesion: 0.07
Nodes (27): 1) Masuk ke folder proyek, 2) Jalankan starter script (recommended), 2b) Jalankan GUI, 3) Siapkan URL, Alur singkat, Apa ini?, Build singlefile executable, code:bash (cd HDL-Flux) (+19 more)

### Community 11 - "Community 11"
Cohesion: 0.09
Nodes (23): ui, auth_mask_chars, comment_prefix, cookie_source_choices, cookie_source_default, format_choices, format_default, hstream_cuda_choices (+15 more)

### Community 12 - "Community 12"
Cohesion: 0.18
Nodes (22): Any, deobfuscate_embedded_json(), extract_embed_page_urls(), extract_jwplayer_media_urls(), extract_jwplayer_with_embeds(), _is_bait_url(), page_uses_jwplayer(), bool (+14 more)

### Community 13 - "Community 13"
Cohesion: 0.10
Nodes (20): action, cookie_close_default, cookie_close_named, cookie_path, cookie_source, delete_chunks, generic_output_dir, hstream_browser (+12 more)

### Community 14 - "Community 14"
Cohesion: 0.12
Nodes (16): browser_detection, darwin, fallback_other, linux, windows, command, fallback, rules (+8 more)

### Community 15 - "Community 15"
Cohesion: 0.12
Nodes (16): messages, browser_start_failed, chunks_missing_template, ffmpeg_not_found, login_error, login_still_on_page, login_timeout, mpd_not_found (+8 more)

### Community 16 - "Community 16"
Cohesion: 0.27
Nodes (11): extract_fallback_media_urls(), join_media_url(), media_priority(), media_quality_rank(), int, str, Shared HTML scraping for direct stream / file URLs (m3u8, mpd, mp4, …). Used by, Estimated quality score (higher = better).     Adaptive HLS/DASH manifests rank (+3 more)

### Community 17 - "Community 17"
Cohesion: 0.18
Nodes (11): browser, chunks, format, headless, method, output, proxy, quality (+3 more)

### Community 18 - "Community 18"
Cohesion: 0.42
Nodes (9): bool, int, Path, str, bin_name(), main(), prepare_windows_ico(), run() (+1 more)

### Community 19 - "Community 19"
Cohesion: 0.20
Nodes (10): bar_width, between_tasks_sleep_sec, fail_prefix, ok_prefix, overall_label, progress_cap_pct, summary_error_display_max, summary_ok_title_max (+2 more)

### Community 20 - "Community 20"
Cohesion: 0.29
Nodes (7): netscape_cookie_file_loads(), bool, Path, Netscape-format cookie files for yt-dlp / requests (MozillaCookieJar)., Return True if path looks like a valid Netscape cookie jar yt-dlp can read., Write Selenium-style cookie dicts to a Netscape cookie file (yt-dlp compatible)., write_netscape_from_browser_cookies()

### Community 21 - "Community 21"
Cohesion: 0.29
Nodes (7): site_host_keywords, pornhub, redtube, xhamster, xnxx, xvideos, youtube

### Community 22 - "Community 22"
Cohesion: 0.29
Nodes (7): ytdlp, ffmpeg_convertor_key, format_best_single, format_error_substring, format_fallback_chain, format_height_template, unsupported_url_substring

### Community 23 - "Community 23"
Cohesion: 0.33
Nodes (6): site_referers, pornhub, redtube, xhamster, xnxx, xvideos

### Community 24 - "Community 24"
Cohesion: 0.33
Nodes (6): method_chunks, method_ytdlp, no, proxy_off, yes, recap_values

### Community 26 - "Community 26"
Cohesion: 0.50
Nodes (4): headers_common, Accept, Accept-Language, Sec-Fetch-Mode

### Community 27 - "Community 27"
Cohesion: 0.50
Nodes (4): quality_resolution, 1080, 2160, 720

### Community 28 - "Community 28"
Cohesion: 0.50
Nodes (4): selenium_selectors, email, password, submit

### Community 29 - "Community 29"
Cohesion: 0.67
Nodes (3): youtube, extractor_args, player_client

### Community 33 - "Community 33"
Cohesion: 0.40
Nodes (12): _apply_webdriver_timeouts(), _common_chromium_args(), create_selenium_driver(), default_browser_ytdlp_name(), export_cookies_ytdlp(), kill_browser_for_cookies(), _match_browser_rules(), AppConfig (+4 more)

### Community 37 - "Community 37"
Cohesion: 0.36
Nodes (10): _host_of(), is_plausible_embed_url(), page_needs_embed_resolve(), prioritize_embed_urls(), bool, int, str, Resolve direct stream URLs from pages that use external embed players (not JW-on (+2 more)

## Knowledge Gaps
- **368 isolated node(s):** `readme`, `list_file`, `state_file`, `cookies_cache_file`, `mode_choices` (+363 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ui` connect `Community 11` to `Community 3`, `Community 7`, `Community 13`, `Community 17`, `Community 19`, `Community 24`?**
  _High betweenness centrality (0.128) - this node is a cross-community bridge._
- **Why does `hstream` connect `Community 0` to `Community 3`, `Community 15`, `Community 22`, `Community 27`, `Community 28`?**
  _High betweenness centrality (0.122) - this node is a cross-community bridge._
- **Why does `generic` connect `Community 9` to `Community 3`, `Community 21`, `Community 23`, `Community 26`, `Community 29`?**
  _High betweenness centrality (0.077) - this node is a cross-community bridge._
- **Are the 17 inferred relationships involving `HStreamDownloader` (e.g. with `Console` and `float`) actually correct?**
  _`HStreamDownloader` has 17 INFERRED edges - model-reasoned connections that need verification._
- **Are the 29 inferred relationships involving `AppConfig` (e.g. with `AppConfig` and `bool`) actually correct?**
  _`AppConfig` has 29 INFERRED edges - model-reasoned connections that need verification._
- **Are the 16 inferred relationships involving `StateManager` (e.g. with `Console` and `float`) actually correct?**
  _`StateManager` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `readme`, `list_file`, `state_file` to the rest of the system?**
  _401 weakly-connected nodes found - possible documentation gaps or missing edges._