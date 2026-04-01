from __future__ import annotations

import copy
import json
import os
import re
import shutil
import sys
from pathlib import Path


def _runtime_root() -> Path:
    # PyInstaller onefile: prefer folder of executable for user-editable config,
    # but keep bundled resources in _MEIPASS as fallback.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


SCRIPT_DIR = _runtime_root()


def load_json_file(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_dotenv_simple(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def _set_nested(d: dict, keys: tuple[str, ...], value):
    cur = d
    for k in keys[:-1]:
        cur = cur.setdefault(k, {})
    cur[keys[-1]] = value


def _apply_hdl_env_overrides(data: dict) -> None:
    strings = [
        ("HDL_PROXY_DEFAULT_URL", ("proxy", "default_url")),
        ("HDL_PATHS_BASE_DIR", ("paths", "base_dir")),
        ("HDL_PATHS_DOWNLOADS_SUBDIR", ("paths", "downloads_subdir")),
        ("HDL_PATHS_VIDEOS_SUBDIR", ("paths", "videos_subdir")),
        ("HDL_PATHS_STATE_FILE", ("paths", "state_file")),
        ("HDL_PATHS_LAST_SESSION_FILE", ("paths", "last_session_file")),
        ("HDL_PATHS_LIST_FILE", ("paths", "list_file")),
        ("HDL_PATHS_CLEAN_LIST_FILE", ("paths", "clean_list_file")),
        ("HDL_PATHS_FAILED_LINKS_FILE", ("paths", "failed_links_file")),
        ("HDL_PATHS_SUCCESS_LINKS_FILE", ("paths", "success_links_file")),
        ("HDL_PATHS_CREDENTIALS_FILE", ("paths", "credentials_file")),
        ("HDL_PATHS_COOKIES_FILE", ("paths", "cookies_file")),
        ("HDL_PATHS_COOKIES_CACHE_FILE", ("paths", "cookies_cache_file")),
        ("HDL_HSTREAM_BASE_URL", ("hstream", "base_url")),
        ("HDL_HSTREAM_LOGIN_PATH", ("hstream", "login_path")),
        ("HDL_HSTREAM_REFERER", ("hstream", "referer")),
        ("HDL_HSTREAM_ORIGIN", ("hstream", "origin")),
        ("HDL_GENERIC_OUTPUT_DIR_DEFAULT", ("generic", "output_dir_default")),
    ]
    for env_key, keypath in strings:
        v = os.environ.get(env_key)
        if v is not None and v != "":
            _set_nested(data, keypath, v)

    ints = [
        ("HDL_UI_WORKERS_DEFAULT", ("ui", "workers_default")),
        ("HDL_UI_WORKERS_MIN", ("ui", "workers_min")),
        ("HDL_UI_WORKERS_MAX", ("ui", "workers_max")),
        ("HDL_HSTREAM_MAX_RETRIES", ("hstream", "max_retries")),
        ("HDL_HSTREAM_REQUEST_TIMEOUT_SEC", ("hstream", "request_timeout_sec")),
        ("HDL_GENERIC_MAX_RETRIES", ("generic", "max_retries")),
        ("HDL_GENERIC_RETRY_DELAY_SEC", ("generic", "retry_delay_sec")),
    ]
    for env_key, keypath in ints:
        v = os.environ.get(env_key)
        if v is not None and v != "":
            try:
                _set_nested(data, keypath, int(v))
            except ValueError:
                pass
    skip_dirs_raw = os.environ.get("HDL_PATHS_SKIP_IF_EXISTS_DIRS", "").strip()
    if skip_dirs_raw:
        items = [x.strip() for x in re.split(r"[,\n]+", skip_dirs_raw) if x.strip()]
        if items:
            _set_nested(data, ("paths", "skip_if_exists_dirs"), items)


class AppConfig:
    def __init__(self, data: dict, script_dir: Path):
        self._d = data
        self.script_dir = script_dir
        p = data["paths"]
        base = p.get("base_dir", "__script__")
        if base in ("__script__", ".", ""):
            root = script_dir
        else:
            root = Path(base).expanduser().resolve()
        self.root_dir = root
        self.downloads_dir = root / p["downloads_subdir"]
        self.videos_dir = root / p["videos_subdir"]
        self.state_file = root / p["state_file"]
        self.last_session_file = root / p.get("last_session_file", ".last_session.json")
        self.list_file = root / p["list_file"]
        self.clean_list_file = root / p.get("clean_list_file", "clean_url.txt")
        self.failed_links_file = root / p.get("failed_links_file", "failed_links.txt")
        self.success_links_file = root / p.get("success_links_file", "success_links.txt")
        self.credentials_file = root / p["credentials_file"]
        self.cookies_file = root / p["cookies_file"]
        self.cookies_cache_file = root / p["cookies_cache_file"]
        self.phantomjs_name = p.get("phantomjs_name", "phantomjs.exe")
        self.link_files_merge = bool(p.get("link_files_merge", True))
        skip_dirs = p.get("skip_if_exists_dirs", [])
        self.skip_if_exists_dirs: list[Path] = [self.videos_dir]
        if isinstance(skip_dirs, list):
            for raw in skip_dirs:
                s = str(raw).strip()
                if not s:
                    continue
                pp = Path(s).expanduser()
                pp = (root / pp).resolve() if not pp.is_absolute() else pp.resolve()
                if pp not in self.skip_if_exists_dirs:
                    self.skip_if_exists_dirs.append(pp)

    @property
    def proxy_default_url(self) -> str:
        return self._d["proxy"]["default_url"]

    @property
    def ui(self) -> dict:
        return self._d["ui"]

    @property
    def h(self) -> dict:
        return self._d["hstream"]

    @property
    def g(self) -> dict:
        return self._d["generic"]

    @property
    def selenium_cfg(self) -> dict:
        return self._d["selenium"]

    @property
    def browser_kill_cfg(self) -> dict:
        return self._d["browser_kill"]

    def hstream_quality_res(self) -> dict[str, tuple[int, int]]:
        return {k: tuple(v) for k, v in self.h["quality_resolution"].items()}

    def text(self, *keys: str, default: str = "", **kwargs) -> str:
        cur: object = self._d.get("strings", {})
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k, "")
        if not isinstance(cur, str):
            return default
        try:
            return cur.format(**kwargs) if kwargs else cur
        except KeyError:
            return default or cur

    def h_msg(self, key: str, **kwargs) -> str:
        cur = self.h.get("messages", {}).get(key, key)
        if not isinstance(cur, str):
            return str(cur)
        try:
            return cur.format(**kwargs) if kwargs else cur
        except KeyError:
            return cur

    def ytdlp_cfg(self) -> dict:
        return self.h["ytdlp"]

    def prompt_label(self, key: str, **kwargs) -> str:
        s = str(self.ui.get("prompts", {}).get(key, key))
        try:
            return s.format(**kwargs) if kwargs else s
        except KeyError:
            return s


def load_config(script_dir: Path | None = None) -> AppConfig:
    script_dir = script_dir or SCRIPT_DIR
    load_dotenv_simple(script_dir / ".env")
    defaults_path = script_dir / "config.defaults.json"
    if not defaults_path.is_file() and getattr(sys, "frozen", False):
        meipass = Path(getattr(sys, "_MEIPASS", ""))
        mp_defaults = meipass / "config.defaults.json"
        if mp_defaults.is_file():
            defaults_path = mp_defaults
    if not defaults_path.is_file():
        raise FileNotFoundError(
            "Missing {name} next to the script (required defaults).".format(
                name=defaults_path.name
            )
        )
    defaults = load_json_file(defaults_path)
    env_config = os.environ.get("HDL_CONFIG", "").strip()
    config_path = Path(env_config) if env_config else script_dir / "config.json"
    if config_path.is_file():
        user = load_json_file(config_path)
        data = _deep_merge(defaults, user)
    elif env_config:
        data = copy.deepcopy(defaults)
    else:
        shutil.copy(defaults_path, script_dir / "config.json")
        data = load_json_file(script_dir / "config.json")
    _apply_hdl_env_overrides(data)
    return AppConfig(data, script_dir)


def apply_phantomjs_path(cfg: AppConfig) -> None:
    if os.name != "nt":
        return
    exe = cfg.script_dir / cfg.phantomjs_name
    if exe.is_file():
        os.environ["PATH"] = str(cfg.script_dir) + os.pathsep + os.environ.get(
            "PATH", ""
        )

