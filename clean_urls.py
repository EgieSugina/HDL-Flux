#!/usr/bin/env python3
from __future__ import annotations

from hdl.config import load_config
from hdl import routing


class PlainConsole:
    @staticmethod
    def print(*args, **kwargs):
        print(*args)


def main():
    cfg = load_config()
    routing.rebuild_clean_url_file(cfg, PlainConsole(), cfg.list_file)


if __name__ == "__main__":
    main()
