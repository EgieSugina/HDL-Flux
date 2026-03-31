#!/usr/bin/env bash
# Create .venv if missing, sync requirements, run serve.py
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

VENV="$ROOT/.venv"
REQ="$ROOT/requirements.txt"
MAIN="$ROOT/serve.py"

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 not found. Install Python 3.10+ and try again." >&2
  exit 1
fi

if [[ ! -d "$VENV" ]]; then
  echo "[start] Creating virtual environment at .venv …"
  python3 -m venv "$VENV"
fi

# shellcheck disable=SC1091
source "$VENV/bin/activate"

if [[ ! -f "$REQ" ]]; then
  echo "Error: requirements.txt not found in $ROOT" >&2
  exit 1
fi

echo "[start] Installing / checking dependencies …"
pip install --upgrade pip setuptools wheel -q
pip install -q -r "$REQ"

if [[ ! -f "$MAIN" ]]; then
  echo "Error: serve.py not found in $ROOT" >&2
  exit 1
fi

echo "[start] Launching serve.py …"
exec python "$MAIN" "$@"
