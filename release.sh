#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  echo "Usage: ./release.sh vX.Y.Z"
  exit 1
fi

if [[ "$VERSION" != v* ]]; then
  echo "Error: version tag must start with 'v' (example: v1.0.0)"
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "Error: git not found."
  exit 1
fi

if ! git remote get-url origin >/dev/null 2>&1; then
  echo "Error: git remote 'origin' not found."
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Error: working tree is not clean. Commit or stash changes first."
  exit 1
fi

if git rev-parse "$VERSION" >/dev/null 2>&1; then
  echo "Error: tag '$VERSION' already exists locally."
  exit 1
fi

if git ls-remote --exit-code --tags origin "refs/tags/$VERSION" >/dev/null 2>&1; then
  echo "Error: tag '$VERSION' already exists on origin."
  exit 1
fi

echo "[release] creating tag $VERSION"
git tag -a "$VERSION" -m "Release $VERSION"

echo "[release] pushing current branch"
git push origin HEAD

echo "[release] pushing tag $VERSION"
git push origin "$VERSION"

echo
echo "Done. GitHub Actions release workflow should start automatically for tag '$VERSION'."
