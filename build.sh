#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="${SRC_DIR:-/opt/lex-src}"
VENV_DIR="${VENV_DIR:-/opt/lex-venv}"
BIN_LINK="${BIN_LINK:-/usr/local/bin/lex}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command not found: $1" >&2
    exit 1
  fi
}

run_as_root() {
  if [[ "${EUID}" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

need_cmd rsync
need_cmd "$PYTHON_BIN"
need_cmd sudo

if [[ ! -f "$REPO_ROOT/pyproject.toml" ]]; then
  echo "Error: build.sh must be run from the lex repository root." >&2
  exit 1
fi

echo "Syncing repository to $SRC_DIR"
run_as_root mkdir -p "$SRC_DIR"
run_as_root rsync -a --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.mypy_cache/' \
  --exclude '.ruff_cache/' \
  "$REPO_ROOT/" "$SRC_DIR/"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  echo "Creating virtual environment at $VENV_DIR"
  run_as_root "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

echo "Upgrading packaging tools"
run_as_root "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel

echo "Installing lex in editable mode"
run_as_root "$VENV_DIR/bin/pip" install --upgrade -e "$SRC_DIR"

if [[ -e "$BIN_LINK" && ! -L "$BIN_LINK" ]]; then
  echo "Error: $BIN_LINK exists and is not a symlink. Refusing to overwrite." >&2
  exit 1
fi

echo "Linking executable to $BIN_LINK"
run_as_root ln -sfn "$VENV_DIR/bin/lex" "$BIN_LINK"

echo
echo "Done."
echo "Source:     $SRC_DIR"
echo "Venv:       $VENV_DIR"
echo "Executable: $BIN_LINK"
echo
echo "Try: lex --help"
