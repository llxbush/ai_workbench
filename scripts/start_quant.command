#!/bin/zsh
set -e

cd "$(dirname "$0")/.."

export QUANT_DATABASE_URL="${QUANT_DATABASE_URL:-mysql+pymysql://root:1234@127.0.0.1:3306/quant?charset=utf8mb4}"

if curl -sS --max-time 2 http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
  open http://127.0.0.1:8000
  exit 0
fi

open http://127.0.0.1:8000
exec .venv/bin/python main.py --mode serve --host 127.0.0.1 --port 8000
