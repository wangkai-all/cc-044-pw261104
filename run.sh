#!/usr/bin/env bash
# Start the demo: API + export worker (in-process) + separate writer process.
# Frontend is served from web/dist when it exists; run `cd web && npm run build`
# once to produce it, or use `cd web && npm run dev` for the dev server.
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python
if [ ! -x "$PY" ]; then
  python3 -m venv .venv
  . .venv/bin/activate
  pip install -r requirements.txt
  PY=.venv/bin/python
fi
PORT="${PORT:-8765}"
exec "$PY" -m uvicorn app.api:app --host 0.0.0.0 --port "$PORT" "$@"
