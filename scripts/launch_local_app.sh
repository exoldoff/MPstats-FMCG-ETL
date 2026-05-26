#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$ROOT_DIR/web"
PYTHON_BIN="${PYTHON_BIN:-python3}"
HOST="${MPSTATS_APP_HOST:-127.0.0.1}"
PORT_START="${MPSTATS_APP_PORT:-8000}"
PORT_END="${MPSTATS_APP_PORT_END:-8010}"

cd "$ROOT_DIR"

log() {
  printf "\033[1;32m[MPStats]\033[0m %s\n" "$1"
}

fail() {
  printf "\033[1;31m[MPStats]\033[0m %s\n" "$1" >&2
  printf "\nНажми Enter, чтобы закрыть окно..." >&2
  read -r _ || true
  exit 1
}

need_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Не найдена команда '$1'."
}

port_is_free() {
  "$PYTHON_BIN" - "$1" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.bind(("127.0.0.1", port))
except OSError:
    sys.exit(1)
finally:
    sock.close()
PY
}

pick_port() {
  local port
  for port in $(seq "$PORT_START" "$PORT_END"); do
    if port_is_free "$port"; then
      printf "%s" "$port"
      return 0
    fi
  done
  return 1
}

frontend_needs_build() {
  if [[ ! -f "$WEB_DIR/dist/index.html" ]]; then
    return 0
  fi
  if [[ "$WEB_DIR/package.json" -nt "$WEB_DIR/dist/index.html" ]]; then
    return 0
  fi
  if [[ -f "$WEB_DIR/package-lock.json" && "$WEB_DIR/package-lock.json" -nt "$WEB_DIR/dist/index.html" ]]; then
    return 0
  fi
  if find "$WEB_DIR/src" -type f -newer "$WEB_DIR/dist/index.html" -print -quit | grep -q .; then
    return 0
  fi
  return 1
}

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    log "Останавливаю backend..."
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

need_command "$PYTHON_BIN"
need_command npm

log "Проверяю Python-зависимости..."
"$PYTHON_BIN" - <<'PY' || fail "Не хватает Python-зависимостей. Выполни: python3 -m pip install -r requirements.txt"
import fastapi
import uvicorn
PY

if [[ ! -d "$WEB_DIR/node_modules" ]]; then
  log "Устанавливаю frontend-зависимости..."
  (cd "$WEB_DIR" && npm install) || fail "npm install завершился с ошибкой."
fi

if frontend_needs_build; then
  log "Собираю frontend..."
  (cd "$WEB_DIR" && npm run build) || fail "npm run build завершился с ошибкой."
fi

PORT="$(pick_port)" || fail "Не нашёл свободный порт в диапазоне $PORT_START-$PORT_END."
URL="http://$HOST:$PORT"

log "Запускаю backend на $URL"
"$PYTHON_BIN" -m uvicorn mpstats_app.main:app --host "$HOST" --port "$PORT" &
SERVER_PID="$!"

READY=0
for _ in $(seq 1 40); do
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    fail "Backend завершился раньше времени."
  fi
  if "$PYTHON_BIN" - "$URL" <<'PY' >/dev/null 2>&1
from urllib.request import urlopen
import sys

with urlopen(sys.argv[1] + "/api/health", timeout=0.5) as response:
    if response.status != 200:
        raise SystemExit(1)
PY
  then
    READY=1
    break
  fi
  sleep 0.25
done

if [[ "$READY" != "1" ]]; then
  fail "Backend не ответил на /api/health."
fi

log "Открываю приложение..."
open "$URL"

printf "\nMPStats Local App работает: %s\n" "$URL"
printf "Закрой это окно или нажми Ctrl+C, чтобы остановить backend.\n\n"

wait "$SERVER_PID"
