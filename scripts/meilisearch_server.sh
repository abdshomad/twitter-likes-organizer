#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${ROOT_DIR}/bin/meilisearch"
PID_FILE="${ROOT_DIR}/meilisearch.pid"
LOG_FILE="${ROOT_DIR}/meilisearch.log"
DB_PATH="${ROOT_DIR}/data/meilisearch_data.ms"
PORT="${MEILI_PORT:-7700}"
HOST="${MEILI_HOST:-127.0.0.1}"

ensure_binary() {
  if [ ! -f "${BIN_PATH}" ]; then
    echo "[Meilisearch] Binary not found at ${BIN_PATH}. Downloading..."
    mkdir -p "${ROOT_DIR}/bin"
    (cd "${ROOT_DIR}/bin" && curl -sSL https://install.meilisearch.com | sh)
  fi
}

start_server() {
  ensure_binary
  if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
    if kill -0 "${PID}" 2>/dev/null; then
      echo "[Meilisearch] Server is already running (PID: ${PID}) on http://${HOST}:${PORT}"
      return 0
    else
      rm -f "${PID_FILE}"
    fi
  fi

  mkdir -p "$(dirname "${DB_PATH}")"
  echo "[Meilisearch] Starting server on http://${HOST}:${PORT}..."
  setsid "${BIN_PATH}" \
    --http-addr "${HOST}:${PORT}" \
    --db-path "${DB_PATH}" \
    --no-analytics \
    < /dev/null > "${LOG_FILE}" 2>&1 &
  
  PID=$!
  echo "${PID}" > "${PID_FILE}"
  sleep 1.5

  if kill -0 "${PID}" 2>/dev/null; then
    echo "[Meilisearch] Successfully started (PID: ${PID}) on http://${HOST}:${PORT}"
  else
    echo "[Meilisearch] Failed to start. Check logs at ${LOG_FILE}"
    exit 1
  fi
}

stop_server() {
  if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
    if kill -0 "${PID}" 2>/dev/null; then
      echo "[Meilisearch] Stopping server (PID: ${PID})..."
      kill "${PID}" || true
      sleep 1
    fi
    rm -f "${PID_FILE}"
    echo "[Meilisearch] Server stopped."
  else
    echo "[Meilisearch] No running PID file found."
  fi
}

status_server() {
  if [ -f "${PID_FILE}" ]; then
    PID=$(cat "${PID_FILE}")
    if kill -0 "${PID}" 2>/dev/null; then
      echo "[Meilisearch] RUNNING (PID: ${PID}) on http://${HOST}:${PORT}"
      curl -s "http://${HOST}:${PORT}/health" || true
      echo ""
      return 0
    fi
  fi
  echo "[Meilisearch] STOPPED"
}

case "$1" in
  start)
    start_server
    ;;
  stop)
    stop_server
    ;;
  restart)
    stop_server
    start_server
    ;;
  status)
    status_server
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac
