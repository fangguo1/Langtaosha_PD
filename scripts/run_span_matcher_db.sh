#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export SPAN_MATCHES="${SPAN_MATCHES:-1}"
export ONTOLOGY_LINKER_URL="${ONTOLOGY_LINKER_URL:-http://127.0.0.1:8765}"
export ONTOLOGY_SOURCE_LIST="${ONTOLOGY_SOURCE_LIST:-umls,mesh}"
export CONFIG_PATH="${CONFIG_PATH:-src/config/config_tecent_backend_server_mimic.yaml}"
export PAPER_SOURCES="${PAPER_SOURCES:-langtaosha,biorxiv_history,biorxiv_daily}"

if [[ "${1:-}" == "--trace" ]]; then
  shift
  exec python3 scripts/run_span_matcher_trace.py \
    --config-path "$CONFIG_PATH" \
    --paper-source-list "$PAPER_SOURCES" \
    --ontology-linker-url "$ONTOLOGY_LINKER_URL" \
    --ontology-source-list "$ONTOLOGY_SOURCE_LIST" \
    "$@"
fi

exec scripts/run_query_analyzer_db.sh "$@"
