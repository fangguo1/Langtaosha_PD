#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${CONFIG_PATH:-src/config/config_tecent_backend_server_mimic.yaml}"
PAPER_SOURCES="${PAPER_SOURCES:-langtaosha,biorxiv_history,biorxiv_daily}"
SCISPACY_MODEL="${SCISPACY_MODEL:-en_core_sci_lg}"

EXAMPLE_SCRIPT="$ROOT_DIR/examples/query_analyzer_example.py"
if [[ ! -f "$EXAMPLE_SCRIPT" ]]; then
  PARENT_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
  FALLBACK_EXAMPLE_SCRIPT="$PARENT_ROOT/examples/query_analyzer_example.py"
  if [[ -f "$FALLBACK_EXAMPLE_SCRIPT" ]]; then
    EXAMPLE_SCRIPT="$FALLBACK_EXAMPLE_SCRIPT"
  else
    echo "Could not find query_analyzer_example.py under '$ROOT_DIR/examples' or '$PARENT_ROOT/examples'." >&2
    exit 1
  fi
fi

args=(
  "$EXAMPLE_SCRIPT"
  --use-db
  --config-path "$CONFIG_PATH"
  --paper-source-list "$PAPER_SOURCES"
  --scispacy-model "$SCISPACY_MODEL"
)

if [[ "${SKIP_SCISPACY:-0}" == "1" ]]; then
  args+=(--skip-scispacy)
fi

if [[ "${JSON_OUTPUT:-0}" == "1" ]]; then
  args+=(--json)
fi

if [[ "${SPAN_MATCHES:-0}" == "1" ]]; then
  args+=(--span-matches)
fi

if [[ -n "${ONTOLOGY_LINKER_URL:-}" ]]; then
  args+=(--ontology-linker-url "$ONTOLOGY_LINKER_URL")
fi

if [[ -n "${ONTOLOGY_SOURCE_LIST:-}" ]]; then
  args+=(--ontology-source-list "$ONTOLOGY_SOURCE_LIST")
fi

if [[ -n "${ONTOLOGY_TOP_K:-}" ]]; then
  args+=(--ontology-top-k "$ONTOLOGY_TOP_K")
fi

if [[ -n "${ONTOLOGY_THRESHOLD:-}" ]]; then
  args+=(--ontology-threshold "$ONTOLOGY_THRESHOLD")
fi

if [[ "${NO_SUBPHRASE_NGRAM:-0}" == "1" ]]; then
  args+=(--no-subphrase-ngram)
fi

if [[ -n "${KEYWORD_SOURCE:-}" ]]; then
  IFS=',' read -ra keyword_sources <<< "$KEYWORD_SOURCE"
  for source in "${keyword_sources[@]}"; do
    [[ -n "$source" ]] && args+=(--keyword-source "$source")
  done
fi

if [[ "$#" -gt 0 ]]; then
  args+=(--query "$*")
fi

python3 "${args[@]}"
