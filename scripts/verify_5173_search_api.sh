#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:5173}"
QUERY="${QUERY:-Nav1.7}"
MODE="${MODE:-smart}"
TOP_K="${TOP_K:-3}"
SOURCE_LIST="${SOURCE_LIST:-langtaosha}"
TOKEN="${API_AUTH_TOKEN:-${API_AUTH_TOKENS:-}}"

auth_args=()
if [[ -n "$TOKEN" ]]; then
  first_token="${TOKEN%%,*}"
  auth_args=(-H "Authorization: Bearer ${first_token}")
fi

echo "== Langtaosha 5173 API local verification =="
echo "BASE_URL=${BASE_URL}"
echo "QUERY=${QUERY}"
echo "MODE=${MODE}"
echo "TOP_K=${TOP_K}"
echo "SOURCE_LIST=${SOURCE_LIST}"
if [[ -n "$TOKEN" ]]; then
  echo "AUTH=Bearer token enabled"
else
  echo "AUTH=no token header"
fi
echo

echo "== 1. health =="
curl -sS -i \
  "${BASE_URL}/api/health"
echo
echo

echo "== 2. ready =="
curl -sS -i \
  "${auth_args[@]}" \
  "${BASE_URL}/api/ready"
echo
echo

echo "== 3. search =="
python3 - "$BASE_URL" "$QUERY" "$MODE" "$TOP_K" "$SOURCE_LIST" "${auth_args[@]}" <<'PY'
import json
import subprocess
import sys
from urllib.parse import urlencode

base_url, query, mode, top_k, source_list, *auth_args = sys.argv[1:]
params = urlencode({
    "query": query,
    "mode": mode,
    "top_k": top_k,
    "source_list": source_list,
})
url = f"{base_url}/api/scholar/search?{params}"
cmd = ["curl", "-sS", "-i", *auth_args, url]
result = subprocess.run(cmd, check=True, text=True, capture_output=True)
print(result.stdout)

header_text, _, body_text = result.stdout.partition("\r\n\r\n")
if not body_text:
    header_text, _, body_text = result.stdout.partition("\n\n")

try:
    payload = json.loads(body_text)
except json.JSONDecodeError:
    sys.exit("Search response is not valid JSON")

print("== search summary ==")
print(json.dumps({
    "success": payload.get("success"),
    "count": payload.get("count"),
    "request_id": payload.get("request_id"),
    "first_title": (payload.get("results") or [{}])[0].get("title"),
}, ensure_ascii=False, indent=2))

if not payload.get("success"):
    sys.exit("Search API returned success=false")
PY
