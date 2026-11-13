#!/usr/bin/env bash
# 端到端演示：上传 -> 轮询报告 -> 取消演示。
# 依赖：curl、jq（可选，没有 jq 时输出原始 JSON）。
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
HERE="$(cd "$(dirname "$0")" && pwd)"
DB="${1:-$HERE/sample_ctms.db}"
RULES="${2:-$HERE/rules.sample.json}"

if [[ ! -f "$DB" ]]; then
  python3 "$HERE/make_sample_db.py" "$DB"
fi

pp() { if command -v jq >/dev/null 2>&1; then jq .; else cat; fi; }

echo "==> 1) 健康检查"
curl -fsS "$BASE/health" | pp

echo "==> 2) 创建核查批次（后台运行，立即返回批次 id）"
create_resp="$(curl -fsS -X POST "$BASE/batches" \
  -F "db_file=@$DB;type=application/x-sqlite3" \
  -F "rules=< $RULES" \
  -F "name=demo-$(date -u +%Y%m%dT%H%M%SZ)")"
echo "$create_resp" | pp
bid="$(printf '%s' "$create_resp" | \
  { command -v jq >/dev/null 2>&1 && jq -r .id || python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'; })"

echo "==> 3) 轮询完整报告（各规则结论 / 违规总数 / 前 20 行）"
for _ in $(seq 1 20); do
  sleep 1
  detail="$(curl -fsS "$BASE/batches/$bid")"
  status="$(printf '%s' "$detail" | \
    { command -v jq >/dev/null 2>&1 && jq -r .status || python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])'; })"
  echo "    status=$status"
  [[ "$status" != "queued" && "$status" != "running" ]] && break
done
echo "$detail" | pp

echo "==> 4) 取消演示（对含慢查询的新批次发起取消）"
cancel_resp="$(curl -fsS -X POST "$BASE/batches" \
  -F "db_file=@$DB;type=application/x-sqlite3" \
  -F "rules=< $RULES" \
  -F "name=cancel-demo")"
cbid="$(printf '%s' "$cancel_resp" | \
  { command -v jq >/dev/null 2>&1 && jq -r .id || python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'; })"
sleep 1
curl -fsS -X POST "$BASE/batches/$cbid/cancel" | pp
sleep 2
echo "--- 被取消批次的最终报告（注意 can_load 必为 false）---"
curl -fsS "$BASE/batches/$cbid" | pp

echo "==> 5) 批次列表"
curl -fsS "$BASE/batches" | pp
