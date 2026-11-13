"""FastAPI 入口：上传核查、报告查询、批次取消。

设计约束（对应需求）：
- 上传库保存为独立副本，全程只读打开，服务绝不修改上传库；
- 核查在后台线程池运行，API 立即返回 batch_id；
- 运行期间其他批次仍可提交 / 查询（批次间并行、I/O 路由不阻塞事件循环）；
- 仅当批次 completed 且每条规则都零行通过时 can_load=true；
- 取消 / 超时 / 中断一律不可放行；
- 报告落控制库，重启后完整可查，未完成批次重启时标记 interrupted。
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from . import __version__, config, runner, storage, validation
from .schemas import (
    BatchDetail,
    BatchSummary,
    CancelResponse,
    HealthResponse,
    RuleResultOut,
)

# 启动时记录被标记为中断的批次，供 /health 观测
_startup_interrupted: list[str] = []


@asynccontextmanager
async def lifespan(_: FastAPI):
    config.uploads_dir()
    storage.init_db()
    global _startup_interrupted
    _startup_interrupted = storage.reset_unfinished_on_startup()
    yield


app = FastAPI(
    title="临床试验数据核查服务",
    description=(
        "上传含 subjects / visits 表的 SQLite 副本与具名只读核查 SQL，"
        "后台执行规则并持久化报告。每条规则限 5 秒，超时实际中断且不算通过；"
        "支持取消整批；仅全部规则零违规才给出可入仓结论。"
    ),
    version=__version__,
    lifespan=lifespan,
)


# ---------------- 报告组装 ----------------

def _rule_out(row: Any) -> RuleResultOut:
    sample = row["sample_rows"]
    return RuleResultOut(
        seq=row["seq"],
        rule_name=row["rule_name"],
        sql_text=row["sql_text"],
        status=row["status"],
        violation_count=row["violation_count"],
        sample_rows=json.loads(sample) if sample else None,
        error_message=row["error_message"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def _summarize(batch_row: Any, rule_rows: list[Any], detail: bool) -> dict[str, Any]:
    total = 0
    complete = True
    passed = 0
    failed = 0
    rules_out: list[RuleResultOut] = []
    for row in rule_rows:
        out = _rule_out(row)
        rules_out.append(out)
        if out.status == storage.RULE_PASSED:
            passed += 1
            total += out.violation_count or 0
        elif out.status == storage.RULE_VIOLATIONS:
            failed += 1
            total += out.violation_count or 0
        else:
            # timeout / error / cancelled / interrupted：计数不可信
            failed += 1
            complete = False

    can_load = (
        batch_row["status"] == storage.BATCH_COMPLETED
        and len(rule_rows) > 0
        and all(r.status == storage.RULE_PASSED for r in rules_out)
    )

    payload: dict[str, Any] = dict(
        id=batch_row["id"],
        name=batch_row["name"],
        source_filename=batch_row["source_filename"],
        status=batch_row["status"],
        can_load=can_load,
        total_violations=total,
        total_violations_complete=complete,
        rule_count=len(rule_rows),
        passed_rules=passed,
        failed_rules=failed,
        created_at=batch_row["created_at"],
        started_at=batch_row["started_at"],
        finished_at=batch_row["finished_at"],
        error=batch_row["error"],
    )
    if detail:
        payload["rules"] = rules_out
        payload["db_path"] = batch_row["db_path"]
    return payload


# ---------------- 路由 ----------------

@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health", response_model=HealthResponse, tags=["运维"])
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        version=__version__,
        interrupted_on_startup=_startup_interrupted,
    )


@app.post(
    "/batches",
    response_model=BatchDetail,
    status_code=201,
    tags=["核查批次"],
    summary="上传 SQLite 副本与具名只读查询，创建后台核查批次",
)
async def create_batch(
    db_file: UploadFile = File(
        ...,
        description=(
            "SQLite 数据库副本，须包含 subjects(id, enrolled_on) 与 "
            "visits(id, subject_id, visited_on) 表"
        ),
    ),
    rules: str = Form(
        ...,
        description=(
            'JSON 数组字符串，形如 [{"name":"规则名","sql":"SELECT ..."}]，'
            "仅允许单条 SELECT/WITH 只读查询"
        ),
    ),
    name: Optional[str] = Form(
        default=None, description="批次名称，缺省用上传文件名"
    ),
) -> BatchDetail:
    # 1. 解析规则载荷
    try:
        rules_payload = json.loads(rules)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"rules 不是合法 JSON: {exc}")
    normalized_rules = validation.validate_rules_payload(
        rules_payload, config.MAX_RULES_PER_BATCH
    )

    # 2. 落盘到独立目录（先临时文件，校验通过再原子改名），不触碰原始上传位置
    batch_id = uuid.uuid4().hex
    target_dir = config.uploads_dir() / batch_id
    target_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(db_file.filename or "upload.db").suffix or ".db"
    final_path = target_dir / f"source{suffix}"
    try:
        tmp_fd, tmp_name = tempfile.mkstemp(
            dir=target_dir, prefix=".tmp-", suffix=suffix
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(tmp_fd, "wb") as tmp:
            size = 0
            while True:
                chunk = await db_file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > config.MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"上传文件超过上限 "
                            f"{config.MAX_UPLOAD_BYTES} 字节"
                        ),
                    )
                tmp.write(chunk)
        shutil.move(str(tmp_path), final_path)
    except HTTPException:
        shutil.rmtree(target_dir, ignore_errors=True)
        raise
    except Exception as exc:
        shutil.rmtree(target_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=f"保存上传文件失败: {exc}")

    # 3. 校验 SQLite 副本与规则 SQL（只读连接，绝不写上传库）
    try:
        validation.validate_sqlite_file(final_path, config.MAX_UPLOAD_BYTES)
        for rule in normalized_rules:
            validation.explain_rule_sql(final_path, rule["sql"])
    except validation.UploadError as exc:
        shutil.rmtree(target_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=str(exc))

    # 4. 持久化批次与规则骨架，随后投入后台执行
    batch_name = (name or db_file.filename or "unnamed-batch").strip()
    storage.insert_batch(
        batch_id=batch_id,
        name=batch_name,
        source_filename=db_file.filename or final_path.name,
        db_path=str(final_path),
        rules=normalized_rules,
    )
    runner.submit_batch(batch_id)

    batch_row = storage.get_batch_row(batch_id)
    rule_rows = storage.get_rule_rows(batch_id)
    return BatchDetail(**_summarize(batch_row, rule_rows, detail=True))


@app.get(
    "/batches",
    response_model=list[BatchSummary],
    tags=["核查批次"],
    summary="列出批次及汇总结论",
)
def list_batches(limit: int = 100) -> list[BatchSummary]:
    limit = max(1, min(limit, 500))
    result: list[BatchSummary] = []
    for row in storage.list_batch_rows(limit):
        rule_rows = storage.get_rule_rows(row["id"])
        result.append(BatchSummary(**_summarize(row, rule_rows, detail=False)))
    return result


@app.get(
    "/batches/{batch_id}",
    response_model=BatchDetail,
    tags=["核查批次"],
    summary="查询完整报告：各规则结论、违规总数、前 20 行违规明细",
)
def get_batch(batch_id: str) -> BatchDetail:
    batch_row = storage.get_batch_row(batch_id)
    if batch_row is None:
        raise HTTPException(status_code=404, detail=f"批次不存在: {batch_id}")
    rule_rows = storage.get_rule_rows(batch_id)
    return BatchDetail(**_summarize(batch_row, rule_rows, detail=True))


@app.post(
    "/batches/{batch_id}/cancel",
    response_model=CancelResponse,
    tags=["核查批次"],
    summary="取消整批（中断该批实际计算，迟到结果不得改写为完成）",
)
def cancel_batch(batch_id: str) -> CancelResponse:
    batch_row = storage.get_batch_row(batch_id)
    if batch_row is None:
        raise HTTPException(status_code=404, detail=f"批次不存在: {batch_id}")

    signal_sent = runner.request_cancel(batch_id)
    if not signal_sent:
        return CancelResponse(
            batch_id=batch_id,
            cancelled_signal_sent=False,
            status=batch_row["status"],
            message=(
                "批次不在运行中（可能已完成 / 已取消 / 重启后中断），"
                "无需取消；当前状态不允许自动放行"
            ),
        )
    return CancelResponse(
        batch_id=batch_id,
        cancelled_signal_sent=True,
        status=batch_row["status"],
        message=(
            "取消信号已送达，正在中断该批实际计算；未完成规则将标记 cancelled，"
            "批次不会给出可入仓结论"
        ),
    )


@app.get("/docs-demo", include_in_schema=False, response_class=HTMLResponse, tags=["运维"])
def docs_demo() -> HTMLResponse:
    return HTMLResponse(_DEMO_HTML)


_DEMO_HTML = """
<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8">
<title>核查服务演示指引</title>
<style>
body{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;max-width:900px;
margin:2rem auto;padding:0 1rem;line-height:1.6;color:#1f2937}
code,pre{background:#f3f4f6;border-radius:6px}
code{padding:.1rem .35rem} pre{padding:1rem;overflow-x:auto}
h1,h2{color:#111827} .ok{color:#047857;font-weight:600}
.bad{color:#b91c1c;font-weight:600}
</style></head>
<body>
<h1>临床试验数据核查服务 · 演示指引</h1>
<ol>
<li>打开 <a href="/docs">Swagger UI（/docs）</a>，所有接口可在线 Try it out。</li>
<li>用 <code>demo/make_sample_db.py</code> 生成含违规数据的 SQLite 副本，
用 <code>demo/rules.sample.json</code> 作为具名查询。</li>
<li>调用 <code>POST /batches</code>：<code>db_file</code> 选 db 文件，
<code>rules</code> 粘贴 JSON，执行后得到 <code>id</code>。</li>
<li>轮询 <code>GET /batches/{id}</code> 查看每条规则结论、违规总数与前 20 行。</li>
<li>对耗时批次调用 <code>POST /batches/{id}/cancel</code> 演示取消。</li>
</ol>
<h2>curl 示例</h2>
<pre>
curl -X POST http://localhost:8000/batches \\
  -F "db_file=@demo/sample_ctms.db" \\
  -F "rules=&lt;demo/rules.sample.json" \\
  -F "name=方案A-首次核查"

curl http://localhost:8000/batches
curl http://localhost:8000/batches/BATCH_ID
curl -X POST http://localhost:8000/batches/BATCH_ID/cancel
</pre>
<h2>放行规则</h2>
<p><span class="ok">can_load=true</span> 当且仅当批次
<code>completed</code> 且所有规则 <code>passed</code>（零行）。
超时 timeout、出错 error、取消 cancelled、中断 interrupted
<span class="bad">一律不可放行</span>。</p>
</body></html>
"""
