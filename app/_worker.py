"""规则工作进程：在独立子进程中执行单条核查 SQL（计数 + 前 N 行取样）。

为什么存在：SQLite 内置函数（如长文本 GLOB/LIKE 匹配）执行期间，
``sqlite3_interrupt()`` 与 progress handler 都要等当前虚拟机指令返回后才
生效，单条 GLOB 调用可能卡住几十秒且无法中止。把规则放进独立子进程后，
父进程可在时限 / 取消时直接 SIGKILL，真正停止计算并由 OS 回收资源。

协议（由 :func:`app.engine.execute_rule` 启动）：

- 启动：``python -I -m app._worker <job.json>``
- 任务文件（JSON）：``{"db_path", "count_sql", "sample_sql",
  "self_destruct_seconds"}``
- stdout 回一行 JSON：
  成功 ``{"ok": true, "count": N, "sample": [...]}``；
  SQL 错误等 ``{"ok": false, "error_type": ..., "error": ...}``
  （SQL 错误属于正常业务结论，退出码仍为 0）。
- 退出码非 0 仅表示工作进程自身故障（任务文件不可读等）。

进程内同时装一个"自毁"看门狗：若父进程异常消失、无人来杀，
超过 self_destruct_seconds 后 progress handler 会中止语句并退出，
避免孤儿进程无限烧 CPU（对 GLOB 这类单指令卡点，最终兜底是
父进程的 SIGKILL 与 PR_SET_PDEATHSIG）。
"""
from __future__ import annotations

import json
import sys
import time


def _emit(payload: dict) -> None:
    json.dump(payload, sys.stdout, ensure_ascii=False, default=str)
    sys.stdout.write("\n")
    sys.stdout.flush()


def _run_job(job: dict) -> dict:
    # 延迟到子进程内导入，保持启动路径最小化
    from app.engine import open_readonly_connection, rows_to_dicts

    self_destruct = float(job.get("self_destruct_seconds") or 0)
    started = time.monotonic()

    conn = open_readonly_connection(job["db_path"])
    try:
        if self_destruct > 0:
            # 指令流类查询（如递归 CTE）的兜底中止；单指令卡点由父进程 SIGKILL
            def _self_destruct_handler() -> int:
                return 1 if time.monotonic() - started > self_destruct else 0

            conn.set_progress_handler(_self_destruct_handler, 1000)

        row = conn.execute(job["count_sql"]).fetchone()
        count = int(row[0]) if row else 0
        sample: list[dict] = []
        if count > 0:
            cursor = conn.execute(job["sample_sql"])
            sample = rows_to_dicts(cursor, cursor.fetchall())
        return {"ok": True, "count": count, "sample": sample}
    finally:
        conn.close()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        sys.stderr.write("usage: python -I -m app._worker <job.json>\n")
        return 2
    try:
        with open(argv[1], "r", encoding="utf-8") as fh:
            job = json.load(fh)
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"worker: 任务文件不可读: {exc}\n")
        return 2

    try:
        payload = _run_job(job)
    except BaseException as exc:  # SQL 错误 / 只读拒绝 / 自毁中断等，正常回传
        payload = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    _emit(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
