#!/usr/bin/env python3
"""执行引擎与持久化语义测试（仅依赖 Python 标准库）。

运行：
    python -I tests/test_engine.py

覆盖：
1. 零行查询 -> passed；有行 -> violations，计数正确且只保留前 20 行；
2. 慢查询在时限内被 sqlite3 interrupt 真正中止 -> timeout，不算通过；
3. 取消事件触发 -> cancelled，实际计算停止；
4. 只读连接拒绝任何写操作（上传库不被修改）；
5. 重启恢复：queued/running 批次标记 interrupted，规则同样标记；
6. 取消后迟到结果不会把规则改写成完成；
7. 取消注册表的基本行为。
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# 用独立临时目录作为 DATA_DIR，必须在导入 app 包之前设置
_TMP = Path(tempfile.mkdtemp(prefix="ctms-test-"))
os.environ["DATA_DIR"] = str(_TMP)

from app import storage  # noqa: E402
from app.engine import (  # noqa: E402
    build_count_sql,
    execute_rule,
    open_readonly_connection,
)
from app.runner import registry, request_cancel  # noqa: E402

PASS = 0
FAIL = 0


def check(cond: bool, label: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}")


def make_fixture_db(dir_: Path) -> Path:
    p = dir_ / "fixture.db"
    conn = sqlite3.connect(p)
    conn.executescript(
        """
        CREATE TABLE subjects(id TEXT PRIMARY KEY, enrolled_on TEXT);
        CREATE TABLE visits(id INTEGER PRIMARY KEY, subject_id TEXT, visited_on TEXT);
        INSERT INTO subjects VALUES ('S1','2026-01-01'),('S2',NULL);
        INSERT INTO visits (subject_id, visited_on) VALUES
            ('S1','2026-02-01'),
            ('S1','2026-02-01'),
            ('S2','2025-12-01');
        """
    )
    # 造 25 行违规数据用于前 20 行截断验证
    for i in range(25):
        conn.execute(
            "INSERT INTO visits(subject_id, visited_on) VALUES (?,?)",
            ("S2", f"2025-11-{1 + (i % 28):02d}"),
        )
    conn.commit()
    conn.close()
    return p


def test_pass_and_violations(db: Path) -> None:
    print("== 规则结论：零行通过 / 违规计数 / 前20行截断 ==")
    ok_sql = "SELECT id FROM subjects WHERE 1 = 0"
    out = execute_rule(str(db), ok_sql, threading.Event(), timeout_seconds=5)
    check(out.status == storage.RULE_PASSED, "零行查询判定 passed")
    check(out.violation_count == 0 and out.sample_rows == [], "零行时计数 0 且明细为空")

    bad_sql = "SELECT id, subject_id FROM visits WHERE subject_id = 'S2'"
    out = execute_rule(str(db), bad_sql, threading.Event(), timeout_seconds=5)
    check(out.status == storage.RULE_VIOLATIONS, "有违规行判定 violations")
    check(out.violation_count == 26, f"违规总数=26（实际 {out.violation_count}）")
    check(len(out.sample_rows) == 20, f"明细仅保留前 20 行（实际 {len(out.sample_rows or [])}）")
    check(set(out.sample_rows[0].keys()) == {"id", "subject_id"}, "明细保留列名")

    wrapped = build_count_sql("SELECT 1;")
    check(
        wrapped == "SELECT COUNT(*) FROM (SELECT 1)",
        f"计数 SQL 包装正确: {wrapped}",
    )


def test_timeout_interrupts_computation(db: Path) -> None:
    print("== 超时：到点 interrupt 真正停止计算，且不算通过 ==")
    slow_sql = (
        "WITH RECURSIVE cnt(x) AS ("
        "SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10000000000"
        ") SELECT x FROM cnt"
    )
    t0 = time.monotonic()
    out = execute_rule(str(db), slow_sql, threading.Event(), timeout_seconds=1.0)
    elapsed = time.monotonic() - t0
    check(out.status == storage.RULE_TIMEOUT, f"慢查询判定 timeout（实际 {out.status}）")
    check(out.timed_out and out.violation_count is None, "超时无违规计数，不可放行")
    check(elapsed < 3.0, f"约 1 秒预算内中止（实际 {elapsed:.2f}s），未拖到查询自然结束")
    check("interrupt" in (out.error_message or "").lower()
          or "时限" in (out.error_message or ""), "错误信息说明已中断")


def test_cancel_stops_computation(db: Path) -> None:
    print("== 取消：事件触发后实际计算中止，结论 cancelled ==")
    slow_sql = (
        "WITH RECURSIVE cnt(x) AS ("
        "SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10000000000"
        ") SELECT x FROM cnt"
    )
    event = threading.Event()
    holder: dict = {}

    def run() -> None:
        holder["out"] = execute_rule(
            str(db), slow_sql, event, timeout_seconds=30.0
        )

    th = threading.Thread(target=run)
    t0 = time.monotonic()
    th.start()
    time.sleep(0.4)
    event.set()
    th.join(timeout=5.0)
    elapsed = time.monotonic() - t0
    check(not th.is_alive(), "取消后工作线程真正退出（计算已停止）")
    out = holder.get("out")
    check(out is not None and out.status == storage.RULE_CANCELLED,
          f"取消判定 cancelled（实际 {None if out is None else out.status}）")
    check(elapsed < 3.0, f"取消后快速中止（{elapsed:.2f}s），未等 30 秒预算")


def test_readonly_never_writes(db: Path) -> None:
    print("== 只读：写操作被拒绝，上传库字节不变 ==")
    before = db.read_bytes()
    conn = open_readonly_connection(db)
    write_sqls = [
        "INSERT INTO subjects(id) VALUES ('X')",
        "UPDATE subjects SET enrolled_on='2026-01-01' WHERE id='S1'",
        "DELETE FROM visits",
        "CREATE TABLE evil(id INTEGER)",
    ]
    rejected = 0
    for sql in write_sqls:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            rejected += 1
    conn.close()
    check(rejected == len(write_sqls), f"全部写语句被只读连接拒绝（{rejected}/{len(write_sqls)}）")
    check(db.read_bytes() == before, "上传库文件内容未被修改")

    # 错误规则 -> error 而非通过
    out = execute_rule(str(db), "SELECT no_such_column FROM nope",
                       threading.Event(), timeout_seconds=5)
    check(out.status == storage.RULE_ERROR, f"坏 SQL 判定 error（实际 {out.status}）")


def test_persistence_and_restart() -> None:
    print("== 持久化与重启恢复 ==")
    storage.init_db()
    bid = "batch-restart-1"
    storage.insert_batch(
        bid, "重启测试", "fixture.db", "/tmp/fixture.db",
        [{"name": "r1", "sql": "SELECT 1"}, {"name": "r2", "sql": "SELECT 2"}],
    )
    storage.set_batch_status(bid, storage.BATCH_RUNNING)
    storage.update_rule_result(bid, 0, status=storage.RULE_PASSED,
                               violation_count=0, sample_rows=[])
    # r2 仍为 queued —— 模拟进程在中途退出

    marked = storage.reset_unfinished_on_startup()
    check(bid in marked, "重启时未完成批次被识别")
    row = storage.get_batch_row(bid)
    check(row["status"] == storage.BATCH_INTERRUPTED,
          f"未完成批次标记 interrupted（实际 {row['status']}）")
    rules = storage.get_rule_rows(bid)
    check(rules[0]["status"] == storage.RULE_PASSED, "已完成规则结论保留")
    check(rules[1]["status"] == storage.RULE_INTERRUPTED,
          f"未完成规则标记 interrupted（实际 {rules[1]['status']}）")
    # 再次重启不应重复处理已终态批次
    check(storage.reset_unfinished_on_startup() == [], "终态批次重启幂等")


def test_cancel_registry() -> None:
    print("== 取消注册表 ==")
    bid = "batch-reg-1"
    event = registry.register(bid)
    check(not event.is_set(), "注册后初始未取消")
    check(request_cancel(bid) is True, "运行中批次可发送取消信号")
    check(event.is_set() and registry.is_cancelled(bid), "事件已置位")
    registry.unregister(bid)
    check(request_cancel(bid) is False, "已注销批次取消返回 False（不伪造成功）")


def test_late_result_guard(db: Path) -> None:
    print("== 迟到结果保护：取消后结论不可改写为完成 ==")
    storage.init_db()
    bid = "batch-late-1"
    storage.insert_batch(
        bid, "迟到结果", "fixture.db", str(db),
        [{"name": "slow", "sql": "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000000) SELECT x FROM c"}],
    )
    storage.set_batch_status(bid, storage.BATCH_RUNNING)
    event = registry.register(bid)

    from app.engine import execute_rule as run_one

    holder: dict = {}

    def worker() -> None:
        holder["out"] = run_one(str(db), storage.get_rule_rows(bid)[0]["sql_text"],
                                event, timeout_seconds=30.0)

    th = threading.Thread(target=worker)
    th.start()
    time.sleep(0.3)
    # 模拟用户取消：先置事件，再把批次终态写为 cancelled
    request_cancel(bid)
    storage.set_batch_status(bid, storage.BATCH_CANCELLED,
                             error="用户取消整批")
    th.join(timeout=5.0)
    check(not th.is_alive(), "取消后计算线程退出")
    check(holder["out"].status == storage.RULE_CANCELLED,
          f"迟到结果为 cancelled 而非 passed/violations（实际 {holder['out'].status}）")
    final = storage.get_batch_row(bid)
    check(final["status"] == storage.BATCH_CANCELLED, "批次终态保持 cancelled")
    registry.unregister(bid)


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="ctms-cases-", dir=str(_TMP)))
    db = make_fixture_db(work)
    try:
        test_pass_and_violations(db)
        test_timeout_interrupts_computation(db)
        test_cancel_stops_computation(db)
        test_readonly_never_writes(db)
        test_persistence_and_restart()
        test_cancel_registry()
        test_late_result_guard(db)
    except Exception:
        traceback.print_exc()
        return 2
    print(f"\n结果: {PASS} 通过, {FAIL} 失败；临时目录 {_TMP}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
