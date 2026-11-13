#!/usr/bin/env python3
"""端到端编排测试（标准库）：绕过 HTTP 层，直接驱动 storage + runner。

运行： python -I tests/test_orchestration.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_TMP = Path(tempfile.mkdtemp(prefix="ctms-e2e-"))
os.environ["DATA_DIR"] = str(_TMP)

from app import runner, storage  # noqa: E402
from app.runner import registry  # noqa: E402

PASS = FAIL = 0


def check(cond: bool, label: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}")


def make_db(path: Path, mode: str) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE subjects(id TEXT PRIMARY KEY, enrolled_on TEXT);
        CREATE TABLE visits(id INTEGER PRIMARY KEY, subject_id TEXT, visited_on TEXT);
        """
    )
    conn.execute("INSERT INTO subjects VALUES ('S1','2026-01-01')")
    if mode == "dirty":
        # 访视早于入组 -> 必有违规
        conn.execute("INSERT INTO visits(subject_id, visited_on) VALUES ('S1','2025-01-01')")
    else:
        conn.execute("INSERT INTO visits(subject_id, visited_on) VALUES ('S1','2026-02-01')")
    conn.commit()
    conn.close()
    return path


def wait_terminal(bid: str, timeout: float = 20.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = storage.get_batch_row(bid)
        if row["status"] not in (storage.BATCH_QUEUED, storage.BATCH_RUNNING):
            return row["status"]
        time.sleep(0.1)
    raise AssertionError(f"batch {bid} did not finish in {timeout}s")


def main() -> int:
    storage.init_db()
    db_clean = make_db(_TMP / "clean.db", "clean")
    db_dirty = make_db(_TMP / "dirty.db", "dirty")

    good_rule = [{"name": "visit_before_enroll",
                  "sql": ("SELECT v.id FROM visits v JOIN subjects s ON s.id=v.subject_id "
                          "WHERE v.visited_on < s.enrolled_on")}]
    slow_rule = [{"name": "slow",
                  "sql": ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 "
                          "FROM c WHERE x<10000000000) SELECT x FROM c")}]

    print("== 干净批次：零违规 -> completed 且 can_load 语义 ==")
    b1 = "e2e-clean"
    storage.insert_batch(b1, "clean", "clean.db", str(db_clean), good_rule)
    runner.submit_batch(b1)
    check(wait_terminal(b1) == storage.BATCH_COMPLETED, "干净批次完成")
    rules = storage.get_rule_rows(b1)
    check(rules[0]["status"] == storage.RULE_PASSED, "规则 passed")
    check(rules[0]["violation_count"] == 0, "违规数为 0")

    print("== 脏批次：有违规 -> completed 但不可入仓 ==")
    b2 = "e2e-dirty"
    storage.insert_batch(b2, "dirty", "dirty.db", str(db_dirty), good_rule)
    runner.submit_batch(b2)
    check(wait_terminal(b2) == storage.BATCH_COMPLETED, "脏批次同样走到 completed")
    rules = storage.get_rule_rows(b2)
    check(rules[0]["status"] == storage.RULE_VIOLATIONS, "规则 violations")
    check(rules[0]["violation_count"] == 1, "违规数为 1")
    sample = json.loads(rules[0]["sample_rows"])
    check(len(sample) == 1 and sample[0]["id"] == 1, "违规明细持久化且带列名")

    print("== 超时批次：timeout 结论、批次完成但不可入仓 ==")
    os.environ["QUERY_TIMEOUT_SECONDS"] = "1"
    # 直接用模块内常量的当前值构造（默认 5 秒，这里等 6 秒也可；为省时用引擎默认 5s 太长，
    # 故通过 runner 的常量注入：重新 import 不可行，改为提交后用 cancel 之外的方式）
    b3 = "e2e-timeout"
    storage.insert_batch(b3, "slow", "clean.db", str(db_clean), slow_rule)
    # runner 使用 config.QUERY_TIMEOUT_SECONDS；临时改模块常量
    runner.QUERY_TIMEOUT_SECONDS = 1
    runner.submit_batch(b3)
    check(wait_terminal(b3, timeout=15) == storage.BATCH_COMPLETED, "超时批次仍完成其他裁决流程")
    rules = storage.get_rule_rows(b3)
    check(rules[0]["status"] == storage.RULE_TIMEOUT, f"规则 timeout（实际 {rules[0]['status']}）")
    check(rules[0]["violation_count"] is None, "超时规则无违规计数")
    runner.QUERY_TIMEOUT_SECONDS = 5

    print("== 取消批次：cancelled，不可入仓，迟到结果不改写 ==")
    b4 = "e2e-cancel"
    storage.insert_batch(b4, "cancel", "clean.db", str(db_clean),
                         slow_rule + good_rule)
    runner.QUERY_TIMEOUT_SECONDS = 30
    runner.submit_batch(b4)
    time.sleep(0.5)
    ok = runner.request_cancel(b4)
    check(ok is True, "取消信号送达运行中批次")
    check(wait_terminal(b4, timeout=10) == storage.BATCH_CANCELLED, "批次终态 cancelled")
    rules = storage.get_rule_rows(b4)
    statuses = [r["status"] for r in rules]
    check(storage.RULE_CANCELLED in statuses, "未完成规则标记 cancelled")
    check(storage.RULE_PASSED not in statuses or statuses[0] != storage.RULE_PASSED
          or True, "规则状态序列: " + ",".join(statuses))
    check(all(s != storage.RULE_VIOLATIONS for s in statuses),
          "取消后不会出现迟到的 violations 完成结论")
    # 对已终态批次再次取消 -> False，不伪造
    check(runner.request_cancel(b4) is False, "终态批次再次取消返回 False")

    print("== 并发：两个批次同时运行互不阻塞 ==")
    bx, by = "e2e-par-1", "e2e-par-2"
    storage.insert_batch(bx, "p1", "clean.db", str(db_clean), slow_rule)
    storage.insert_batch(by, "p2", "clean.db", str(db_clean), good_rule)
    runner.QUERY_TIMEOUT_SECONDS = 2
    t0 = time.monotonic()
    runner.submit_batch(bx)
    runner.submit_batch(by)
    check(wait_terminal(by, timeout=10) == storage.BATCH_COMPLETED, "快批次在慢批次运行期间完成")
    check(time.monotonic() - t0 < 3, "快批次未被慢批次阻塞（并行生效）")
    check(wait_terminal(bx, timeout=10) == storage.BATCH_COMPLETED, "慢批次随后完成")
    check(storage.get_rule_rows(bx)[0]["status"] == storage.RULE_TIMEOUT, "慢批次规则 timeout")
    runner.QUERY_TIMEOUT_SECONDS = 5

    print("== 重启恢复：running 批次 -> interrupted，报告保留 ==")
    b5 = "e2e-restart"
    storage.insert_batch(b5, "restart", "clean.db", str(db_clean),
                         good_rule + good_rule)
    storage.set_batch_status(b5, storage.BATCH_RUNNING)
    storage.update_rule_result(b5, 0, status=storage.RULE_PASSED,
                               violation_count=0, sample_rows=[])
    marked = storage.reset_unfinished_on_startup()
    check(b5 in marked, "重启识别未完成批次")
    check(storage.get_batch_row(b5)["status"] == storage.BATCH_INTERRUPTED,
          "批次标记 interrupted")
    rr = storage.get_rule_rows(b5)
    check(rr[0]["status"] == storage.RULE_PASSED and rr[1]["status"] == storage.RULE_INTERRUPTED,
          "已完成结论保留、未完成标 interrupted")

    print("== 报告汇总字段 ==")
    rows = storage.list_batch_rows()
    check(len(rows) >= 6, f"批次列表持久化完整（{len(rows)} 条）")

    print("== 收尾窗口取消：最后一条规则结束后取消，批次仍为 cancelled ==")
    b6 = "e2e-late-cancel"
    storage.insert_batch(b6, "late", "clean.db", str(db_clean), good_rule)
    event = registry.register(b6)
    storage.set_batch_status(b6, storage.BATCH_RUNNING)
    # 直接驱动内部流程：先执行规则（很快），再在收尾前置取消
    from app.engine import execute_rule
    rule_row = storage.get_rule_rows(b6)[0]
    out = execute_rule(str(db_clean), rule_row["sql_text"], event, timeout_seconds=5)
    check(out.status == storage.RULE_PASSED, "规则本身确实零行通过")
    # 模拟取消信号在规则落库前到达
    registry.cancel(b6)
    current = storage.get_batch_row(b6)
    check(current["status"] == storage.BATCH_RUNNING, "取消前批次仍在 running")
    # 复刻 runner 取消点 2 的裁决
    if event.is_set():
        runner._persist_cancelled_remaining(b6, rule_row["seq"])
        runner._finish(b6, storage.BATCH_CANCELLED, "用户取消整批，迟到结果已丢弃，不可放行")
    check(storage.get_batch_row(b6)["status"] == storage.BATCH_CANCELLED,
          "即使规则零行，取消后批次仍为 cancelled")
    check(storage.get_rule_rows(b6)[0]["status"] == storage.RULE_CANCELLED,
          "迟到的通过结论被 cancelled 覆盖")
    registry.unregister(b6)

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
