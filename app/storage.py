"""控制库：批次与规则报告的持久化。

该库属于本服务自身（WAL 模式、可写），与上传的受试者数据库完全分离。
上传库始终以只读 URI 打开，本模块不会写它。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from . import config

# 批次终态 / 运行态
BATCH_QUEUED = "queued"
BATCH_RUNNING = "running"
BATCH_COMPLETED = "completed"
BATCH_CANCELLED = "cancelled"
BATCH_INTERRUPTED = "interrupted"

# 规则结论
RULE_PASSED = "passed"          # 零行
RULE_VIOLATIONS = "violations"  # 有违规行
RULE_TIMEOUT = "timeout"        # 超过时限，已实际中断，绝不可视为通过
RULE_ERROR = "error"            # SQL 执行出错（只读库上写操作会在这里报错）
RULE_CANCELLED = "cancelled"    # 批次被取消，未完成的规则
RULE_INTERRUPTED = "interrupted"  # 进程中断/重启，未完成的规则

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS batches (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    source_filename TEXT NOT NULL,
    db_path         TEXT NOT NULL,
    status          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    error           TEXT
);

CREATE TABLE IF NOT EXISTS rule_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL REFERENCES batches(id),
    rule_name       TEXT NOT NULL,
    sql_text        TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    status          TEXT NOT NULL,
    violation_count INTEGER,
    sample_rows     TEXT,
    error_message   TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    UNIQUE(batch_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_rule_results_batch ON rule_results(batch_id);
"""


def utcnow_iso() -> str:
    """统一的 UTC ISO-8601 时间戳。"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(
        config.control_db_path(),
        timeout=30.0,
        detect_types=sqlite3.PARSE_DECLTYPES,
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    Path(config.control_db_path()).parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA_SQL)


def reset_unfinished_on_startup() -> list[str]:
    """重启恢复：未到终态的批次标记为 interrupted。

    运行中（running/queued）的进程随容器退出已经消失，其实际计算随之终止；
    未完成规则一律标 interrupted，报告完整保留。interrupted 不可入仓。
    返回被标记的批次 id 列表。
    """
    unfinished_rule_statuses = (
        "queued",
        "running",
    )  # 规则初始态：尚未产生结论
    with connect() as conn:
        rows = conn.execute(
            "SELECT id FROM batches WHERE status IN (?, ?)",
            (BATCH_QUEUED, BATCH_RUNNING),
        ).fetchall()
        batch_ids = [r["id"] for r in rows]
        if not batch_ids:
            return []
        now = utcnow_iso()
        conn.executemany(
            """
            UPDATE batches
               SET status = ?, finished_at = ?,
                   error = COALESCE(error, '服务重启，批次未完成，标记为中断')
             WHERE id = ?
            """,
            [(BATCH_INTERRUPTED, now, bid) for bid in batch_ids],
        )
        conn.executemany(
            """
            UPDATE rule_results
               SET status = ?, finished_at = COALESCE(finished_at, ?),
                   error_message = COALESCE(
                       error_message, '服务重启时该规则未完成，结果无效，不可放行')
             WHERE batch_id = ? AND status IN (?, ?)
            """,
            [
                (RULE_INTERRUPTED, now, bid, *unfinished_rule_statuses)
                for bid in batch_ids
            ],
        )
    return batch_ids


# ---------------- 批次 ----------------

def insert_batch(
    batch_id: str,
    name: str,
    source_filename: str,
    db_path: str,
    rules: list[dict[str, Any]],
) -> None:
    now = utcnow_iso()
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO batches (id, name, source_filename, db_path,
                                 status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (batch_id, name, source_filename, db_path, BATCH_QUEUED, now),
        )
        conn.executemany(
            """
            INSERT INTO rule_results (batch_id, rule_name, sql_text, seq,
                                      status, started_at, finished_at)
            VALUES (?, ?, ?, ?, 'queued', NULL, NULL)
            """,
            [
                (batch_id, r["name"], r["sql"], i)
                for i, r in enumerate(rules)
            ],
        )


def set_batch_status(batch_id: str, status: str, error: Optional[str] = None) -> None:
    now = utcnow_iso()
    with connect() as conn:
        conn.execute(
            """
            UPDATE batches
               SET status = ?,
                   started_at = CASE WHEN ? = 'running'
                                     THEN COALESCE(started_at, ?)
                                     ELSE started_at END,
                   finished_at = CASE WHEN ? IN ('completed', 'cancelled',
                                                 'interrupted')
                                      THEN ? ELSE finished_at END,
                   error = COALESCE(?, error)
             WHERE id = ?
            """,
            (status, status, now, status, now, error, batch_id),
        )


def get_batch_row(batch_id: str) -> Optional[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()


def list_batch_rows(limit: int = 100) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM batches ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()


# ---------------- 规则结果 ----------------

def update_rule_result(
    batch_id: str,
    seq: int,
    *,
    status: str,
    violation_count: Optional[int] = None,
    sample_rows: Optional[list[dict[str, Any]]] = None,
    error_message: Optional[str] = None,
) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE rule_results
               SET status = ?, violation_count = ?, sample_rows = ?,
                   error_message = ?, started_at = COALESCE(started_at, ?),
                   finished_at = ?
             WHERE batch_id = ? AND seq = ?
            """,
            (
                status,
                violation_count,
                json.dumps(sample_rows, ensure_ascii=False, default=str)
                if sample_rows is not None
                else None,
                error_message,
                utcnow_iso(),
                utcnow_iso(),
                batch_id,
                seq,
            ),
        )


def get_rule_rows(batch_id: str) -> list[sqlite3.Row]:
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM rule_results WHERE batch_id = ? ORDER BY seq",
            (batch_id,),
        ).fetchall()
