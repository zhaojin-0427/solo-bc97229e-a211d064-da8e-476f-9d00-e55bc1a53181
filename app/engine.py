"""规则执行引擎。

关键语义：
1. 上传库始终以 ``mode=ro`` 只读 URI 打开，核查 SQL 无法修改上传库；
2. 每条规则共享一个 5 秒预算；看门狗线程到点调用 ``conn.interrupt()``
   让 SQLite 虚拟机真正中止计算（而不是仅在客户端放弃结果）；
3. 取消通过 threading.Event 触发同一套中断机制，并由 progress handler
   在虚拟机内部协作式快速退出；
4. 超时 / 取消 / 出错都不产生“通过”结论。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from . import storage
from .config import QUERY_TIMEOUT_SECONDS, SAMPLE_ROW_LIMIT


@dataclass
class RuleOutcome:
    """单条规则的执行结论（内存态，随后持久化）。"""

    status: str
    violation_count: Optional[int] = None
    sample_rows: Optional[list[dict[str, Any]]] = None
    error_message: Optional[str] = None
    elapsed_ms: int = 0
    timed_out: bool = False
    cancelled: bool = False


@dataclass
class _WatchResult:
    reason: Optional[str] = None  # None=正常结束；"timeout" / "cancelled"


@dataclass
class _WorkerBundle:
    """工作线程与看门狗之间的共享状态。"""

    cancel_event: threading.Event
    conn: sqlite3.Connection
    deadline: float
    result: _WatchResult = field(default_factory=_WatchResult)
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    finished: bool = False
    # 工作线程正常完成时写入 (违规总数, 前 N 行明细)
    normal_payload: Optional[tuple[int, list[dict[str, Any]]]] = None


def open_readonly_connection(db_path: str | Path) -> sqlite3.Connection:
    """以只读方式打开 SQLite 副本。

    - ``mode=ro``：任何写操作（含 ATTACH 写、隐式事务写）都会被 SQLite 拒绝；
    - ``immutable=1``：告知 SQLite 文件不会被外部修改，可安全只读并减少锁开销；
    - URI 中的路径做百分号编码，避免空格 / 中文路径问题。
    """
    absolute = str(Path(db_path).resolve())
    uri = f"file:{quote(absolute)}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0, check_same_thread=False)
    # 只读连接上再设防御性 PRAGMA；query_only 禁止一切写语句
    conn.execute("PRAGMA query_only = ON")
    return conn


def strip_trailing_semicolon(sql: str) -> str:
    return sql.strip().rstrip(";").strip()


def build_count_sql(sql: str) -> str:
    """把具名查询包装成计数查询：SELECT COUNT(*) FROM (<原查询>)。

    原查询在子查询中执行，外层只做计数，不会改变原查询的过滤逻辑。
    """
    return f"SELECT COUNT(*) FROM ({strip_trailing_semicolon(sql)})"


def build_sample_sql(sql: str, limit: int) -> str:
    """取违规明细前 N 行（外层 LIMIT，不改写原查询语义）。"""
    return f"SELECT * FROM ({strip_trailing_semicolon(sql)}) LIMIT {int(limit)}"


def _rows_to_dicts(cursor: sqlite3.Cursor, rows: list[tuple]) -> list[dict[str, Any]]:
    columns = [d[0] for d in cursor.description] if cursor.description else []
    result: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for col, value in zip(columns, row):
            item[col] = _json_safe(value)
        result.append(item)
    return result


def _json_safe(value: Any) -> Any:
    """让 SQLite 取值可 JSON 序列化；BLOB 转 base64 字符串。"""
    import base64

    if isinstance(value, bytes):
        return {"__blob_base64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _make_progress_handler(bundle: _WorkerBundle):
    """SQLite 虚拟机进度回调：每 ~1000 个字节码指令调用一次。

    返回非零会让 SQLite 中止当前语句并抛 OperationalError(interrupted)。
    这让取消在长查询内部也能快速生效，不必等到看门狗超时。
    """

    def handler() -> int:
        if bundle.cancel_event.is_set():
            with bundle.state_lock:
                if not bundle.finished:
                    bundle.result.reason = "cancelled"
            return 1
        remaining = bundle.deadline - time.monotonic()
        if remaining <= 0:
            with bundle.state_lock:
                if not bundle.finished:
                    bundle.result.reason = "timeout"
            return 1
        return 0

    return handler


def _watchdog(bundle: _WorkerBundle, worker: threading.Thread) -> None:
    """看门狗：到 deadline 或收到取消时，对连接执行 interrupt。

    interrupt 是 SQLite 官方提供的异步取消入口，可从其它线程安全调用，
    会让正在执行的语句尽快终止——这是“停止实际计算”的关键。
    """
    while True:
        if worker.is_alive() is False:
            return
        remaining = bundle.deadline - time.monotonic()
        if remaining <= 0:
            with bundle.state_lock:
                if not bundle.finished:
                    bundle.result.reason = "timeout"
            _safe_interrupt(bundle)
            return
        # 最多每 50ms 醒一次，兼顾取消响应速度与 CPU 开销
        if bundle.cancel_event.wait(timeout=min(0.05, max(remaining, 0.0))):
            with bundle.state_lock:
                if not bundle.finished:
                    bundle.result.reason = "cancelled"
            _safe_interrupt(bundle)
            return


def _safe_interrupt(bundle: _WorkerBundle) -> None:
    try:
        bundle.conn.interrupt()
    except sqlite3.Error:
        # 连接已关闭等竞态：工作线程马上也会结束，忽略即可
        pass


def _classify_interrupt_error(bundle: _WorkerBundle) -> str:
    """根据看门狗 / 回调记录的原因判定中断类型。"""
    with bundle.state_lock:
        reason = bundle.result.reason
    if reason == "cancelled" or bundle.cancel_event.is_set():
        return storage.RULE_CANCELLED
    return storage.RULE_TIMEOUT  # reason == "timeout" 或竞态下的保守判定


def execute_rule(
    db_path: str,
    sql_text: str,
    cancel_event: threading.Event,
    timeout_seconds: float = QUERY_TIMEOUT_SECONDS,
    sample_limit: int = SAMPLE_ROW_LIMIT,
) -> RuleOutcome:
    """执行一条核查规则，返回结构化结论。

    预算说明：计数查询与取前 N 行查询共享同一个 deadline（共 timeout_seconds
    秒），保证“每条查询限 5 秒、超时停止实际计算”。
    """
    started = time.monotonic()
    deadline = started + timeout_seconds
    conn: Optional[sqlite3.Connection] = None
    try:
        try:
            conn = open_readonly_connection(db_path)
        except sqlite3.Error as exc:
            return RuleOutcome(
                status=storage.RULE_ERROR,
                error_message=f"无法只读打开上传库: {type(exc).__name__}: {exc}",
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        bundle = _WorkerBundle(
            cancel_event=cancel_event,
            conn=conn,
            deadline=deadline,
        )
        conn.set_progress_handler(_make_progress_handler(bundle), 1000)

        worker_error: list[BaseException] = []

        def worker() -> None:
            try:
                # 第一步：计数（违规总数）
                cursor = conn.execute(build_count_sql(sql_text))
                count_row = cursor.fetchone()
                count_value = int(count_row[0]) if count_row else 0

                sample: list[dict[str, Any]] = []
                if count_value > 0:
                    # 第二步：仅在确有违规时取前 N 行明细，共享同一 deadline
                    sample_cursor = conn.execute(
                        build_sample_sql(sql_text, sample_limit)
                    )
                    sample = _rows_to_dicts(
                        sample_cursor, sample_cursor.fetchall()
                    )

                # 正常完成；若看门狗已记录中断原因，则不能当作正常结果
                with bundle.state_lock:
                    if bundle.result.reason is None:
                        bundle.finished = True
                        bundle.normal_payload = (count_value, sample)
            except BaseException as exc:  # 含 OperationalError(interrupted)
                worker_error.append(exc)

        thread = threading.Thread(
            target=worker, name="rule-worker", daemon=True
        )
        thread.start()
        watcher = threading.Thread(
            target=_watchdog,
            args=(bundle, thread),
            name="rule-watchdog",
            daemon=True,
        )
        watcher.start()
        thread.join()
        watcher.join(timeout=1.0)

        elapsed_ms = int((time.monotonic() - started) * 1000)

        if worker_error:
            exc = worker_error[0]
            if isinstance(exc, sqlite3.OperationalError) and (
                "interrupt" in str(exc).lower()
            ):
                status = _classify_interrupt_error(bundle)
                if status == storage.RULE_CANCELLED:
                    return RuleOutcome(
                        status=status,
                        error_message="批次被取消，规则计算已中止",
                        elapsed_ms=elapsed_ms,
                        cancelled=True,
                    )
                return RuleOutcome(
                    status=status,
                    error_message=(
                        f"规则超过 {timeout_seconds:.0f} 秒时限，"
                        "已调用 sqlite3 interrupt 中止实际计算；超时不算通过"
                    ),
                    elapsed_ms=elapsed_ms,
                    timed_out=True,
                )
            # 只读库上的写尝试、SQL 语法/ schema 错误等
            return RuleOutcome(
                status=storage.RULE_ERROR,
                error_message=f"{type(exc).__name__}: {exc}",
                elapsed_ms=elapsed_ms,
            )

        # 无异常：确认看门狗没有在临界时刻判定超时 / 取消
        with bundle.state_lock:
            reason = bundle.result.reason
            normal = bundle.normal_payload
        if reason is not None or normal is None:
            status = (
                storage.RULE_CANCELLED
                if (reason == "cancelled" or cancel_event.is_set())
                else storage.RULE_TIMEOUT
            )
            if status == storage.RULE_CANCELLED:
                return RuleOutcome(
                    status=status,
                    error_message="批次被取消，规则结果作废",
                    elapsed_ms=elapsed_ms,
                    cancelled=True,
                )
            return RuleOutcome(
                status=status,
                error_message=(
                    f"规则达到 {timeout_seconds:.0f} 秒时限被中断；超时不算通过"
                ),
                elapsed_ms=elapsed_ms,
                timed_out=True,
            )

        count_value, sample = normal
        if count_value == 0:
            return RuleOutcome(
                status=storage.RULE_PASSED,
                violation_count=0,
                sample_rows=[],
                elapsed_ms=elapsed_ms,
            )
        return RuleOutcome(
            status=storage.RULE_VIOLATIONS,
            violation_count=count_value,
            sample_rows=sample,
            elapsed_ms=elapsed_ms,
        )
    finally:
        if conn is not None:
            try:
                conn.set_progress_handler(None, 0)
                conn.close()
            except sqlite3.Error:
                pass
