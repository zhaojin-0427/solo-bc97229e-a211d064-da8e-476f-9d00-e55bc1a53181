"""后台批次编排。

- 每个批次一个工作线程，批次间并行；批次内规则顺序执行；
- 每条规则由引擎放进独立子进程执行（见 engine.py）：超时 / 取消都会
  SIGKILL 子进程，真正停止计算并回收资源，同批后续规则随即继续；
- 取消注册表保存在内存：cancel_event 控制本进程内的实际计算中断；
- 终态（completed/cancelled/interrupted）由控制库持久化，重启后依然可查；
- 迟到结果保护：线程在写每条规则结论前复核批次当前状态，已取消/已终态
  的批次不再写入“完成”结论。
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from . import storage
from .config import QUERY_TIMEOUT_SECONDS, SAMPLE_ROW_LIMIT
from .engine import RuleOutcome, execute_rule


class _CancelRegistry:
    """batch_id -> 取消事件。仅记录本进程内仍可中断的批次。"""

    def __init__(self) -> None:
        self._events: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def register(self, batch_id: str) -> threading.Event:
        event = threading.Event()
        with self._lock:
            self._events[batch_id] = event
        return event

    def cancel(self, batch_id: str) -> bool:
        with self._lock:
            event = self._events.get(batch_id)
        if event is None:
            return False
        event.set()
        return True

    def is_cancelled(self, batch_id: str) -> bool:
        with self._lock:
            event = self._events.get(batch_id)
        return event.is_set() if event else False

    def unregister(self, batch_id: str) -> None:
        with self._lock:
            self._events.pop(batch_id, None)


registry = _CancelRegistry()

# 批次级并行线程池；max_workers 可按部署需要调整。
# 规则执行内部使用独立工作线程 + 看门狗，不占用该池的线程做阻塞等待之外的事。
_executor = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="batch-runner"
)


def submit_batch(batch_id: str) -> None:
    """登记取消事件并把批次投入后台线程池，立即返回，不阻塞 API 请求。"""
    event = registry.register(batch_id)
    _executor.submit(_run_batch, batch_id, event)


def request_cancel(batch_id: str) -> bool:
    """请求取消整批。

    返回 True 表示批次仍在本进程运行、取消信号已送达（实际计算将被中断）；
    False 表示批次已结束或不在本进程（重启后的批次无法远程中断）。
    """
    return registry.cancel(batch_id)


def _run_batch(batch_id: str, cancel_event: threading.Event) -> None:
    try:
        storage.set_batch_status(batch_id, storage.BATCH_RUNNING)
        batch_row = storage.get_batch_row(batch_id)
        db_path = batch_row["db_path"]
        rules = storage.get_rule_rows(batch_id)
        all_passed = True

        for row in rules:
            seq = row["seq"]

            # 取消点 1：开始下一条规则前
            if cancel_event.is_set() or registry.is_cancelled(batch_id):
                _persist_cancelled_remaining(batch_id, seq)
                _finish(batch_id, storage.BATCH_CANCELLED,
                        "用户取消整批，未完成规则的结果无效，不可放行")
                return

            outcome: RuleOutcome = execute_rule(
                db_path=db_path,
                sql_text=row["sql_text"],
                cancel_event=cancel_event,
                timeout_seconds=QUERY_TIMEOUT_SECONDS,
                sample_limit=SAMPLE_ROW_LIMIT,
            )

            # 取消点 2：规则返回后、落库前复核，防止迟到结果改写成完成
            current = storage.get_batch_row(batch_id)
            if current is None:
                return
            if current["status"] in (
                storage.BATCH_CANCELLED,
                storage.BATCH_INTERRUPTED,
                storage.BATCH_COMPLETED,
            ) or cancel_event.is_set():
                # 本规则及后续规则一律作废；主动落 cancelled，
                # 不依赖重启恢复来兜底（取消成功必须立刻体现终态）
                _persist_cancelled_remaining(batch_id, seq)
                if current["status"] != storage.BATCH_COMPLETED:
                    _finish(batch_id, storage.BATCH_CANCELLED,
                            "用户取消整批，迟到结果已丢弃，不可放行")
                return

            storage.update_rule_result(
                batch_id,
                seq,
                status=outcome.status,
                violation_count=outcome.violation_count,
                sample_rows=outcome.sample_rows,
                error_message=outcome.error_message,
            )

            if outcome.status != storage.RULE_PASSED:
                all_passed = False

        # 收尾窗口的取消：最后一条规则落库后、批次收尾前收到取消，
        # 同样不得给出 completed
        if cancel_event.is_set() or registry.is_cancelled(batch_id):
            _finish(batch_id, storage.BATCH_CANCELLED,
                    "收尾阶段收到取消信号，批次按取消处理，不可放行")
            return

        # 全部规则都执行完毕才可能 completed；且只有零违规才给可入仓
        _finish(
            batch_id,
            storage.BATCH_COMPLETED,
            error=None if all_passed else "存在未通过规则，不可入仓",
        )
    except Exception as exc:  # 编排层兜底：异常批次绝不放行
        storage.set_batch_status(
            batch_id,
            storage.BATCH_INTERRUPTED,
            error=f"执行过程中断: {type(exc).__name__}: {exc}",
        )
    finally:
        registry.unregister(batch_id)


def _persist_cancelled_remaining(batch_id: str, from_seq: int) -> None:
    """把从 from_seq 起仍处于排队态的规则统一标记为 cancelled。"""
    for row in storage.get_rule_rows(batch_id):
        if row["seq"] >= from_seq and row["status"] in ("queued", "running"):
            storage.update_rule_result(
                batch_id,
                row["seq"],
                status=storage.RULE_CANCELLED,
                error_message="批次被取消，该规则未执行或结果作废",
            )


def _finish(batch_id: str, status: str, error: str | None) -> None:
    storage.set_batch_status(batch_id, status, error=error)
