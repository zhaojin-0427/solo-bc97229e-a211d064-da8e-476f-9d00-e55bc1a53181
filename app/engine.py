"""规则执行引擎。

关键语义：
1. 上传库始终以 ``mode=ro`` 只读 URI 打开，核查 SQL 无法修改上传库；
2. 每条规则在**独立子进程**中执行（计数 + 前 N 行取样共享同一 deadline）。
   时限到期或收到取消时，父进程直接向子进程发 SIGKILL——
   SQLite 内置函数（如长文本 GLOB/LIKE 匹配）执行期间，
   ``sqlite3_interrupt()`` 与 progress handler 都要等当前虚拟机指令返回
   才生效，单条 GLOB 调用可能卡住几十秒且无法中止；只有进程级终止
   才能立刻停止实际计算，并由操作系统回收 CPU / 内存；
3. 超时记 ``timeout``、取消记 ``cancelled``，两者都不产生"通过"结论
   （fail-closed），且父进程回收子进程后同批后续规则立即继续。
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from . import storage
from .config import QUERY_TIMEOUT_SECONDS, SAMPLE_ROW_LIMIT

# 看门狗轮询间隔：取消 / 超时的响应粒度（秒）
_POLL_INTERVAL_SECONDS = 0.05
# SIGKILL 后等待子进程回收的上限（秒）；SIGKILL 正常即刻生效，这只是兜底
_KILL_GRACE_SECONDS = 5.0


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


def rows_to_dicts(cursor: sqlite3.Cursor, rows: list[tuple]) -> list[dict[str, Any]]:
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


# ---------------- 子进程工作器 ----------------
#
# 为什么必须用子进程而不是线程 + conn.interrupt()：
# SQLite 的中断标志与 progress handler 都只在**虚拟机指令之间**检查。
# 递归 CTE 这类"指令流"查询可以被 interrupt 中止；但长文本 GLOB/LIKE
# 匹配是**单条内置函数调用**（实测 50 万字符文本 × 2 万字符模式要约 22 秒），
# 期间 interrupt() 和 progress handler 完全失效，线程又无法被外部杀死，
# 结果就是"取消也无法停止、同批后续规则一直等待"。
# 把规则放进独立子进程后，父进程可用 SIGKILL 立即终止计算并由 OS 回收资源。


def _package_root() -> Path:
    """app 包的父目录，用于子进程 PYTHONPATH。"""
    return Path(__file__).resolve().parent.parent


def _worker_env() -> dict[str, str]:
    """子进程环境：注入包路径，保证 ``python -m app._worker`` 可导入。

    不能用 ``-I``（隔离模式隐含 -E，会忽略 PYTHONPATH），因此改用
    ``-P -s`` 并显式重建 PYTHONPATH；同时保留 DATA_DIR 等运行配置
    （子进程只用到只读打开逻辑）。
    """
    env = dict(os.environ)
    root = str(_package_root())
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = root if not existing else root + os.pathsep + existing
    return env


def _worker_command(job_path: str) -> list[str]:
    return [
        sys.executable,
        "-P",  # 不把 cwd / 脚本目录放进 sys.path，避免上传目录同名模块被导入
        "-s",  # 不加载用户 site-packages
        "-m",
        "app._worker",
        job_path,
    ]


def _write_job_file(job: dict[str, Any]) -> str:
    """把任务参数写入临时 JSON 文件（0600），路径经 argv 传给子进程。

    不用 stdin 传参：SQL 文本可能超过管道缓冲（64KB），
    communicate 超时重试不会续写未发完的 input，会造成假超时。
    """
    fd, path = tempfile.mkstemp(prefix="ctms-rule-job-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(job, fh, ensure_ascii=False)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return path


def _remove_quietly(path: Optional[str]) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        pass


# Linux 上让子进程在父进程死亡时收到 SIGKILL（PR_SET_PDEATHSIG），
# 防止服务重启后遗留孤儿进程继续烧 CPU；libc 句柄在父进程提前打开，
# 避免 fork 后的子进程里再走 dlopen。
_PDEATHSIG_LIBC: Any = None
if sys.platform.startswith("linux"):
    try:
        import ctypes

        _PDEATHSIG_LIBC = ctypes.CDLL("libc.so.6", use_errno=True)
    except Exception:  # pragma: no cover - 极端环境无 libc 时降级
        _PDEATHSIG_LIBC = None


def _child_set_pdeathsig() -> None:
    """preexec_fn：子进程 exec 前设置父亡信号；失败不影响主流程。"""
    if _PDEATHSIG_LIBC is None:
        return
    try:
        _PDEATHSIG_LIBC.prctl(1, 9, 0, 0, 0)  # PR_SET_PDEATHSIG, SIGKILL
    except Exception:
        pass


def _spawn_worker(job_path: str) -> subprocess.Popen:
    kwargs: dict[str, Any] = dict(
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,  # 独立会话，信号语义干净
        env=_worker_env(),
    )
    if _PDEATHSIG_LIBC is not None:
        kwargs["preexec_fn"] = _child_set_pdeathsig
    return subprocess.Popen(_worker_command(job_path), **kwargs)


def _kill_and_reap(proc: subprocess.Popen) -> None:
    """SIGKILL 子进程并回收：等退出、读干管道，确保计算停止、资源回收。"""
    try:
        proc.kill()
    except OSError:
        pass  # 进程已退出（竞态），直接走回收
    try:
        # communicate 在 TimeoutExpired 后重试是文档允许的安全操作，
        # 这里进程尚未完成过一次成功的 communicate，直接调用即可。
        proc.communicate(timeout=_KILL_GRACE_SECONDS)
    except Exception:
        try:
            proc.wait(timeout=1.0)
        except Exception:
            pass


def execute_rule(
    db_path: str,
    sql_text: str,
    cancel_event: threading.Event,
    timeout_seconds: float = QUERY_TIMEOUT_SECONDS,
    sample_limit: int = SAMPLE_ROW_LIMIT,
) -> RuleOutcome:
    """执行一条核查规则，返回结构化结论。

    预算说明：子进程启动、计数查询与取前 N 行查询共享同一个 deadline
    （共 timeout_seconds 秒）。到点或收到取消即 SIGKILL 子进程，
    停止实际计算并回收资源，不等待 SQLite 内置函数自然结束。
    """
    started = time.monotonic()
    deadline = started + timeout_seconds
    job = {
        "db_path": str(Path(db_path).resolve()),
        "count_sql": build_count_sql(sql_text),
        "sample_sql": build_sample_sql(sql_text, sample_limit),
        # 兜底自毁时限：父进程异常消失时，子进程也不无限运行
        "self_destruct_seconds": timeout_seconds + 2.0,
    }
    try:
        job_path = _write_job_file(job)
    except OSError as exc:
        return RuleOutcome(
            status=storage.RULE_ERROR,
            error_message=f"无法创建规则任务文件: {exc}",
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )

    proc: Optional[subprocess.Popen] = None
    try:
        try:
            proc = _spawn_worker(job_path)
        except OSError as exc:
            return RuleOutcome(
                status=storage.RULE_ERROR,
                error_message=f"无法启动规则工作进程: {exc}",
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )

        reason: Optional[str] = None  # "timeout" / "cancelled"
        out_bytes: bytes = b""
        err_bytes: bytes = b""
        while True:
            try:
                out_bytes, err_bytes = proc.communicate(
                    timeout=_POLL_INTERVAL_SECONDS
                )
                break  # 子进程已退出且输出收齐
            except subprocess.TimeoutExpired:
                if cancel_event.is_set():
                    reason = "cancelled"
                    break
                if time.monotonic() >= deadline:
                    reason = "timeout"
                    break

        if reason is not None:
            # 到点 / 取消：立即 SIGKILL，回收进程，不等待 GLOB 等内置函数自然结束
            _kill_and_reap(proc)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            if reason == "cancelled":
                return RuleOutcome(
                    status=storage.RULE_CANCELLED,
                    error_message=(
                        "批次被取消，规则工作进程已终止（SIGKILL），"
                        "实际计算停止且资源已回收"
                    ),
                    elapsed_ms=elapsed_ms,
                    cancelled=True,
                )
            return RuleOutcome(
                status=storage.RULE_TIMEOUT,
                error_message=(
                    f"规则超过 {timeout_seconds:.0f} 秒时限，工作进程已被强制终止"
                    "（SIGKILL），实际计算停止且资源已回收；超时不算通过"
                ),
                elapsed_ms=elapsed_ms,
                timed_out=True,
            )

        elapsed_ms = int((time.monotonic() - started) * 1000)

        # 临界复核（fail-closed）：子进程虽正常退出，但输出到手时
        # 取消已受理或时限已过，结果一律作废，绝不放行
        if cancel_event.is_set():
            return RuleOutcome(
                status=storage.RULE_CANCELLED,
                error_message="批次被取消，迟到的规则结果作废",
                elapsed_ms=elapsed_ms,
                cancelled=True,
            )
        if time.monotonic() > deadline:
            return RuleOutcome(
                status=storage.RULE_TIMEOUT,
                error_message=(
                    f"规则结果在 {timeout_seconds:.0f} 秒时限后才产出，"
                    "按超时处理；超时不算通过"
                ),
                elapsed_ms=elapsed_ms,
                timed_out=True,
            )

        if proc.returncode != 0:
            detail = err_bytes.decode("utf-8", "replace").strip()
            return RuleOutcome(
                status=storage.RULE_ERROR,
                error_message=(
                    f"规则工作进程异常退出(code={proc.returncode}): "
                    f"{detail[:500] or '无错误输出'}"
                ),
                elapsed_ms=elapsed_ms,
            )

        try:
            payload = json.loads(out_bytes.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            return RuleOutcome(
                status=storage.RULE_ERROR,
                error_message=f"规则工作进程输出无法解析: {exc}",
                elapsed_ms=elapsed_ms,
            )
        if not isinstance(payload, dict) or "ok" not in payload:
            return RuleOutcome(
                status=storage.RULE_ERROR,
                error_message="规则工作进程输出格式非法",
                elapsed_ms=elapsed_ms,
            )
        if not payload["ok"]:
            return RuleOutcome(
                status=storage.RULE_ERROR,
                error_message=(
                    f"{payload.get('error_type', 'Error')}: "
                    f"{payload.get('error', '')}"
                ),
                elapsed_ms=elapsed_ms,
            )

        count_value = int(payload.get("count") or 0)
        sample = payload.get("sample") or []
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
        # 兜底：任何路径返回前，绝不留下仍在运行的子进程
        if proc is not None and proc.poll() is None:
            _kill_and_reap(proc)
        _remove_quietly(job_path)
