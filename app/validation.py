"""上传内容校验：SQLite 文件完整性、必需表、具名只读查询合法性。"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from .engine import open_readonly_connection

SQLITE_MAGIC = b"SQLite format 3\x00"

REQUIRED_TABLES = {
    "subjects": {"id", "enrolled_on"},
    "visits": {"id", "subject_id", "visited_on"},
}

# 允许的语句起始关键字（核查规则只允许查询）
_ALLOWED_SQL_PREFIXES = ("select", "with")
# 显式拒绝的危险关键字（出现在语句中即拒绝，注释/字符串内的也保守拒绝）
_FORBIDDEN_PATTERN = re.compile(
    r"\b("
    r"insert|update|delete|drop|alter|create|replace|truncate|attach|detach|"
    r"vacuum|reindex|pragma|savepoint|release|grant|revoke"
    r")\b",
    re.IGNORECASE,
)


class UploadError(ValueError):
    """上传内容不合法（映射为 HTTP 400）。"""


def validate_sqlite_file(path: str | Path, max_bytes: int) -> None:
    p = Path(path)
    if not p.exists():
        raise UploadError("上传文件不存在")
    size = p.stat().st_size
    if size == 0:
        raise UploadError("上传文件为空，不是有效的 SQLite 数据库")
    if size > max_bytes:
        raise UploadError(
            f"上传文件 {size} 字节，超过上限 {max_bytes} 字节"
        )
    with p.open("rb") as fh:
        header = fh.read(16)
    if header != SQLITE_MAGIC:
        raise UploadError(
            "文件头不是 SQLite 数据库格式（应以 'SQLite format 3' 开头）"
        )
    # 用只读连接做完整性检查；integrity_check 本身是只读 pragma
    try:
        conn = open_readonly_connection(p)
    except sqlite3.DatabaseError as exc:
        raise UploadError(f"无法以只读方式打开 SQLite 文件: {exc}") from exc
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
        if not integrity or integrity[0] != "ok":
            raise UploadError(
                f"SQLite 完整性检查失败: {integrity[0] if integrity else 'unknown'}"
            )
        _validate_required_tables(conn)
    finally:
        conn.close()


def _validate_required_tables(conn: sqlite3.Connection) -> None:
    existing = {
        r["name"]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
        ).fetchall()
    }
    for table, required_cols in REQUIRED_TABLES.items():
        if table not in existing:
            raise UploadError(f"上传库缺少必需表: {table}")
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        missing = required_cols - cols
        if missing:
            raise UploadError(
                f"表 {table} 缺少必需列: {', '.join(sorted(missing))}"
            )


def validate_rule_sql(sql: str) -> str:
    """校验具名查询：单条、只读、可被 SQLite 解析。"""
    if not isinstance(sql, str) or not sql.strip():
        raise UploadError("规则 SQL 不能为空")
    cleaned = sql.strip()
    lowered = cleaned.lower()
    if not lowered.startswith(_ALLOWED_SQL_PREFIXES):
        raise UploadError("规则只允许 SELECT / WITH 查询语句")
    if _FORBIDDEN_PATTERN.search(cleaned):
        raise UploadError(
            "规则中包含写操作或 PRAGMA 等禁用关键字，只读核查不允许执行"
        )
    # 拒绝多语句（sqlite3 execute 本身只执行第一条，这里显式拦截防绕过）
    parts = [s for s in cleaned.split(";") if s.strip()]
    if len(parts) > 1:
        raise UploadError("每条规则只能包含一条 SQL 语句")
    return cleaned


def explain_rule_sql(db_path: str | Path, sql: str) -> None:
    """在上传库上做一次 EXPLAIN 静态解析（不实际产出结果集）。

    用于提交阶段提前发现列名 / 表名错误；EXPLAIN 为只读操作。
    """
    conn = open_readonly_connection(db_path)
    try:
        conn.execute(f"EXPLAIN {sql}").fetchall()
    except sqlite3.Error as exc:
        raise UploadError(f"规则 SQL 无法在上传库上解析: {exc}") from exc
    finally:
        conn.close()


def validate_rules_payload(rules: Any, max_rules: int) -> list[dict[str, str]]:
    if not isinstance(rules, list) or not rules:
        raise UploadError("rules 必须是非空数组，每项含 name 与 sql")
    if len(rules) > max_rules:
        raise UploadError(f"单批规则数超过上限 {max_rules}")
    seen_names: set[str] = set()
    normalized: list[dict[str, str]] = []
    for idx, item in enumerate(rules):
        if not isinstance(item, dict):
            raise UploadError(f"第 {idx + 1} 条规则必须是对象")
        name = item.get("name")
        sql = item.get("sql")
        if not isinstance(name, str) or not name.strip():
            raise UploadError(f"第 {idx + 1} 条规则缺少 name")
        name = name.strip()
        if name in seen_names:
            raise UploadError(f"规则名称重复: {name}")
        seen_names.add(name)
        normalized.append({"name": name, "sql": validate_rule_sql(sql)})
    return normalized
