#!/usr/bin/env python3
"""生成"长文本 GLOB 卡死"演示库。

用法：
    python demo/make_glob_stall_db.py demo/glob_stall.db

库内容：
- subjects 表有一行 enrolled_on 为 50 万个 'a' 的长文本（LONG-001）；
- 搭配 demo/rules.glob-stall.json 中的 GLOB 规则（星号 + 2 万个 a + 末尾 b），
  单次内置函数匹配自然完成约需 22 秒，且 sqlite3 interrupt 无法中止——
  用于演示服务在 5 秒时限 / 取消时通过 SIGKILL 真正停止计算，
  同批后续规则与并发批次均不受影响。
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


def build(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE subjects (
                id          TEXT PRIMARY KEY,
                enrolled_on TEXT
            );
            CREATE TABLE visits (
                id         INTEGER PRIMARY KEY,
                subject_id TEXT NOT NULL,
                visited_on TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO subjects(id, enrolled_on) VALUES ('LONG-001', ?)",
            ("a" * 500_000,),
        )
        conn.execute(
            "INSERT INTO subjects(id, enrolled_on) VALUES ('S001', '2026-01-10')"
        )
        conn.execute(
            "INSERT INTO visits(id, subject_id, visited_on) VALUES (1, 'S001', '2026-01-20')"
        )
        conn.commit()
    finally:
        conn.close()
    print(f"glob stall db written: {db_path.resolve()} ({db_path.stat().st_size} bytes)")


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "demo/glob_stall.db")
    build(target)
