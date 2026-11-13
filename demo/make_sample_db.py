#!/usr/bin/env python3
"""生成演示用 SQLite 副本（含 subjects / visits 两张表及预置违规数据）。

用法（容器内或本地均可）：
    python demo/make_sample_db.py demo/sample_ctms.db

预置问题：
- S002 有访视但没有入组日期（enrolled_on 为空）；
- S003 的两次访视早于入组日期；
- S004 在 visits 中完全没有记录（受试者缺失访视）；
- S001 存在重复访视（同 subject_id + visited_on 两行）；
- 存在 subject_id 指向不存在受试者的孤儿访视。
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
        subjects = [
            ("S001", "2026-01-10"),   # 正常 + 一条重复访视
            ("S002", None),           # 缺入组日期，却有访视
            ("S003", "2026-03-01"),   # 访视早于入组
            ("S004", "2026-02-15"),   # 无任何访视
        ]
        conn.executemany(
            "INSERT INTO subjects(id, enrolled_on) VALUES (?, ?)", subjects
        )

        visits = [
            # S001 正常随访，外加一条同日重复
            (1, "S001", "2026-01-20"),
            (2, "S001", "2026-02-20"),
            (3, "S001", "2026-02-20"),  # 重复
            # S002 未入组先访视
            (4, "S002", "2026-01-05"),
            # S003 两次访视均早于 2026-03-01 入组
            (5, "S003", "2026-02-10"),
            (6, "S003", "2026-02-25"),
            # S004 无访视（受试者缺失访视）
            # 孤儿访视：受试者不存在
            (7, "S999", "2026-04-01"),
            # S001 再来一次正常访视
            (8, "S001", "2026-03-20"),
        ]
        conn.executemany(
            "INSERT INTO visits(id, subject_id, visited_on) VALUES (?, ?, ?)",
            visits,
        )
        conn.commit()
    finally:
        conn.close()
    print(f"sample db written: {db_path.resolve()} ({db_path.stat().st_size} bytes)")


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "demo/sample_ctms.db")
    build(target)
