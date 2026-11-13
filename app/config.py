"""运行期配置，全部可通过环境变量覆盖。"""
from __future__ import annotations

import os
from pathlib import Path


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


# 数据根目录：上传库副本、控制库（报告持久化）都放这里，挂载为 Docker volume
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data")).resolve()

# 每条规则 SQL 的计算时限（秒）。届时父进程 SIGKILL 规则工作子进程，真正停止计算。
QUERY_TIMEOUT_SECONDS = _int("QUERY_TIMEOUT_SECONDS", 5)

# 报告中每条规则保留的违规明细行数
SAMPLE_ROW_LIMIT = _int("SAMPLE_ROW_LIMIT", 20)

# 控制库文件名（服务自身的报告库，与上传库严格分开）
CONTROL_DB_FILENAME = "control.db"

# 上传库副本子目录
UPLOADS_DIRNAME = "uploads"

# 允许提交的规则数量上限，防止单批过大
MAX_RULES_PER_BATCH = _int("MAX_RULES_PER_BATCH", 100)

# 上传文件大小上限（字节），默认 200MB
MAX_UPLOAD_BYTES = _int("MAX_UPLOAD_BYTES", 200 * 1024 * 1024)


def uploads_dir() -> Path:
    path = DATA_DIR / UPLOADS_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def control_db_path() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / CONTROL_DB_FILENAME
