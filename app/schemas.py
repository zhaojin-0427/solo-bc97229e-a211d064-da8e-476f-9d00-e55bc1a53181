"""请求 / 响应模型。"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class RuleInput(BaseModel):
    name: str = Field(..., description="规则名称，批次内唯一", examples=["subject_missing_visits"])
    sql: str = Field(
        ...,
        description=(
            "具名只读核查 SQL，仅允许单条 SELECT/WITH；"
            "返回违规记录，零行才算通过"
        ),
    )


class RuleResultOut(BaseModel):
    seq: int
    rule_name: str
    sql_text: str
    status: str = Field(
        ...,
        description=(
            "passed=零行通过；violations=有违规；timeout=超时已中断；"
            "error=执行出错；cancelled=随批次取消；interrupted=进程中断"
        ),
    )
    violation_count: Optional[int] = None
    sample_rows: Optional[list[dict[str, Any]]] = None
    error_message: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class BatchSummary(BaseModel):
    id: str
    name: str
    source_filename: str
    status: str
    can_load: bool = Field(..., description="仅全部规则零违规通过且批次完成时为 true")
    total_violations: int
    total_violations_complete: bool = Field(
        ...,
        description="违规总数是否完整：超时/取消/中断的规则无法计数时为 false",
    )
    rule_count: int
    passed_rules: int
    failed_rules: int
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[str] = None


class BatchDetail(BatchSummary):
    rules: list[RuleResultOut]
    db_path: str = Field(..., description="服务端保存的上传库副本路径（只读使用）")


class CancelResponse(BaseModel):
    batch_id: str
    cancelled_signal_sent: bool = Field(
        ..., description="true=取消信号已送达运行中的批次；false=批次已结束/不存在"
    )
    status: str
    message: str


class HealthResponse(BaseModel):
    status: str
    version: str
    interrupted_on_startup: list[str]
