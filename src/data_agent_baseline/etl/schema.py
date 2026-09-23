"""抽取契约与确定性规范化。"""

from __future__ import annotations

import math
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Column(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1)
    type: Literal["VARCHAR", "BIGINT", "DOUBLE", "BOOLEAN", "DATE"] = "VARCHAR"
    description: str = ""
    unit: str | None = None


class DocumentSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str = Field(min_length=1)
    columns: list[Column] = Field(min_length=2)
    primary_key: list[str] = Field(min_length=1)
    identity_labels: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_columns(self):
        names = [col.name for col in self.columns]
        if len({name.casefold() for name in names}) != len(names):
            raise ValueError("Schema 列名必须唯一（忽略大小写）")
        if not set(self.primary_key).issubset(names):
            raise ValueError("主键必须属于 Schema；纵向观测必须包含日期等粒度键")
        return self


def normalize(value: Any, column: Column) -> Any:
    if value is None or (
        isinstance(value, str) and value.strip().lower() in {"", "none", "null", "nan", "n/a"}
    ):
        return None
    if isinstance(value, (dict, list)):
        raise ValueError("字段必须为标量")
    if column.type == "VARCHAR":
        return str(value).strip()
    if column.type == "BOOLEAN":
        if value in (True, False):
            return bool(value)
        if str(value).lower() in {"true", "false"}:
            return str(value).lower() == "true"
        raise ValueError("无效布尔值")
    if column.type == "DATE":
        from datetime import date

        return date.fromisoformat(str(value)).isoformat()
    # 不盲目删除百分号、单位或近似值标记，不执行无契约的单位换算。
    text = str(value).strip()
    if re.fullmatch(r"[+-]?\d{1,3}(,\d{3})+(\.\d+)?", text):
        text = text.replace(",", "")
    number = float(text)
    if not math.isfinite(number):
        raise ValueError("数值必须有限")
    if column.type == "BIGINT":
        from decimal import Decimal

        exact = Decimal(text)
        if exact != exact.to_integral_value():
            raise ValueError("整数字段出现小数")
        return int(exact)
    return number
