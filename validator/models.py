"""数据模型定义"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class SourceTable:
    """多表 JOIN 中的源表配置"""
    table_name: str
    alias: str  # 表别名 (必需)
    schema: Optional[str] = None
    join_type: str = "inner"  # inner, left, right, full, cross, primary (仅第一个表使用)
    join_condition: Optional[str] = None  # 除第一个表外必需


@dataclass
class FieldMapping:
    """字段映射配置"""
    target_field: str
    source_field: Optional[str] = None
    transform: Optional[Dict[str, Any]] = None
    is_primary_key: bool = False
    nullable: bool = True
    compare_rule: str = "exact"
    tolerance: Optional[float] = None
    description: str = ""
    # 取值范围校验配置
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    pattern: Optional[str] = None  # 正则表达式
    value_check_expr: Optional[str] = None  # 自定义 Spark SQL 表达式
    # 多表 JOIN 支持
    source_alias: Optional[str] = None  # 源表别名，用于多表 JOIN


@dataclass
class TableMapping:
    """表映射配置"""
    source_table: Optional[str] = None  # 单表模式 (向后兼容)
    target_table: str = ""
    field_mappings: List[FieldMapping] = field(default_factory=list)
    source_schema: Optional[str] = None
    target_schema: Optional[str] = None
    description: str = ""
    source_filter: Optional[str] = None  # 单表模式过滤条件 (向后兼容)
    target_filter: Optional[str] = None
    sample_size: int = 0
    batch_size: int = 1000
    # 多表 JOIN 支持
    source_tables: List[SourceTable] = field(default_factory=list)
    table_filters: Dict[str, str] = field(default_factory=dict)  # 各表过滤条件，key 为 alias

    def is_multi_source(self) -> bool:
        """判断是否为多表 JOIN 模式"""
        return len(self.source_tables) > 0


@dataclass
class ValidationResult:
    """校验结果"""
    table_name: str
    total_rows: int = 0
    matched_rows: int = 0
    mismatched_rows: int = 0
    missing_in_source: int = 0
    missing_in_target: int = 0
    value_check_failed: int = 0  # 取值范围校验失败数
    errors: List[Dict[str, Any]] = field(default_factory=list)
    duration_seconds: float = 0.0
    status: str = "pending"


@dataclass
class DebugConfig:
    """Debug 配置"""
    enabled: bool = False
    output_dir: str = "./debug"
    formats: List[str] = field(default_factory=lambda: ["jsonl"])
    max_records: int = 10000
    # record types
    include_matched: bool = False
    include_mismatched: bool = True
    include_missing: bool = True
    include_value_check_failed: bool = True
    # data content
    include_raw_source: bool = True
    include_expected: bool = True
    include_target: bool = True
    buffer_size: int = 1000

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'DebugConfig':
        """从配置字典创建 DebugConfig"""
        if not config:
            return cls()

        record_types = config.get('record_types', {})
        data_content = config.get('data_content', {})

        return cls(
            enabled=config.get('enabled', False),
            output_dir=config.get('output_dir', './debug'),
            formats=config.get('formats', ['jsonl']),
            max_records=config.get('max_records', 10000),
            include_matched=record_types.get('matched', False),
            include_mismatched=record_types.get('mismatched', True),
            include_missing=record_types.get('missing_in_source', True) and record_types.get('missing_in_target', True),
            include_value_check_failed=record_types.get('value_check_failed', True),
            include_raw_source=data_content.get('include_raw_source', True),
            include_expected=data_content.get('include_expected', True),
            include_target=data_content.get('include_target', True),
            buffer_size=config.get('buffer_size', 1000)
        )


@dataclass
class DebugRecord:
    """单条 debug 记录"""
    record_id: str
    primary_key: Dict[str, Any]
    match_type: str  # matched, mismatched, missing_in_source, missing_in_target, value_check_failed
    raw_source: Dict[str, Any] = field(default_factory=dict)
    expected_values: Dict[str, Any] = field(default_factory=dict)
    target_values: Dict[str, Any] = field(default_factory=dict)
    comparison_result: Dict[str, Any] = field(default_factory=dict)
    value_check_failures: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'record_id': self.record_id,
            'primary_key': self.primary_key,
            'match_type': self.match_type,
            'raw_source': self.raw_source,
            'expected_values': self.expected_values,
            'target_values': self.target_values,
            'comparison_result': self.comparison_result,
            'value_check_failures': self.value_check_failures
        }
