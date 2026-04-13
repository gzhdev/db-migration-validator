"""Debug 导出功能 - JSONL 写入器和 Debug 导出器"""

import decimal
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import DebugConfig, DebugRecord, FieldMapping, TableMapping

logger = logging.getLogger(__name__)


def _fmt_value(value: Any) -> str:
    """将值转换为字符串，浮点数使用定点表示避免科学计数法"""
    if value is None:
        return 'NULL'
    if isinstance(value, float):
        return format(decimal.Decimal(repr(value)), 'f')
    if isinstance(value, decimal.Decimal):
        return format(value, 'f')
    return str(value)


class JsonlWriter:
    """流式 JSONL 写入器"""

    def __init__(self, output_path: str, buffer_size: int = 1000):
        self.output_path = output_path
        self.buffer_size = buffer_size
        self._buffer: List[Dict[str, Any]] = []
        self._file = None
        self._record_count = 0

    def open(self):
        """打开文件"""
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.output_path, 'w', encoding='utf-8')
        self._buffer = []
        self._record_count = 0

    def write_record(self, record: Dict[str, Any]):
        """写入单条记录 (带缓冲)"""
        if not self._file:
            self.open()

        self._buffer.append(record)
        self._record_count += 1

        if len(self._buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        """刷新缓冲区到文件"""
        if self._file and self._buffer:
            for record in self._buffer:
                self._file.write(json.dumps(record, ensure_ascii=False, default=str) + '\n')
            self._file.flush()
            self._buffer = []

    def close(self):
        """关闭文件"""
        self.flush()
        if self._file:
            self._file.close()
            self._file = None
        logger.info(f"JSONL 文件已保存: {self.output_path} ({self._record_count} 条记录)")

    @property
    def record_count(self) -> int:
        return self._record_count


class DebugExporter:
    """主 Debug 导出器 - 协调 JSONL 输出"""

    def __init__(self, config: DebugConfig):
        self.config = config
        self._jsonl_writer: Optional[JsonlWriter] = None
        self._current_table: str = ""
        self._record_counter: int = 0
        self._table_counter: int = 0

    def start(self):
        """初始化导出器"""
        if not self.config.enabled:
            return

        # 创建输出目录
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)

        logger.info(f"Debug 导出器已启动, 输出目录: {self.config.output_dir}")

    def start_table(self, table_name: str, mapping: TableMapping):
        """开始处理表"""
        if not self.config.enabled:
            return

        self._current_table = table_name
        self._table_counter += 1

        # 创建表的 JSONL 写入器
        if 'jsonl' in self.config.formats:
            # 清理表名用于目录名
            safe_name = table_name.replace(' -> ', '_to_').replace(' ', '_').replace('(', '').replace(')', '')
            table_dir = Path(self.config.output_dir) / safe_name
            jsonl_path = table_dir / "records.jsonl"
            self._jsonl_writer = JsonlWriter(str(jsonl_path), self.config.buffer_size)
            self._jsonl_writer.open()

        logger.debug(f"开始 Debug 记录表: {table_name}")

    def export_record(self, row: Any, mapping: TableMapping, field_alias_map: Dict[str, str],
                     source_columns: List[str], value_check_fields: List[FieldMapping] = None):
        """
        导出单条记录

        Args:
            row: Spark Row 对象
            mapping: 表映射配置
            field_alias_map: 字段别名映射
            source_columns: 原始源表列名列表
            value_check_fields: 需要取值范围校验的字段列表
        """
        if not self.config.enabled:
            return

        # 检查记录数限制
        if self.config.max_records > 0 and self._record_counter >= self.config.max_records:
            return

        # 获取 Row 的字段列表 (Spark 4.x 兼容)
        row_fields = row.__fields__ if hasattr(row, '__fields__') else []

        # 获取匹配类型
        match_type = self._get_row_value(row, '_match_type', row_fields)
        if match_type is None:
            return

        # 确定有效的匹配类型：DataFrame 中字段不一致的行 _match_type 仍为 "matched"，
        # 需结合 _all_fields_match 推导出真正的 "mismatched"
        all_fields_match = self._get_row_value(row, '_all_fields_match', row_fields, default=True)
        effective_match_type = 'mismatched' if (match_type == 'matched' and not all_fields_match) else match_type

        # 检查是否需要记录该类型
        if not self._should_record_type(effective_match_type):
            return

        # 生成记录 ID
        self._record_counter += 1
        record_id = f"{self._table_counter:03d}_{self._record_counter:06d}"

        # 获取主键
        primary_key = self._extract_primary_key(row, mapping, row_fields, match_type)

        # 提取原始源数据
        raw_source = {}
        if self.config.include_raw_source and match_type != 'missing_in_source':
            raw_source = self._extract_raw_source(row, source_columns, row_fields, field_alias_map)
            logger.debug(f"extracted raw_source: {raw_source}")

        # 提取期望值
        expected_values = {}
        if self.config.include_expected and match_type not in ('missing_in_source', 'missing_in_target'):
            expected_values = self._extract_expected_values(row, mapping, row_fields)
            logger.debug(f"extracted expected_values: {expected_values}")

        # 提取目标值
        target_values = {}
        if self.config.include_target and match_type != 'missing_in_target':
            target_values = self._extract_target_values(row, mapping, row_fields)
            logger.debug(f"extracted target_values: {target_values}")

        # 提取比对结果
        comparison_result = {}
        if effective_match_type == 'mismatched':
            comparison_result = self._extract_comparison_result(row, mapping, row_fields)

        # 提取取值范围校验失败
        value_check_failures = []
        if value_check_fields and match_type != 'missing_in_target':
            if not self._get_row_value(row, '_value_check_passed', row_fields, default=True):
                value_check_failures = self._extract_value_check_failures(row, value_check_fields, row_fields)

        # 创建记录
        record = DebugRecord(
            record_id=record_id,
            primary_key=primary_key,
            match_type=effective_match_type,
            raw_source=raw_source,
            expected_values=expected_values,
            target_values=target_values,
            comparison_result=comparison_result,
            value_check_failures=value_check_failures
        )

        # 写入 JSONL
        if self._jsonl_writer:
            self._jsonl_writer.write_record(record.to_dict())

    def end_table(self, stats: Dict[str, int]):
        """结束表处理"""
        if not self.config.enabled:
            return

        # 关闭 JSONL 写入器
        if self._jsonl_writer:
            self._jsonl_writer.close()
            self._jsonl_writer = None

            # 写入 summary.json
            safe_name = self._current_table.replace(' -> ', '_to_').replace(' ', '_').replace('(', '').replace(')', '')
            summary_path = Path(self.config.output_dir) / safe_name / "summary.json"
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)

        logger.debug(f"完成 Debug 记录表: {self._current_table}")

    def close(self):
        """关闭导出器"""
        if not self.config.enabled:
            return

        logger.info(f"Debug 导出完成, 共 {self._record_counter} 条记录")

    def _should_record_type(self, match_type: str) -> bool:
        """检查是否应该记录该类型"""
        type_mapping = {
            'matched': self.config.include_matched,
            'mismatched': self.config.include_mismatched,
            'missing_in_source': self.config.include_missing,
            'missing_in_target': self.config.include_missing,
            'value_check_failed': self.config.include_value_check_failed
        }
        return type_mapping.get(match_type, False)

    def _get_row_value(self, row: Any, field: str, row_fields: List[str], default: Any = None) -> Any:
        """安全获取 Row 字段值 (Spark 4.x 兼容)"""
        # 尝试带表别名
        if f"s.{field}" in row_fields:
            return row[f"s.{field}"]
        if f"t.{field}" in row_fields:
            return row[f"t.{field}"]
        if field in row_fields:
            return row[field]
        return default

    def _extract_primary_key(self, row: Any, mapping: TableMapping, row_fields: List[str], match_type: str) -> Dict[str, Any]:
        """提取主键"""
        pk_fields = [fm.target_field for fm in mapping.field_mappings if fm.is_primary_key]
        if not pk_fields:
            pk_fields = [fm.target_field for fm in mapping.field_mappings]

        pk_dict = {}
        # 优先尝试带 s. 前缀
        source_pk = self._get_row_value(row, 's._source_pk', row_fields)
        if source_pk is None:
            source_pk = self._get_row_value(row, '_source_pk', row_fields)
        # 优先尝试带 t. 前缀
        target_pk = self._get_row_value(row, 't._target_pk', row_fields)
        if target_pk is None:
            target_pk = self._get_row_value(row, '_target_pk', row_fields)

        if match_type == 'missing_in_target' and source_pk is not None:
            # 从 source_pk 结构中提取
            if hasattr(source_pk, '__fields__'):
                for f in source_pk.__fields__:
                    pk_dict[f] = _fmt_value(source_pk[f])
            elif isinstance(source_pk, dict):
                pk_dict = {k: _fmt_value(v) for k, v in source_pk.items()}
            else:
                pk_dict['pk'] = _fmt_value(source_pk)
        elif match_type == 'missing_in_source' and target_pk is not None:
            if hasattr(target_pk, '__fields__'):
                for f in target_pk.__fields__:
                    pk_dict[f] = _fmt_value(target_pk[f])
            elif isinstance(target_pk, dict):
                pk_dict = {k: _fmt_value(v) for k, v in target_pk.items()}
            else:
                pk_dict['pk'] = _fmt_value(target_pk)
        else:
            # 从各字段提取，以字段存在性判断
            for pk_field in pk_fields:
                s_exp_col = f"s._expected_{pk_field}"
                exp_col = f"_expected_{pk_field}"
                if s_exp_col in row_fields:
                    pk_dict[pk_field] = _fmt_value(row[s_exp_col])
                elif exp_col in row_fields:
                    pk_dict[pk_field] = _fmt_value(row[exp_col])
                elif pk_field in row_fields:
                    pk_dict[pk_field] = _fmt_value(row[pk_field])

        return pk_dict if pk_dict else {'pk': 'unknown'}

    def _extract_raw_source(self, row: Any, source_columns: List[str], row_fields: List[str],
                            field_alias_map: Dict[str, str] = None) -> Dict[str, Any]:
        """提取原始源数据，多表模式下 key 还原为 alias.field 格式"""
        # 构建反向映射: alias_field -> alias.field (仅多表模式下有实际映射)
        reverse_alias_map = {}
        if field_alias_map:
            for dotted, col_name in field_alias_map.items():
                if '.' in dotted:  # 只处理多表别名字段
                    reverse_alias_map[col_name] = dotted

        raw_source = {}
        for col_name in source_columns:
            # 尝试不同的列名格式 (join 后列名带 s. 前缀)，以字段存在性判断而非值是否为 None
            col_with_prefix = f"s.{col_name}"
            if col_with_prefix in row_fields:
                value = _fmt_value(row[col_with_prefix])
            elif col_name in row_fields:
                value = _fmt_value(row[col_name])
            else:
                continue
            # 多表模式: 还原 key 为 alias.field 格式
            display_key = reverse_alias_map.get(col_name, col_name)
            raw_source[display_key] = value

        return raw_source

    def _extract_expected_values(self, row: Any, mapping: TableMapping, row_fields: List[str]) -> Dict[str, Any]:
        """提取期望值"""
        expected = {}
        for fm in mapping.field_mappings:
            expected_col = f"_expected_{fm.target_field}"
            s_expected_col = f"s.{expected_col}"
            # 以字段存在性判断，优先带 s. 前缀（join 后列名格式）
            if s_expected_col in row_fields:
                expected[fm.target_field] = _fmt_value(row[s_expected_col])
            elif expected_col in row_fields:
                expected[fm.target_field] = _fmt_value(row[expected_col])
        return expected

    def _extract_target_values(self, row: Any, mapping: TableMapping, row_fields: List[str]) -> Dict[str, Any]:
        """提取目标值"""
        target = {}
        for fm in mapping.field_mappings:
            # 优先使用 _target_* 专用列（join 前预先添加，名称唯一，不受 alias/同名列影响）
            dedicated_col = f"_target_{fm.target_field}"
            t_col = f"t.{fm.target_field}"
            if dedicated_col in row_fields:
                target[fm.target_field] = _fmt_value(row[dedicated_col])
            elif t_col in row_fields:
                target[fm.target_field] = _fmt_value(row[t_col])
            elif fm.target_field in row_fields:
                target[fm.target_field] = _fmt_value(row[fm.target_field])
        return target

    def _extract_comparison_result(self, row: Any, mapping: TableMapping, row_fields: List[str]) -> Dict[str, Any]:
        """提取比对结果"""
        result = {}
        for fm in mapping.field_mappings:
            cmp_col = f"_cmp_{fm.target_field}"
            is_match = self._get_row_value(row, cmp_col, row_fields)
            if is_match is False:  # 明确不匹配
                # 以字段存在性判断，避免将 NULL 值误判为字段缺失而继续 fallback
                s_exp_col = f"s._expected_{fm.target_field}"
                exp_col = f"_expected_{fm.target_field}"
                if s_exp_col in row_fields:
                    expected_val = row[s_exp_col]
                elif exp_col in row_fields:
                    expected_val = row[exp_col]
                else:
                    expected_val = None

                dedicated_col = f"_target_{fm.target_field}"
                t_col = f"t.{fm.target_field}"
                if dedicated_col in row_fields:
                    actual_val = row[dedicated_col]
                elif t_col in row_fields:
                    actual_val = row[t_col]
                elif fm.target_field in row_fields:
                    actual_val = row[fm.target_field]
                else:
                    actual_val = None

                result[fm.target_field] = {
                    'match': False,
                    'expected': _fmt_value(expected_val),
                    'actual': _fmt_value(actual_val)
                }
        return result

    def _extract_value_check_failures(self, row: Any, value_check_fields: List[FieldMapping], row_fields: List[str]) -> List[Dict[str, Any]]:
        """提取取值范围校验失败"""
        failures = []
        for fm in value_check_fields:
            check_col = f"_value_check_{fm.target_field}"
            is_passed = self._get_row_value(row, check_col, row_fields)
            if is_passed is False:
                # 优先使用 _target_* 专用列，避免同名列或 alias 格式不一致
                dedicated_col = f"_target_{fm.target_field}"
                t_col = f"t.{fm.target_field}"
                if dedicated_col in row_fields:
                    field_value = row[dedicated_col]
                elif t_col in row_fields:
                    field_value = row[t_col]
                elif fm.target_field in row_fields:
                    field_value = row[fm.target_field]
                else:
                    field_value = None

                # 构建失败原因
                reasons = []
                if field_value is None:
                    if not fm.nullable:
                        reasons.append("字段不允许为空")
                else:
                    if fm.min_value is not None and field_value < fm.min_value:
                        reasons.append(f"小于最小值 {fm.min_value}")
                    if fm.max_value is not None and field_value > fm.max_value:
                        reasons.append(f"大于最大值 {fm.max_value}")
                    if fm.allowed_values and field_value not in fm.allowed_values:
                        reasons.append(f"不在允许值列表中")
                    if fm.pattern:
                        reasons.append(f"不匹配正则 {fm.pattern}")
                    if fm.value_check_expr:
                        reasons.append(f"不满足条件 {fm.value_check_expr}")

                failures.append({
                    'field': fm.target_field,
                    'value': _fmt_value(field_value),
                    'reasons': reasons
                })
        return failures
