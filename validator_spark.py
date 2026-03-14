#!/usr/bin/env python3
"""
数据库迁移数据一致性校验工具 - Spark 版本
Database Migration Data Consistency Validator - Spark Version

使用 PySpark 进行分布式计算，支持大数据量的迁移校验。
"""

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
from pyspark.sql.functions import col, when, lit, concat_ws, upper, lower, trim, substring, regexp_replace, coalesce, to_json, from_json, expr

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


@dataclass
class TableMapping:
    """表映射配置"""
    source_table: str
    target_table: str
    field_mappings: List[FieldMapping]
    source_schema: Optional[str] = None
    target_schema: Optional[str] = None
    description: str = ""
    source_filter: Optional[str] = None
    target_filter: Optional[str] = None
    sample_size: int = 0
    batch_size: int = 1000


@dataclass
class ValidationResult:
    """校验结果"""
    table_name: str
    total_rows: int = 0
    matched_rows: int = 0
    mismatched_rows: int = 0
    missing_in_source: int = 0
    missing_in_target: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    duration_seconds: float = 0.0
    status: str = "pending"


class SparkDataValidator:
    """Spark 数据校验器"""
    
    def __init__(self, mapping_config: Dict[str, Any], spark: SparkSession = None):
        self.config = mapping_config
        self.spark = spark or self._create_spark_session()
        self.results: List[ValidationResult] = []
        
        # JDBC 连接属性缓存
        self._source_jdbc_url = None
        self._target_jdbc_url = None
        self._source_props = None
        self._target_props = None
    
    def _create_spark_session(self) -> SparkSession:
        """创建 Spark Session"""
        builder = SparkSession.builder \
            .appName("DB-Migration-Validator") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # 可以根据需要添加更多配置
        # .config("spark.executor.memory", "4g") \
        # .config("spark.driver.memory", "2g") \
        
        return builder.getOrCreate()
    
    def _get_jdbc_url(self, db_config: Dict[str, Any]) -> str:
        """生成 JDBC URL"""
        db_type = db_config.get('type', 'mysql').lower()
        host = db_config['host']
        port = db_config['port']
        database = db_config['database']
        
        jdbc_urls = {
            'mysql': f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&serverTimezone=UTC",
            'postgresql': f"jdbc:postgresql://{host}:{port}/{database}",
            'oracle': f"jdbc:oracle:thin:@{host}:{port}:{database}",
            'sqlserver': f"jdbc:sqlserver://{host}:{port};databaseName={database}",
        }
        
        if db_type not in jdbc_urls:
            raise ValueError(f"不支持的数据库类型: {db_type}")
        
        return jdbc_urls[db_type]
    
    def _get_jdbc_properties(self, db_config: Dict[str, Any]) -> Dict[str, str]:
        """生成 JDBC 连接属性"""
        password = db_config.get('password')
        if not password and db_config.get('password_env'):
            password = os.environ.get(db_config['password_env'])
        
        props = {
            'user': db_config.get('user', ''),
            'password': password or '',
        }
        
        # MySQL 驱动
        db_type = db_config.get('type', 'mysql').lower()
        drivers = {
            'mysql': 'com.mysql.cj.jdbc.Driver',
            'postgresql': 'org.postgresql.Driver',
            'oracle': 'oracle.jdbc.driver.OracleDriver',
            'sqlserver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        }
        
        if db_type in drivers:
            props['driver'] = drivers[db_type]
        
        return props
    
    def _get_full_table_name(self, table: str, schema: str = None, db_type: str = 'mysql') -> str:
        """获取完整表名"""
        if schema:
            return f"{schema}.{table}"
        return table
    
    def load_table_mapping(self, table_config: Dict[str, Any]) -> TableMapping:
        """加载表映射配置"""
        field_mappings = []
        for fm in table_config['field_mappings']:
            field_mappings.append(FieldMapping(
                target_field=fm['target_field'],
                source_field=fm.get('source_field'),
                transform=fm.get('transform'),
                is_primary_key=fm.get('is_primary_key', False),
                nullable=fm.get('nullable', True),
                compare_rule=fm.get('compare_rule', 'exact'),
                tolerance=fm.get('tolerance'),
                description=fm.get('description', '')
            ))
        
        filters = table_config.get('filters', {})
        return TableMapping(
            source_table=table_config['source_table'],
            target_table=table_config['target_table'],
            field_mappings=field_mappings,
            source_schema=table_config.get('source_schema'),
            target_schema=table_config.get('target_schema'),
            description=table_config.get('description', ''),
            source_filter=filters.get('source_filter'),
            target_filter=filters.get('target_filter'),
            sample_size=table_config.get('sample_size', 0),
            batch_size=table_config.get('batch_size', 1000)
        )
    
    def read_source_table(self, mapping: TableMapping) -> DataFrame:
        """读取源表数据 (优化: 谓词下推 + 并行读取)"""
        jdbc_url = self._get_jdbc_url(self.config['source_db'])
        props = self._get_jdbc_properties(self.config['source_db'])
        
        # 收集需要的源字段
        source_fields = set()
        for fm in mapping.field_mappings:
            if fm.source_field:
                source_fields.add(fm.source_field)
            elif fm.transform and fm.transform.get('fields'):
                source_fields.update(fm.transform['fields'])
        
        source_fields = list(source_fields)
        
        # 构建查询 - 使用子查询实现谓词下推
        table = self._get_full_table_name(
            mapping.source_table, 
            mapping.source_schema,
            self.config['source_db'].get('type', 'mysql')
        )
        
        # 构建带过滤条件的子查询 (谓词下推到数据库)
        if mapping.source_filter:
            query = f"(SELECT {', '.join(source_fields)} FROM {table} WHERE {mapping.source_filter}) AS subq"
        else:
            query = f"(SELECT {', '.join(source_fields)} FROM {table}) AS subq"
        
        # JDBC 并行读取配置
        options = {
            'url': jdbc_url,
            'dbtable': query,
            **props
        }
        
        # 如果配置了分区列，启用并行读取
        pk_fields = [fm.source_field for fm in mapping.field_mappings if fm.is_primary_key]
        if pk_fields and mapping.batch_size > 0:
            # 获取主键范围用于分区
            try:
                bounds_query = f"(SELECT MIN({pk_fields[0]}) as min_val, MAX({pk_fields[0]}) as max_val FROM {table}) AS bounds"
                bounds_df = self.spark.read.jdbc(url=jdbc_url, table=bounds_query, properties=props)
                bounds = bounds_df.first()
                if bounds and bounds['min_val'] is not None:
                    options['partitionColumn'] = pk_fields[0]
                    options['lowerBound'] = bounds['min_val']
                    options['upperBound'] = bounds['max_val']
                    options['numPartitions'] = max(4, mapping.batch_size // 10000)
            except Exception as e:
                logger.warning(f"无法获取分区边界，使用默认读取: {e}")
        
        df = self.spark.read.format("jdbc").options(**options).load()
        
        # 抽样
        if mapping.sample_size > 0:
            df = df.sample(withReplacement=False, fraction=1.0).limit(mapping.sample_size)
        
        return df
    
    def read_target_table(self, mapping: TableMapping) -> DataFrame:
        """读取目标表数据 (优化: 谓词下推 + 并行读取)"""
        jdbc_url = self._get_jdbc_url(self.config['target_db'])
        props = self._get_jdbc_properties(self.config['target_db'])
        
        # 收集目标字段
        target_fields = [fm.target_field for fm in mapping.field_mappings]
        
        # 构建查询
        table = self._get_full_table_name(
            mapping.target_table,
            mapping.target_schema,
            self.config['target_db'].get('type', 'mysql')
        )
        
        # 构建带过滤条件的子查询 (谓词下推)
        if mapping.target_filter:
            query = f"(SELECT {', '.join(target_fields)} FROM {table} WHERE {mapping.target_filter}) AS subq"
        else:
            query = f"(SELECT {', '.join(target_fields)} FROM {table}) AS subq"
        
        # JDBC 并行读取配置
        options = {
            'url': jdbc_url,
            'dbtable': query,
            **props
        }
        
        # 如果配置了分区列，启用并行读取
        pk_fields = [fm.target_field for fm in mapping.field_mappings if fm.is_primary_key]
        if pk_fields and mapping.batch_size > 0:
            try:
                bounds_query = f"(SELECT MIN({pk_fields[0]}) as min_val, MAX({pk_fields[0]}) as max_val FROM {table}) AS bounds"
                bounds_df = self.spark.read.jdbc(url=jdbc_url, table=bounds_query, properties=props)
                bounds = bounds_df.first()
                if bounds and bounds['min_val'] is not None:
                    options['partitionColumn'] = pk_fields[0]
                    options['lowerBound'] = bounds['min_val']
                    options['upperBound'] = bounds['max_val']
                    options['numPartitions'] = max(4, mapping.batch_size // 10000)
            except Exception as e:
                logger.warning(f"无法获取分区边界，使用默认读取: {e}")
        
        df = self.spark.read.format("jdbc").options(**options).load()
        return df
    
    def apply_transform(self, df: DataFrame, transform: Dict[str, Any], 
                        field_mappings: List[FieldMapping]) -> DataFrame:
        """应用字段转换，返回包含转换后字段的 DataFrame"""
        transform_type = transform.get('type')
        
        # 收集源字段
        fields = transform.get('fields', [])
        output_field = f"_transformed_{transform_type}"
        
        if transform_type == 'concat':
            separator = transform.get('separator', '')
            df = df.withColumn(output_field, concat_ws(separator, *[col(f) for f in fields]))
        
        elif transform_type == 'upper':
            source_field = fields[0] if fields else None
            if source_field:
                df = df.withColumn(output_field, upper(col(source_field)))
        
        elif transform_type == 'lower':
            source_field = fields[0] if fields else None
            if source_field:
                df = df.withColumn(output_field, lower(col(source_field)))
        
        elif transform_type == 'trim':
            source_field = fields[0] if fields else None
            if source_field:
                df = df.withColumn(output_field, trim(col(source_field)))
        
        elif transform_type == 'substring':
            source_field = fields[0] if fields else None
            start = transform.get('start', 1)
            length = transform.get('length')
            if source_field:
                if length:
                    df = df.withColumn(output_field, substring(col(source_field), start, length))
                else:
                    df = df.withColumn(output_field, substring(col(source_field), start, 1000000))
        
        elif transform_type == 'replace':
            source_field = fields[0] if fields else None
            pattern = transform.get('pattern', '')
            replacement = transform.get('replacement', '')
            if source_field:
                df = df.withColumn(output_field, regexp_replace(col(source_field), pattern, replacement))
        
        elif transform_type == 'constant':
            value = transform.get('value')
            df = df.withColumn(output_field, lit(value))
        
        elif transform_type == 'coalesce':
            default = transform.get('default')
            cols_to_check = [col(f) for f in fields]
            if default is not None:
                cols_to_check.append(lit(default))
            df = df.withColumn(output_field, coalesce(*cols_to_check))
        
        elif transform_type == 'case':
            source_field = fields[0] if fields else None
            cases = transform.get('cases', [])
            default_value = transform.get('else')
            
            if source_field:
                case_expr = None
                for c in reversed(cases):
                    if case_expr is None:
                        case_expr = when(col(source_field) == c['when'], lit(c['then']))
                    else:
                        case_expr = when(col(source_field) == c['when'], lit(c['then'])).otherwise(case_expr)
                
                if case_expr is not None:
                    if default_value is not None:
                        case_expr = case_expr.otherwise(lit(default_value))
                    df = df.withColumn(output_field, case_expr)
        
        elif transform_type == 'cast':
            source_field = fields[0] if fields else None
            cast_type = transform.get('cast_type', 'string')
            type_map = {
                'string': StringType(),
                'int': IntegerType(),
                'float': DoubleType(),
                'bool': BooleanType(),
            }
            if source_field and cast_type in type_map:
                df = df.withColumn(output_field, col(source_field).cast(type_map[cast_type]))
        
        elif transform_type == 'math':
            expression = transform.get('expression', '')
            if expression:
                # 替换字段名为列引用
                expr_replaced = expression
                for f in fields:
                    expr_replaced = expr_replaced.replace(f, f"`{f}`")
                df = df.withColumn(output_field, expr(expr_replaced))
        
        else:
            # 未知转换类型，直接取第一个字段
            if fields:
                df = df.withColumn(output_field, col(fields[0]))
        
        return df
    
    def build_source_df(self, source_df: DataFrame, mapping: TableMapping) -> DataFrame:
        """构建源 DataFrame，包含所有需要的比较字段"""
        df = source_df
        
        # 为每个字段映射添加期望值列
        for fm in mapping.field_mappings:
            expected_col = f"_expected_{fm.target_field}"
            
            if fm.source_field:
                # 直接映射
                df = df.withColumn(expected_col, col(fm.source_field))
            
            elif fm.transform:
                # 需要转换
                df = self.apply_transform(df, fm.transform, mapping.field_mappings)
                transform_type = fm.transform.get('type')
                transform_col = f"_transformed_{transform_type}"
                df = df.withColumn(expected_col, col(transform_col))
                df = df.drop(transform_col)  # 清理临时列
        
        return df
    
    def validate_table(self, mapping: TableMapping) -> ValidationResult:
        """校验单表数据 (优化: 单次 FULL JOIN + 聚合统计)"""
        result = ValidationResult(
            table_name=f"{mapping.source_table} -> {mapping.target_table}"
        )
        start_time = datetime.now()
        
        try:
            logger.info(f"读取源表: {mapping.source_table}")
            source_df = self.read_source_table(mapping)
            
            logger.info(f"读取目标表: {mapping.target_table}")
            target_df = self.read_target_table(mapping)
            
            # 构建源 DataFrame（包含期望值）
            source_with_expected = self.build_source_df(source_df, mapping)
            
            # 获取主键字段
            pk_fields = [fm.target_field for fm in mapping.field_mappings if fm.is_primary_key]
            if not pk_fields:
                logger.warning(f"表 {mapping.source_table} 没有配置主键，使用全部字段")
                pk_fields = [fm.target_field for fm in mapping.field_mappings]
            
            # 构建主键列 - 使用 struct 更安全 (避免分隔符冲突)
            source_pk_cols = [col(f"_expected_{f}") for f in pk_fields]
            target_pk_cols = [col(f) for f in pk_fields]
            
            source_with_expected = source_with_expected.withColumn("_source_pk", F.struct(*source_pk_cols))
            target_df = target_df.withColumn("_target_pk", F.struct(*target_pk_cols))
            
            # 缓存数据
            source_with_expected.persist()
            target_df.persist()
            
            # 统计记录数 (触发缓存)
            source_count = source_with_expected.count()
            result.total_rows = source_count
            logger.info(f"源表记录数: {source_count}")
            
            # ========== 优化: 单次 FULL OUTER JOIN 完成所有比较 ==========
            joined_df = source_with_expected.alias("s").join(
                target_df.alias("t"),
                col("s._source_pk") == col("t._target_pk"),
                "full_outer"
            )
            
            # 添加分类标记
            classified_df = joined_df.withColumn(
                "_match_type",
                when(col("s._source_pk").isNull(), "missing_in_source")
                .when(col("t._target_pk").isNull(), "missing_in_target")
                .otherwise("matched")
            )
            
            # 对匹配记录添加字段比较结果
            for fm in mapping.field_mappings:
                expected_col = f"s._expected_{fm.target_field}"
                actual_col = f"t.{fm.target_field}"
                result_col = f"_cmp_{fm.target_field}"
                
                if fm.compare_rule == 'skip':
                    classified_df = classified_df.withColumn(result_col, lit(True))
                elif fm.compare_rule == 'exact':
                    classified_df = classified_df.withColumn(
                        result_col,
                        col(expected_col).eqNullSafe(col(actual_col))
                    )
                elif fm.compare_rule == 'ignore_case':
                    classified_df = classified_df.withColumn(
                        result_col,
                        lower(col(expected_col)).eqNullSafe(lower(col(actual_col)))
                    )
                elif fm.compare_rule == 'ignore_whitespace':
                    classified_df = classified_df.withColumn(
                        result_col,
                        F.regexp_replace(col(expected_col), r'\s+', '').eqNullSafe(
                            F.regexp_replace(col(actual_col), r'\s+', '')
                        )
                    )
                elif fm.compare_rule == 'numeric_tolerance':
                    tol = fm.tolerance or 0.0
                    classified_df = classified_df.withColumn(
                        result_col,
                        F.abs(col(expected_col) - col(actual_col)) <= tol
                    )
                else:
                    classified_df = classified_df.withColumn(
                        result_col,
                        col(expected_col).eqNullSafe(col(actual_col))
                    )
            
            # 计算所有字段是否匹配
            cmp_cols = [col(f"_cmp_{fm.target_field}") for fm in mapping.field_mappings]
            all_match_expr = cmp_cols[0]
            for c in cmp_cols[1:]:
                all_match_expr = all_match_expr & c
            
            classified_df = classified_df.withColumn("_all_fields_match", all_match_expr)
            
            # ========== 优化: 单次聚合计算所有统计值 ==========
            stats = classified_df.agg(
                F.sum(when(col("_match_type") == "missing_in_source", 1).otherwise(0)).alias("missing_in_source"),
                F.sum(when(col("_match_type") == "missing_in_target", 1).otherwise(0)).alias("missing_in_target"),
                F.sum(when((col("_match_type") == "matched") & col("_all_fields_match"), 1).otherwise(0)).alias("fully_matched"),
                F.sum(when((col("_match_type") == "matched") & ~col("_all_fields_match"), 1).otherwise(0)).alias("mismatched")
            ).first()
            
            result.missing_in_source = stats["missing_in_source"] or 0
            result.missing_in_target = stats["missing_in_target"] or 0
            result.matched_rows = stats["fully_matched"] or 0
            result.mismatched_rows = stats["mismatched"] or 0
            
            logger.info(f"匹配: {result.matched_rows}, 不匹配: {result.mismatched_rows}, "
                       f"源缺失: {result.missing_in_source}, 目标缺失: {result.missing_in_target}")
            
            # 收集错误样本 (限制数量)
            if result.missing_in_target > 0:
                sample_errors = classified_df.filter(col("_match_type") == "missing_in_target").limit(10).collect()
                for row in sample_errors:
                    result.errors.append({
                        'type': 'missing_in_target',
                        'key': row['s._source_pk']
                    })
            
            if result.missing_in_source > 0:
                sample_errors = classified_df.filter(col("_match_type") == "missing_in_source").limit(10).collect()
                for row in sample_errors:
                    result.errors.append({
                        'type': 'missing_in_source',
                        'key': row['t._target_pk']
                    })
            
            if result.mismatched_rows > 0:
                sample_errors = classified_df.filter(
                    (col("_match_type") == "matched") & ~col("_all_fields_match")
                ).limit(10).collect()
                
                for row in sample_errors:
                    mismatched_fields = []
                    for fm in mapping.field_mappings:
                        if not row[f"_cmp_{fm.target_field}"]:
                            mismatched_fields.append({
                                'field': fm.target_field,
                                'expected': str(row[f"s._expected_{fm.target_field}"]),
                                'actual': str(row[f"t.{fm.target_field}"])
                            })
                    
                    result.errors.append({
                        'type': 'field_mismatch',
                        'key': row['s._source_pk'],
                        'mismatched_fields': mismatched_fields
                    })
            
            # 清理缓存
            source_with_expected.unpersist()
            target_df.unpersist()
            
            result.status = "completed"
            logger.info(f"完成表 {mapping.source_table} 校验")
            
        except Exception as e:
            logger.error(f"校验表 {mapping.source_table} 时出错: {e}")
            result.status = "error"
            result.errors.append({
                'type': 'exception',
                'error': str(e)
            })
        
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result
    
    def run_validation(self) -> List[ValidationResult]:
        """运行全部校验"""
        tables = self.config.get('tables', [])
        
        for table_config in tables:
            mapping = self.load_table_mapping(table_config)
            result = self.validate_table(mapping)
            self.results.append(result)
        
        return self.results
    
    def generate_report(self, output_dir: str = None, formats: List[str] = None):
        """生成报告"""
        if not output_dir:
            output_dir = self.config.get('global_settings', {}).get('output_dir', './reports')
        if not formats:
            formats = self.config.get('global_settings', {}).get('report_format', ['json'])
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if 'json' in formats:
            report = {
                'generated_at': datetime.now().isoformat(),
                'config_version': self.config.get('version'),
                'summary': {
                    'total_tables': len(self.results),
                    'completed': sum(1 for r in self.results if r.status == 'completed'),
                    'errors': sum(1 for r in self.results if r.status == 'error'),
                    'total_mismatched': sum(r.mismatched_rows for r in self.results),
                    'total_missing_source': sum(r.missing_in_source for r in self.results),
                    'total_missing_target': sum(r.missing_in_target for r in self.results),
                },
                'results': [
                    {
                        'table_name': r.table_name,
                        'total_rows': r.total_rows,
                        'matched_rows': r.matched_rows,
                        'mismatched_rows': r.mismatched_rows,
                        'missing_in_source': r.missing_in_source,
                        'missing_in_target': r.missing_in_target,
                        'duration_seconds': r.duration_seconds,
                        'status': r.status,
                        'errors': r.errors[:100]
                    }
                    for r in self.results
                ]
            }
            
            report_file = output_path / f'validation_report_{timestamp}.json'
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            logger.info(f"JSON 报告已生成: {report_file}")
        
        if 'markdown' in formats:
            report_file = output_path / f'validation_report_{timestamp}.md'
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("# 数据一致性校验报告\n\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("## 概要\n\n")
                f.write(f"- 校验表数: {len(self.results)}\n")
                f.write(f"- 成功: {sum(1 for r in self.results if r.status == 'completed')}\n")
                f.write(f"- 失败: {sum(1 for r in self.results if r.status == 'error')}\n")
                f.write(f"- 不匹配记录: {sum(r.mismatched_rows for r in self.results)}\n")
                f.write(f"- 源表缺失: {sum(r.missing_in_source for r in self.results)}\n")
                f.write(f"- 目标表缺失: {sum(r.missing_in_target for r in self.results)}\n\n")
                
                f.write("## 表详情\n\n")
                for r in self.results:
                    f.write(f"### {r.table_name}\n\n")
                    f.write(f"- 状态: {r.status}\n")
                    f.write(f"- 总行数: {r.total_rows}\n")
                    f.write(f"- 匹配: {r.matched_rows}\n")
                    f.write(f"- 不匹配: {r.mismatched_rows}\n")
                    f.write(f"- 耗时: {r.duration_seconds:.2f}s\n\n")
            
            logger.info(f"Markdown 报告已生成: {report_file}")
    
    def stop(self):
        """停止 Spark Session"""
        if self.spark:
            self.spark.stop()


def load_config(config_path: str) -> Dict[str, Any]:
    """加载配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    if len(sys.argv) < 2:
        print("用法: python validator_spark.py <config.json> [--master <spark-master>]")
        print("示例: python validator_spark.py mapping_example.json --master spark://localhost:7077")
        sys.exit(1)
    
    config_path = sys.argv[1]
    master = None
    
    if '--master' in sys.argv:
        master_idx = sys.argv.index('--master')
        if master_idx + 1 < len(sys.argv):
            master = sys.argv[master_idx + 1]
    
    # 加载配置
    config = load_config(config_path)
    
    # 创建 Spark 配置
    spark_builder = SparkSession.builder.appName("DB-Migration-Validator")
    if master:
        spark_builder = spark_builder.master(master)
    
    spark = spark_builder.getOrCreate()
    
    # 创建校验器
    validator = SparkDataValidator(config, spark)
    
    try:
        # 运行校验
        logger.info("开始数据一致性校验 (Spark)...")
        validator.run_validation()
        
        # 生成报告
        validator.generate_report()
        
        # 打印摘要
        print("\n=== 校验摘要 ===")
        for result in validator.results:
            print(f"{result.table_name}: {result.status}")
            print(f"  匹配: {result.matched_rows}/{result.total_rows}")
            if result.mismatched_rows > 0:
                print(f"  不匹配: {result.mismatched_rows}")
            if result.missing_in_source > 0:
                print(f"  源表缺失: {result.missing_in_source}")
            if result.missing_in_target > 0:
                print(f"  目标表缺失: {result.missing_in_target}")
        
        print("\n校验完成！")
        
    finally:
        validator.stop()


if __name__ == '__main__':
    main()