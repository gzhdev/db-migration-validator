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
        port = db_config.get('port')
        database = db_config.get('database', '')
        
        if db_type == 'mysql':
            port = port or 3306
            return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&serverTimezone=UTC"
        
        elif db_type == 'postgresql':
            port = port or 5432
            return f"jdbc:postgresql://{host}:{port}/{database}"
        
        elif db_type == 'oracle':
            port = port or 1521
            service_name = db_config.get('service_name')
            sid = db_config.get('sid')
            
            if service_name:
                # 使用 service_name 格式
                return f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
            elif sid:
                # 使用 SID 格式
                return f"jdbc:oracle:thin:@{host}:{port}:{sid}"
            else:
                # 默认使用 database 作为 service_name
                if database:
                    return f"jdbc:oracle:thin:@//{host}:{port}/{database}"
                else:
                    return f"jdbc:oracle:thin:@{host}:{port}"
        
        elif db_type == 'sqlserver':
            port = port or 1433
            return f"jdbc:sqlserver://{host}:{port};databaseName={database}"
        
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")
    
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

    def build_source_query(self, mapping: TableMapping) -> Tuple[str, Dict[str, str]]:
        """
        构建源表查询 SQL
        返回: (query_sql, field_alias_map) - 查询语句和字段别名映射 (alias.field -> field)
        """
        if mapping.is_multi_source():
            return self._build_multi_table_join_query(mapping)
        else:
            return self._build_single_table_query(mapping)

    def _build_single_table_query(self, mapping: TableMapping) -> Tuple[str, Dict[str, str]]:
        """构建单表查询 SQL"""
        # 收集需要的源字段
        source_fields = set()
        for fm in mapping.field_mappings:
            if fm.source_field:
                # 去除别名前缀 (如果有)
                field_name = fm.source_field.split('.')[-1] if '.' in fm.source_field else fm.source_field
                source_fields.add(field_name)
            elif fm.transform and fm.transform.get('fields'):
                for f in fm.transform['fields']:
                    field_name = f.split('.')[-1] if '.' in f else f
                    source_fields.add(field_name)

        source_fields = list(source_fields)

        # 构建表名
        table = self._get_full_table_name(
            mapping.source_table,
            mapping.source_schema,
            self.config['source_db'].get('type', 'mysql')
        )

        db_type = self.config['source_db'].get('type', 'mysql').lower()
        alias_prefix = "" if db_type == 'oracle' else "AS "

        # 构建带过滤条件的子查询 (谓词下推到数据库)
        if mapping.source_filter:
            query = f"(SELECT {', '.join(source_fields)} FROM {table} WHERE {mapping.source_filter}) {alias_prefix}subq"
        else:
            query = f"(SELECT {', '.join(source_fields)} FROM {table}) {alias_prefix}subq"

        # 字段映射 (单表模式下无别名前缀)
        field_alias_map = {f: f for f in source_fields}

        return query, field_alias_map

    def _build_multi_table_join_query(self, mapping: TableMapping) -> Tuple[str, Dict[str, str]]:
        """构建多表 JOIN 查询 SQL"""
        if not mapping.source_tables:
            raise ValueError("多表 JOIN 模式需要配置 source_tables")

        db_type = self.config['source_db'].get('type', 'mysql').lower()
        is_oracle = db_type == 'oracle'

        # 收集需要的源字段 (带别名)
        source_fields_with_alias = self._collect_source_fields_with_alias(mapping)

        # 构建字段列表 (alias.field AS alias_field)
        select_fields = []
        field_alias_map = {}  # alias.field -> 实际列名 (alias_field)

        for alias, field in source_fields_with_alias:
            col_name = f"{alias}_{field}"  # 在 DataFrame 中的列名
            if is_oracle:
                # Oracle 不支持 AS 别名
                select_fields.append(f"{alias}.{field} {col_name}")
            else:
                select_fields.append(f"{alias}.{field} AS {col_name}")
            field_alias_map[f"{alias}.{field}"] = col_name

        # 构建 FROM 子句 (第一个表)
        first_table = mapping.source_tables[0]
        first_table_name = self._get_full_table_name(
            first_table.table_name,
            first_table.schema or mapping.source_schema,
            db_type
        )

        if is_oracle:
            from_clause = f"{first_table_name} {first_table.alias}"
        else:
            from_clause = f"{first_table_name} AS {first_table.alias}"

        # 构建 JOIN 子句
        join_clauses = []
        where_conditions = []

        # 第一个表的过滤条件
        if first_table.alias in mapping.table_filters:
            where_conditions.append(mapping.table_filters[first_table.alias])

        for source_table in mapping.source_tables[1:]:
            table_name = self._get_full_table_name(
                source_table.table_name,
                source_table.schema or mapping.source_schema,
                db_type
            )

            join_type = source_table.join_type.upper()
            if join_type == 'PRIMARY':
                join_type = 'INNER'  # primary 作为 inner 处理

            # Oracle 不支持 AS 别名
            if is_oracle:
                table_ref = f"{table_name} {source_table.alias}"
            else:
                table_ref = f"{table_name} AS {source_table.alias}"

            join_condition = source_table.join_condition
            if not join_condition:
                raise ValueError(f"表 {source_table.table_name} (alias: {source_table.alias}) 缺少 join_condition")

            join_clauses.append(f"{join_type} JOIN {table_ref} ON {join_condition}")

            # 添加该表的过滤条件
            if source_table.alias in mapping.table_filters:
                where_conditions.append(mapping.table_filters[source_table.alias])

        # 组装完整 SQL
        sql_parts = [
            f"SELECT {', '.join(select_fields)}",
            f"FROM {from_clause}"
        ]
        sql_parts.extend(join_clauses)

        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")

        query = "(" + " ".join(sql_parts) + ") subq"

        return query, field_alias_map

    def _collect_source_fields_with_alias(self, mapping: TableMapping) -> List[Tuple[str, str]]:
        """
        收集所有需要的源字段 (带别名)
        返回: [(alias, field_name), ...]
        """
        fields_with_alias = set()

        for fm in mapping.field_mappings:
            if fm.source_field:
                # 解析 alias.field 格式
                if '.' in fm.source_field:
                    alias, field = fm.source_field.split('.', 1)
                    fields_with_alias.add((alias, field))
                elif fm.source_alias:
                    fields_with_alias.add((fm.source_alias, fm.source_field))
                else:
                    # 无别名，使用第一个表的别名
                    if mapping.source_tables:
                        first_alias = mapping.source_tables[0].alias
                        fields_with_alias.add((first_alias, fm.source_field))
                    else:
                        # 单表模式，无别名
                        fields_with_alias.add(('', fm.source_field))

            elif fm.transform and fm.transform.get('fields'):
                for f in fm.transform['fields']:
                    if '.' in f:
                        alias, field = f.split('.', 1)
                        fields_with_alias.add((alias, field))
                    elif mapping.source_tables:
                        first_alias = mapping.source_tables[0].alias
                        fields_with_alias.add((first_alias, f))
                    else:
                        fields_with_alias.add(('', f))

        # 过滤掉空别名的情况 (单表模式)
        return [(a, f) for a, f in fields_with_alias if a]
    
    def load_table_mapping(self, table_config: Dict[str, Any]) -> TableMapping:
        """加载表映射配置 (支持单表和多表 JOIN 模式)"""
        field_mappings = self._parse_field_mappings(table_config.get('field_mappings', []))

        # 解析多表 JOIN 配置
        source_tables = []
        if 'source_tables' in table_config:
            for st in table_config['source_tables']:
                source_tables.append(SourceTable(
                    table_name=st['table_name'],
                    alias=st['alias'],
                    schema=st.get('schema'),
                    join_type=st.get('join_type', 'inner'),
                    join_condition=st.get('join_condition')
                ))

        filters = table_config.get('filters', {})
        table_filters = table_config.get('table_filters', {})

        return TableMapping(
            source_table=table_config.get('source_table'),  # 单表模式
            target_table=table_config['target_table'],
            field_mappings=field_mappings,
            source_schema=table_config.get('source_schema'),
            target_schema=table_config.get('target_schema'),
            description=table_config.get('description', ''),
            source_filter=filters.get('source_filter'),  # 单表模式过滤条件
            target_filter=filters.get('target_filter'),
            sample_size=table_config.get('sample_size', 0),
            batch_size=table_config.get('batch_size', 1000),
            source_tables=source_tables,
            table_filters=table_filters
        )

    def _parse_field_mappings(self, field_configs: List[Dict[str, Any]]) -> List[FieldMapping]:
        """解析字段映射配置"""
        field_mappings = []
        for fm in field_configs:
            source_field = fm.get('source_field')
            source_alias = None

            # 解析带别名的字段引用 (e.g., "o.id" -> alias="o", field="id")
            if source_field and '.' in source_field:
                parts = source_field.split('.', 1)
                source_alias = parts[0]
                # source_field 保持原样，在后续处理中会解析

            field_mappings.append(FieldMapping(
                target_field=fm['target_field'],
                source_field=source_field,
                transform=fm.get('transform'),
                is_primary_key=fm.get('is_primary_key', False),
                nullable=fm.get('nullable', True),
                compare_rule=fm.get('compare_rule', 'exact'),
                tolerance=fm.get('tolerance'),
                description=fm.get('description', ''),
                min_value=fm.get('min_value'),
                max_value=fm.get('max_value'),
                allowed_values=fm.get('allowed_values'),
                pattern=fm.get('pattern'),
                value_check_expr=fm.get('value_check_expr'),
                source_alias=source_alias
            ))
        return field_mappings

    def validate_table_mapping(self, mapping: TableMapping) -> List[str]:
        """
        验证表映射配置的有效性

        Returns:
            错误消息列表，空列表表示验证通过
        """
        errors = []

        if mapping.is_multi_source():
            # 验证别名唯一性
            aliases = [st.alias for st in mapping.source_tables]
            if len(aliases) != len(set(aliases)):
                duplicate_aliases = [a for a in aliases if aliases.count(a) > 1]
                errors.append(f"源表别名重复: {set(duplicate_aliases)}")

            # 验证 JOIN 条件完整性 (第一个表除外)
            for i, st in enumerate(mapping.source_tables):
                if i == 0:
                    # 第一个表应该使用 'primary' 或无 join_type
                    if st.join_type.lower() not in ('primary', 'inner'):
                        errors.append(f"第一个源表 {st.table_name} (alias: {st.alias}) 的 join_type 应为 'primary'")
                else:
                    # 其他表必须有 join_condition
                    if not st.join_condition:
                        errors.append(f"源表 {st.table_name} (alias: {st.alias}) 缺少 join_condition")

            # 验证字段引用的别名有效性
            valid_aliases = set(aliases)
            for fm in mapping.field_mappings:
                if fm.source_field and '.' in fm.source_field:
                    alias = fm.source_field.split('.')[0]
                    if alias not in valid_aliases:
                        errors.append(f"字段 {fm.source_field} 引用了无效的表别名 '{alias}'")

                if fm.transform and fm.transform.get('fields'):
                    for f in fm.transform['fields']:
                        if '.' in f:
                            alias = f.split('.')[0]
                            if alias not in valid_aliases:
                                errors.append(f"Transform 字段 {f} 引用了无效的表别名 '{alias}'")

        else:
            # 单表模式验证
            if not mapping.source_table:
                errors.append("单表模式需要配置 source_table")

        return errors
    
    def read_source_table(self, mapping: TableMapping) -> Tuple[DataFrame, Dict[str, str]]:
        """
        读取源表数据 (优化: 谓词下推 + 并行读取)
        返回: (DataFrame, field_alias_map) - 数据帧和字段别名映射
        """
        jdbc_url = self._get_jdbc_url(self.config['source_db'])
        props = self._get_jdbc_properties(self.config['source_db'])

        # 使用新的查询构建器
        query, field_alias_map = self.build_source_query(mapping)

        # JDBC 读取配置
        options = {
            'url': jdbc_url,
            'dbtable': query,
            **props
        }

        # 多表 JOIN 模式不支持分区读取
        if not mapping.is_multi_source():
            # 单表模式: 如果配置了分区列，启用并行读取
            # 注意：只使用有 source_field 的主键字段（transform 字段不能用于分区）
            pk_fields = [fm.source_field for fm in mapping.field_mappings
                         if fm.is_primary_key and fm.source_field]
            if pk_fields and mapping.batch_size > 0:
                pk_field = pk_fields[0]
                # 去除别名前缀
                if '.' in pk_field:
                    pk_field = pk_field.split('.')[-1]

                table = self._get_full_table_name(
                    mapping.source_table,
                    mapping.source_schema,
                    self.config['source_db'].get('type', 'mysql')
                )
                db_type = self.config['source_db'].get('type', 'mysql').lower()
                alias_prefix = "" if db_type == 'oracle' else "AS "

                try:
                    bounds_query = f"(SELECT MIN({pk_field}) as min_val, MAX({pk_field}) as max_val FROM {table}) {alias_prefix}bounds"
                    bounds_df = self.spark.read.jdbc(url=jdbc_url, table=bounds_query, properties=props)
                    bounds = bounds_df.first()
                    if bounds and bounds['min_val'] is not None:
                        options['partitionColumn'] = pk_field
                        options['lowerBound'] = bounds['min_val']
                        options['upperBound'] = bounds['max_val']
                        options['numPartitions'] = max(4, mapping.batch_size // 10000)
                except Exception as e:
                    logger.warning(f"无法获取分区边界，使用默认读取: {e}")

        df = self.spark.read.format("jdbc").options(**options).load()

        # 抽样
        if mapping.sample_size > 0:
            df = df.sample(withReplacement=False, fraction=1.0).limit(mapping.sample_size)

        return df, field_alias_map
    
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
        
        db_type = self.config['target_db'].get('type', 'mysql').lower()
        
        # Oracle 不支持 AS 别名语法，需要直接使用别名
        alias_prefix = "" if db_type == 'oracle' else "AS "
        
        # 构建带过滤条件的子查询 (谓词下推)
        if mapping.target_filter:
            query = f"(SELECT {', '.join(target_fields)} FROM {table} WHERE {mapping.target_filter}) {alias_prefix}subq"
        else:
            query = f"(SELECT {', '.join(target_fields)} FROM {table}) {alias_prefix}subq"
        
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
                bounds_query = f"(SELECT MIN({pk_fields[0]}) as min_val, MAX({pk_fields[0]}) as max_val FROM {table}) {alias_prefix}bounds"
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
                        field_alias_map: Dict[str, str] = None) -> DataFrame:
        """
        应用字段转换，返回包含转换后字段的 DataFrame

        Args:
            df: 源 DataFrame
            transform: 转换配置
            field_alias_map: 字段别名映射 (alias.field -> 实际列名)
        """
        transform_type = transform.get('type')
        field_alias_map = field_alias_map or {}

        # 收集源字段并转换为实际列名
        fields = transform.get('fields', [])
        actual_fields = []
        for f in fields:
            actual_col = field_alias_map.get(f, f)
            # 如果字段名带别名但未在映射中，尝试去除别名
            if '.' in f and f not in field_alias_map:
                actual_col = f.split('.')[-1]
            actual_fields.append((f, actual_col))

        output_field = f"_transformed_{transform_type}"

        if transform_type == 'concat':
            separator = transform.get('separator', '')
            df = df.withColumn(output_field, concat_ws(separator, *[col(actual) for _, actual in actual_fields]))

        elif transform_type == 'upper':
            if actual_fields:
                df = df.withColumn(output_field, upper(col(actual_fields[0][1])))

        elif transform_type == 'lower':
            if actual_fields:
                df = df.withColumn(output_field, lower(col(actual_fields[0][1])))

        elif transform_type == 'trim':
            if actual_fields:
                df = df.withColumn(output_field, trim(col(actual_fields[0][1])))

        elif transform_type == 'substring':
            if actual_fields:
                start = transform.get('start', 1)
                length = transform.get('length')
                if length:
                    df = df.withColumn(output_field, substring(col(actual_fields[0][1]), start, length))
                else:
                    df = df.withColumn(output_field, substring(col(actual_fields[0][1]), start, 1000000))

        elif transform_type == 'replace':
            if actual_fields:
                pattern = transform.get('pattern', '')
                replacement = transform.get('replacement', '')
                df = df.withColumn(output_field, regexp_replace(col(actual_fields[0][1]), pattern, replacement))

        elif transform_type == 'constant':
            value = transform.get('value')
            df = df.withColumn(output_field, lit(value))

        elif transform_type == 'coalesce':
            default = transform.get('default')
            cols_to_check = [col(actual) for _, actual in actual_fields]
            if default is not None:
                cols_to_check.append(lit(default))
            df = df.withColumn(output_field, coalesce(*cols_to_check))

        elif transform_type == 'case':
            if actual_fields:
                actual_col = actual_fields[0][1]
                cases = transform.get('cases', [])
                default_value = transform.get('else')

                case_expr = None
                for c in cases:
                    if case_expr is None:
                        case_expr = when(col(actual_col) == c['when'], lit(c['then']))
                    else:
                        case_expr = case_expr.when(col(actual_col) == c['when'], lit(c['then']))

                if case_expr is not None:
                    if default_value is not None:
                        case_expr = case_expr.otherwise(lit(default_value))
                    df = df.withColumn(output_field, case_expr)

        elif transform_type == 'cast':
            if actual_fields:
                cast_type = transform.get('cast_type', 'string')
                type_map = {
                    'string': StringType(),
                    'int': IntegerType(),
                    'float': DoubleType(),
                    'bool': BooleanType(),
                }
                if cast_type in type_map:
                    df = df.withColumn(output_field, col(actual_fields[0][1]).cast(type_map[cast_type]))

        elif transform_type == 'math':
            expression = transform.get('expression', '')
            if expression:
                # 替换字段名为实际列名
                expr_replaced = expression
                for orig_field, actual_col in actual_fields:
                    # 替换带别名的字段引用 (o.price -> `o_price`)
                    expr_replaced = expr_replaced.replace(orig_field, f"`{actual_col}`")
                df = df.withColumn(output_field, expr(expr_replaced))

        else:
            # 未知转换类型，直接取第一个字段
            if actual_fields:
                df = df.withColumn(output_field, col(actual_fields[0][1]))

        return df

    def build_source_df(self, source_df: DataFrame, mapping: TableMapping,
                        field_alias_map: Dict[str, str] = None) -> DataFrame:
        """
        构建源 DataFrame，包含所有需要的比较字段

        Args:
            source_df: 源数据 DataFrame
            mapping: 表映射配置
            field_alias_map: 字段别名映射 (alias.field -> 实际列名)
        """
        df = source_df
        field_alias_map = field_alias_map or {}

        # 为每个字段映射添加期望值列
        for fm in mapping.field_mappings:
            expected_col = f"_expected_{fm.target_field}"

            if fm.source_field:
                # 直接映射 - 获取实际列名
                actual_col = field_alias_map.get(fm.source_field, fm.source_field)
                if '.' in fm.source_field and fm.source_field not in field_alias_map:
                    actual_col = fm.source_field.split('.')[-1]
                df = df.withColumn(expected_col, col(actual_col))

            elif fm.transform:
                # 需要转换
                df = self.apply_transform(df, fm.transform, field_alias_map)
                transform_type = fm.transform.get('type')
                transform_col = f"_transformed_{transform_type}"
                df = df.withColumn(expected_col, col(transform_col))
                df = df.drop(transform_col)  # 清理临时列

        return df

    def validate_table(self, mapping: TableMapping) -> ValidationResult:
        """校验单表数据 (优化: 单次 FULL JOIN + 聚合统计)"""
        # 构建源表描述 (支持多表 JOIN)
        if mapping.is_multi_source():
            source_desc = "JOIN(".join(st.alias for st in mapping.source_tables) + ")"
        else:
            source_desc = mapping.source_table or "unknown"

        result = ValidationResult(
            table_name=f"{source_desc} -> {mapping.target_table}"
        )
        start_time = datetime.now()

        try:
            logger.info(f"读取源表: {source_desc}")
            source_df, field_alias_map = self.read_source_table(mapping)

            logger.info(f"读取目标表: {mapping.target_table}")
            target_df = self.read_target_table(mapping)

            # 构建源 DataFrame（包含期望值）
            source_with_expected = self.build_source_df(source_df, mapping, field_alias_map)

            # 获取主键字段
            pk_fields = [fm.target_field for fm in mapping.field_mappings if fm.is_primary_key]
            if not pk_fields:
                logger.warning(f"表 {source_desc} 没有配置主键，使用全部字段")
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

            # ========== 取值范围校验 ==========
            value_check_fields = [fm for fm in mapping.field_mappings
                                  if any([fm.min_value is not None, fm.max_value is not None,
                                          fm.allowed_values, fm.pattern, fm.value_check_expr])]

            if value_check_fields:
                for fm in value_check_fields:
                    check_col = f"_value_check_{fm.target_field}"
                    actual_col = f"t.{fm.target_field}"  # 使用表别名避免歧义

                    # 构建非 NULL 值的校验表达式
                    non_null_check_expr = lit(True)

                    # 最小值校验
                    if fm.min_value is not None:
                        non_null_check_expr = non_null_check_expr & (col(actual_col) >= fm.min_value)

                    # 最大值校验
                    if fm.max_value is not None:
                        non_null_check_expr = non_null_check_expr & (col(actual_col) <= fm.max_value)

                    # 允许值列表校验
                    if fm.allowed_values:
                        non_null_check_expr = non_null_check_expr & col(actual_col).isin(fm.allowed_values)

                    # 正则表达式校验
                    if fm.pattern:
                        non_null_check_expr = non_null_check_expr & col(actual_col).rlike(fm.pattern)

                    # 自定义表达式校验
                    if fm.value_check_expr:
                        # 替换字段名，添加表别名
                        custom_expr = fm.value_check_expr.replace(fm.target_field, f"`t`.`{fm.target_field}`")
                        non_null_check_expr = non_null_check_expr & expr(custom_expr)

                    # 组合最终校验表达式：NULL 值处理
                    if fm.nullable:
                        # 允许 NULL：NULL 值直接通过，非 NULL 值需要校验
                        check_expr = col(actual_col).isNull() | non_null_check_expr
                    else:
                        # 不允许 NULL：必须非空且通过校验
                        check_expr = col(actual_col).isNotNull() & non_null_check_expr

                    classified_df = classified_df.withColumn(check_col, check_expr)

                # 计算取值范围校验结果
                value_check_cols = [col(f"_value_check_{fm.target_field}") for fm in value_check_fields]
                all_value_check_expr = value_check_cols[0]
                for c in value_check_cols[1:]:
                    all_value_check_expr = all_value_check_expr & c

                classified_df = classified_df.withColumn("_value_check_passed", all_value_check_expr)

                # 统计取值范围校验失败数（仅统计目标表存在的记录）
                value_check_stats = classified_df.filter(col("_match_type") != "missing_in_target").agg(
                    F.sum(when(~col("_value_check_passed"), 1).otherwise(0)).alias("value_check_failed")
                ).first()

                result.value_check_failed = value_check_stats["value_check_failed"] or 0

                if result.value_check_failed > 0:
                    logger.info(f"取值范围校验失败: {result.value_check_failed}")
            else:
                classified_df = classified_df.withColumn("_value_check_passed", lit(True))
            
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
                        'key': row['_source_pk']
                    })
            
            if result.missing_in_source > 0:
                sample_errors = classified_df.filter(col("_match_type") == "missing_in_source").limit(10).collect()
                for row in sample_errors:
                    result.errors.append({
                        'type': 'missing_in_source',
                        'key': row['_target_pk']
                    })
            
            if result.mismatched_rows > 0:
                sample_errors = classified_df.filter(
                    (col("_match_type") == "matched") & ~col("_all_fields_match")
                ).limit(10).collect()

                for row in sample_errors:
                    mismatched_fields = []
                    for fm in mapping.field_mappings:
                        if not row[f"_cmp_{fm.target_field}"]:
                            expected_val = row[f"_expected_{fm.target_field}"]
                            actual_val = row[fm.target_field]
                            mismatched_fields.append({
                                'field': fm.target_field,
                                'expected': str(expected_val) if expected_val is not None else 'NULL',
                                'actual': str(actual_val) if actual_val is not None else 'NULL'
                            })

                    result.errors.append({
                        'type': 'field_mismatch',
                        'key': row['_source_pk'],
                        'mismatched_fields': mismatched_fields
                    })

            # 收集取值范围校验失败样本
            if result.value_check_failed > 0:
                sample_errors = classified_df.filter(
                    (col("_match_type") != "missing_in_target") & ~col("_value_check_passed")
                ).limit(10).collect()

                # 获取 Row 的字段名列表
                row_fields = sample_errors[0].__fields__ if sample_errors else []

                for row in sample_errors:
                    failed_checks = []
                    for fm in value_check_fields:
                        if not row[f"_value_check_{fm.target_field}"]:
                            check_reasons = []
                            # 获取目标表字段值 (尝试多种列名格式)
                            field_value = None
                            t_col_name = f"t.{fm.target_field}"
                            if t_col_name in row_fields:
                                field_value = row[t_col_name]
                            elif fm.target_field in row_fields:
                                field_value = row[fm.target_field]

                            # NULL 值处理
                            if field_value is None:
                                if not fm.nullable:
                                    check_reasons.append("字段不允许为空")
                            else:
                                # 非 NULL 值的校验
                                if fm.min_value is not None and field_value < fm.min_value:
                                    check_reasons.append(f"小于最小值 {fm.min_value}")
                                if fm.max_value is not None and field_value > fm.max_value:
                                    check_reasons.append(f"大于最大值 {fm.max_value}")
                                if fm.allowed_values and field_value not in fm.allowed_values:
                                    check_reasons.append(f"不在允许值列表中 {fm.allowed_values}")
                                if fm.pattern and not __import__('re').match(fm.pattern, str(field_value)):
                                    check_reasons.append(f"不匹配正则 {fm.pattern}")
                                if fm.value_check_expr:
                                    check_reasons.append(f"不满足条件 {fm.value_check_expr}")

                            failed_checks.append({
                                'field': fm.target_field,
                                'value': str(field_value) if field_value is not None else 'NULL',
                                'reasons': check_reasons
                            })

                    # 获取主键值
                    key_value = None
                    if '_source_pk' in row_fields:
                        key_value = row['_source_pk']
                    elif '_target_pk' in row_fields:
                        key_value = row['_target_pk']

                    result.errors.append({
                        'type': 'value_check_failed',
                        'key': key_value,
                        'failed_checks': failed_checks
                    })

            # 清理缓存
            source_with_expected.unpersist()
            target_df.unpersist()

            result.status = "completed"
            logger.info(f"完成表 {source_desc} 校验")

        except Exception as e:
            logger.error(f"校验表 {source_desc} 时出错: {e}")
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

            # 验证配置
            validation_errors = self.validate_table_mapping(mapping)
            if validation_errors:
                # 配置验证失败，创建错误结果
                if mapping.is_multi_source():
                    source_desc = "JOIN(".join(st.alias for st in mapping.source_tables) + ")"
                else:
                    source_desc = mapping.source_table or "unknown"

                result = ValidationResult(
                    table_name=f"{source_desc} -> {mapping.target_table}",
                    status="error"
                )
                for err in validation_errors:
                    result.errors.append({
                        'type': 'config_validation',
                        'error': err
                    })
                    logger.error(f"配置验证失败: {err}")
                self.results.append(result)
                continue

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
                    'total_value_check_failed': sum(r.value_check_failed for r in self.results),
                },
                'results': [
                    {
                        'table_name': r.table_name,
                        'total_rows': r.total_rows,
                        'matched_rows': r.matched_rows,
                        'mismatched_rows': r.mismatched_rows,
                        'missing_in_source': r.missing_in_source,
                        'missing_in_target': r.missing_in_target,
                        'value_check_failed': r.value_check_failed,
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
                f.write(f"- 目标表缺失: {sum(r.missing_in_target for r in self.results)}\n")
                f.write(f"- 取值范围校验失败: {sum(r.value_check_failed for r in self.results)}\n\n")

                f.write("## 表详情\n\n")
                for r in self.results:
                    f.write(f"### {r.table_name}\n\n")
                    f.write(f"- 状态: {r.status}\n")
                    f.write(f"- 总行数: {r.total_rows}\n")
                    f.write(f"- 匹配: {r.matched_rows}\n")
                    f.write(f"- 不匹配: {r.mismatched_rows}\n")
                    f.write(f"- 源表缺失: {r.missing_in_source}\n")
                    f.write(f"- 目标表缺失: {r.missing_in_target}\n")
                    f.write(f"- 取值范围失败: {r.value_check_failed}\n")
                    f.write(f"- 耗时: {r.duration_seconds:.2f}s\n\n")

                    # 输出错误样本
                    if r.errors:
                        f.write("#### 错误样本\n\n")

                        # 字段不匹配的记录
                        mismatches = [e for e in r.errors if e.get('type') == 'field_mismatch']
                        if mismatches:
                            f.write("**字段不匹配:**\n\n")
                            for i, err in enumerate(mismatches[:5], 1):
                                f.write(f"{i}. 主键: `{err.get('key')}`\n")
                                for field_err in err.get('mismatched_fields', []):
                                    f.write(f"   - `{field_err['field']}`: 期望 `{field_err['expected']}` → 实际 `{field_err['actual']}`\n")
                                f.write("\n")

                        # 取值范围校验失败的记录
                        value_check_errors = [e for e in r.errors if e.get('type') == 'value_check_failed']
                        if value_check_errors:
                            f.write("**取值范围校验失败:**\n\n")
                            for i, err in enumerate(value_check_errors[:5], 1):
                                f.write(f"{i}. 主键: `{err.get('key')}`\n")
                                for check_err in err.get('failed_checks', []):
                                    reasons = ", ".join(check_err['reasons'])
                                    f.write(f"   - `{check_err['field']}`: 值 `{check_err['value']}` - {reasons}\n")
                                f.write("\n")

                        # 目标表缺失的记录
                        missing_target = [e for e in r.errors if e.get('type') == 'missing_in_target']
                        if missing_target:
                            f.write("**目标表缺失:**\n\n")
                            keys = [str(e.get('key')) for e in missing_target[:5]]
                            f.write(", ".join(f"`{k}`" for k in keys) + "\n\n")

                        # 源表缺失的记录
                        missing_source = [e for e in r.errors if e.get('type') == 'missing_in_source']
                        if missing_source:
                            f.write("**源表缺失:**\n\n")
                            keys = [str(e.get('key')) for e in missing_source[:5]]
                            f.write(", ".join(f"`{k}`" for k in keys) + "\n\n")

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