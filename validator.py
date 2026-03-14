#!/usr/bin/env python3
"""
数据库迁移数据一致性校验工具
Database Migration Data Consistency Validator

用于验证数据库迁移后源表和目标表的数据一致性。
支持灵活的字段映射配置，包括直接映射、字段转换、条件映射等。
"""

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 尝试导入数据库驱动
try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False

try:
    import pyodbc
    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLSERVER_AVAILABLE = False

try:
    import sqlite3
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False


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


class DatabaseConnection(ABC):
    """数据库连接抽象类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
    
    @abstractmethod
    def connect(self):
        """建立连接"""
        pass
    
    @abstractmethod
    def close(self):
        """关闭连接"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """执行查询，返回字典列表"""
        pass
    
    @abstractmethod
    def get_table_count(self, table: str, schema: str = None, filter_clause: str = None) -> int:
        """获取表记录数"""
        pass
    
    @abstractmethod
    def get_primary_keys(self, table: str, schema: str = None) -> List[str]:
        """获取表的主键"""
        pass


class MySQLConnection(DatabaseConnection):
    """MySQL 连接"""
    
    def connect(self):
        if not MYSQL_AVAILABLE:
            raise ImportError("mysql-connector-python 未安装")
        
        password = self.config.get('password')
        if not password and self.config.get('password_env'):
            password = os.environ.get(self.config['password_env'])
        
        self.connection = mysql.connector.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config.get('user'),
            password=password
        )
    
    def close(self):
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def get_table_count(self, table: str, schema: str = None, filter_clause: str = None) -> int:
        query = f"SELECT COUNT(*) as cnt FROM {table}"
        if filter_clause:
            query += f" WHERE {filter_clause}"
        result = self.execute_query(query)
        return result[0]['cnt'] if result else 0
    
    def get_primary_keys(self, table: str, schema: str = None) -> List[str]:
        db = self.config['database']
        query = """
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s 
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """
        results = self.execute_query(query, (db, table))
        return [r['COLUMN_NAME'] for r in results]


class PostgreSQLConnection(DatabaseConnection):
    """PostgreSQL 连接"""
    
    def connect(self):
        if not POSTGRES_AVAILABLE:
            raise ImportError("psycopg2 未安装")
        
        password = self.config.get('password')
        if not password and self.config.get('password_env'):
            password = os.environ.get(self.config['password_env'])
        
        self.connection = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config.get('user'),
            password=password
        )
    
    def close(self):
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()
    
    def get_table_count(self, table: str, schema: str = None, filter_clause: str = None) -> int:
        full_table = f"{schema}.{table}" if schema else table
        query = f'SELECT COUNT(*) as cnt FROM {full_table}'
        if filter_clause:
            query += f" WHERE {filter_clause}"
        result = self.execute_query(query)
        return result[0]['cnt'] if result else 0
    
    def get_primary_keys(self, table: str, schema: str = None) -> List[str]:
        schema = schema or 'public'
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        """
        full_table = f"{schema}.{table}"
        results = self.execute_query(query, (full_table,))
        return [r['attname'] for r in results]


class SQLiteConnection(DatabaseConnection):
    """SQLite 连接"""
    
    def connect(self):
        if not SQLITE_AVAILABLE:
            raise ImportError("sqlite3 未安装（标准库应自带）")
        
        self.connection = sqlite3.connect(self.config['database'])
        self.connection.row_factory = sqlite3.Row
    
    def close(self):
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or ())
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()
    
    def get_table_count(self, table: str, schema: str = None, filter_clause: str = None) -> int:
        query = f'SELECT COUNT(*) as cnt FROM {table}'
        if filter_clause:
            query += f" WHERE {filter_clause}"
        result = self.execute_query(query)
        return result[0]['cnt'] if result else 0
    
    def get_primary_keys(self, table: str, schema: str = None) -> List[str]:
        query = f"PRAGMA table_info({table})"
        results = self.execute_query(query)
        return [r['name'] for r in results if r.get('pk')]


class OracleConnection(DatabaseConnection):
    """Oracle 连接 (使用 python-oracledb)"""
    
    def connect(self):
        if not ORACLE_AVAILABLE:
            raise ImportError("oracledb (python-oracledb) 未安装，请执行: pip install oracledb")
        
        password = self.config.get('password')
        if not password and self.config.get('password_env'):
            password = os.environ.get(self.config['password_env'])
        
        # 构建 DSN
        host = self.config['host']
        port = self.config.get('port', 1521)
        service_name = self.config.get('service_name')
        sid = self.config.get('sid')
        
        if service_name:
            dsn = f"{host}:{port}/{service_name}"
        elif sid:
            dsn = oracledb.makedsn(host, port, sid=sid)
        else:
            # 默认使用 service_name，database 字段作为 service_name
            database = self.config.get('database', '')
            if database:
                dsn = f"{host}:{port}/{database}"
            else:
                dsn = f"{host}:{port}"
        
        # 检查是否使用 Thick 模式
        thick_mode = self.config.get('thick_mode', False)
        oracle_client = self.config.get('oracle_client')
        
        if thick_mode:
            if oracle_client:
                oracledb.init_oracle_client(lib_dir=oracle_client)
            else:
                oracledb.init_oracle_client()
            self.connection = oracledb.connect(
                user=self.config.get('user'),
                password=password,
                dsn=dsn
            )
        else:
            # Thin 模式 (默认，无需 Oracle Client)
            self.connection = oracledb.connect(
                user=self.config.get('user'),
                password=password,
                dsn=dsn
            )
    
    def close(self):
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor()
        try:
            # Oracle 使用命名参数或字典
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # 获取列名 (Oracle 列名默认为大写)
            if cursor.description:
                columns = [desc[0].lower() for desc in cursor.description]
            else:
                columns = []
            
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()
    
    def get_table_count(self, table: str, schema: str = None, filter_clause: str = None) -> int:
        full_table = f"{schema}.{table}" if schema else table
        query = f'SELECT COUNT(*) as cnt FROM {full_table}'
        if filter_clause:
            query += f" WHERE {filter_clause}"
        result = self.execute_query(query)
        return result[0]['cnt'] if result else 0
    
    def get_primary_keys(self, table: str, schema: str = None) -> List[str]:
        owner = schema or self.config.get('user', '').upper()
        query = """
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
            WHERE cons.constraint_type = 'P'
                AND cons.table_name = UPPER(:table_name)
                AND cons.owner = UPPER(:owner)
            ORDER BY cols.position
        """
        results = self.execute_query(query, {'table_name': table, 'owner': owner})
        return [r['column_name'].lower() for r in results]


class DatabaseConnectionFactory:
    """数据库连接工厂"""
    
    @staticmethod
    def create(config: Dict[str, Any]) -> DatabaseConnection:
        db_type = config.get('type', '').lower()
        
        factories = {
            'mysql': MySQLConnection,
            'postgresql': PostgreSQLConnection,
            'sqlite': SQLiteConnection,
            'oracle': OracleConnection,
        }
        
        if db_type not in factories:
            raise ValueError(f"不支持的数据库类型: {db_type}")
        
        return factories[db_type](config)


class DataValidator:
    """数据校验器"""
    
    def __init__(self, mapping_config: Dict[str, Any]):
        self.config = mapping_config
        self.source_db: Optional[DatabaseConnection] = None
        self.target_db: Optional[DatabaseConnection] = None
        self.results: List[ValidationResult] = []
        
    def connect_databases(self):
        """连接数据库"""
        self.source_db = DatabaseConnectionFactory.create(self.config['source_db'])
        self.target_db = DatabaseConnectionFactory.create(self.config['target_db'])
        
        self.source_db.connect()
        self.target_db.connect()
        logger.info("数据库连接成功")
    
    def close_databases(self):
        """关闭数据库连接"""
        if self.source_db:
            self.source_db.close()
        if self.target_db:
            self.target_db.close()
    
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
    
    def build_source_query(self, mapping: TableMapping) -> str:
        """构建源表查询SQL"""
        fields = []
        for fm in mapping.field_mappings:
            if fm.source_field:
                fields.append(fm.source_field)
            elif fm.transform and fm.transform.get('fields'):
                fields.extend(fm.transform['fields'])
        
        fields = list(dict.fromkeys(fields))  # 去重保持顺序
        
        query = f"SELECT {', '.join(fields)} FROM {mapping.source_table}"
        if mapping.source_filter:
            query += f" WHERE {mapping.source_filter}"
        
        return query
    
    def build_target_query(self, mapping: TableMapping) -> str:
        """构建目标表查询SQL"""
        fields = [fm.target_field for fm in mapping.field_mappings]
        
        if mapping.target_schema:
            table = f"{mapping.target_schema}.{mapping.target_table}"
        else:
            table = mapping.target_table
        
        query = f"SELECT {', '.join(fields)} FROM {table}"
        if mapping.target_filter:
            query += f" WHERE {mapping.target_filter}"
        
        return query
    
    def transform_value(self, value: Any, transform: Dict[str, Any], row: Dict[str, Any]) -> Any:
        """应用字段转换"""
        transform_type = transform.get('type')
        
        if transform_type == 'concat':
            fields = transform.get('fields', [])
            separator = transform.get('separator', '')
            values = [str(row.get(f, '')) for f in fields]
            return separator.join(values)
        
        elif transform_type == 'constant':
            return transform.get('value')
        
        elif transform_type == 'upper':
            field = transform.get('fields', [])[0] if transform.get('fields') else None
            return str(row.get(field, '')).upper() if field else value
        
        elif transform_type == 'lower':
            field = transform.get('fields', [])[0] if transform.get('fields') else None
            return str(row.get(field, '')).lower() if field else value
        
        elif transform_type == 'trim':
            field = transform.get('fields', [])[0] if transform.get('fields') else None
            return str(row.get(field, '')).strip() if field else value
        
        elif transform_type == 'coalesce':
            fields = transform.get('fields', [])
            default = transform.get('default')
            for f in fields:
                val = row.get(f)
                if val is not None:
                    return val
            return default
        
        elif transform_type == 'date_format':
            # 简化实现，实际应使用 datetime 处理
            field = transform.get('fields', [])[0] if transform.get('fields') else None
            return row.get(field, value)
        
        elif transform_type == 'case':
            fields = transform.get('fields', [])
            source_value = row.get(fields[0]) if fields else None
            cases = transform.get('cases', [])
            for case in cases:
                if str(source_value) == case['when']:
                    return case['then']
            return transform.get('else')
        
        elif transform_type == 'json_extract':
            import json as json_module
            field = transform.get('fields', [])[0] if transform.get('fields') else None
            pattern = transform.get('pattern', '$')
            try:
                data = json_module.loads(row.get(field, '{}'))
                # 简化的 JSONPath 实现
                if pattern.startswith('$.'):
                    keys = pattern[2:].split('.')
                    for key in keys:
                        if isinstance(data, dict) and key in data:
                            data = data[key]
                        else:
                            return None
                    return data
            except:
                return None
            return row.get(field)
        
        elif transform_type == 'cast':
            field = transform.get('fields', [])[0] if transform.get('fields') else None
            value = row.get(field, value)
            cast_type = transform.get('cast_type', 'string')
            try:
                if cast_type == 'int':
                    return int(value)
                elif cast_type == 'float':
                    return float(value)
                elif cast_type == 'bool':
                    return bool(value)
                elif cast_type == 'string':
                    return str(value)
                else:
                    return value
            except:
                return value
        
        return value
    
    def compare_values(self, source_val: Any, target_val: Any, mapping: FieldMapping) -> Tuple[bool, str]:
        """比较两个值"""
        if source_val is None and target_val is None:
            return True, ""
        
        if source_val is None or target_val is None:
            return False, f"NULL mismatch: source={source_val}, target={target_val}"
        
        compare_rule = mapping.compare_rule
        
        if compare_rule == 'skip':
            return True, ""
        
        if compare_rule == 'exact':
            if str(source_val) == str(target_val):
                return True, ""
            return False, f"value mismatch: source={source_val}, target={target_val}"
        
        if compare_rule == 'ignore_case':
            if str(source_val).lower() == str(target_val).lower():
                return True, ""
            return False, f"case-insensitive mismatch: source={source_val}, target={target_val}"
        
        if compare_rule == 'ignore_whitespace':
            s_clean = str(source_val).replace(' ', '').replace('\t', '').replace('\n', '')
            t_clean = str(target_val).replace(' ', '').replace('\t', '').replace('\n', '')
            if s_clean == t_clean:
                return True, ""
            return False, f"whitespace-insensitive mismatch: source={source_val}, target={target_val}"
        
        if compare_rule == 'numeric_tolerance':
            try:
                tolerance = mapping.tolerance or 0.0
                if abs(float(source_val) - float(target_val)) <= tolerance:
                    return True, ""
                return False, f"numeric mismatch: source={source_val}, target={target_val}, tolerance={tolerance}"
            except (ValueError, TypeError):
                return False, f"numeric comparison failed: source={source_val}, target={target_val}"
        
        return True, ""
    
    def validate_table(self, table_mapping: TableMapping) -> ValidationResult:
        """校验单表数据"""
        result = ValidationResult(table_name=f"{table_mapping.source_table} -> {table_mapping.target_table}")
        start_time = datetime.now()
        
        try:
            # 获取主键字段
            pk_fields = [fm.target_field for fm in table_mapping.field_mappings if fm.is_primary_key]
            if not pk_fields:
                logger.warning(f"表 {table_mapping.source_table} 没有配置主键，使用全部字段比较")
            
            # 构建查询
            source_query = self.build_source_query(table_mapping)
            target_query = self.build_target_query(table_mapping)
            
            logger.info(f"执行源表查询: {source_query}")
            logger.info(f"执行目标表查询: {target_query}")
            
            # 获取数据
            source_data = self.source_db.execute_query(source_query)
            target_data = self.target_db.execute_query(target_query)
            
            result.total_rows = len(source_data)
            
            # 构建目标数据索引（使用主键）
            target_index = {}
            for row in target_data:
                if pk_fields:
                    key = tuple(row.get(f) for f in pk_fields)
                else:
                    key = tuple(row.values())
                target_index[key] = row
            
            # 比较数据
            source_index = {}
            for row in source_data:
                # 计算主键值
                if pk_fields:
                    # 需要先计算转换后的值
                    key_values = []
                    for fm in table_mapping.field_mappings:
                        if fm.is_primary_key:
                            if fm.source_field:
                                key_values.append(row.get(fm.source_field))
                            elif fm.transform:
                                key_values.append(self.transform_value(None, fm.transform, row))
                            else:
                                key_values.append(None)
                    key = tuple(key_values)
                else:
                    key = tuple(row.values())
                source_index[key] = row
            
            # 检查源表有但目标表没有的记录
            for key, source_row in source_index.items():
                if key not in target_index:
                    result.missing_in_target += 1
                    result.errors.append({
                        'type': 'missing_in_target',
                        'key': key,
                        'source_data': source_row
                    })
                    continue
                
                # 比较字段
                target_row = target_index[key]
                mismatched = False
                
                for fm in table_mapping.field_mappings:
                    # 获取源值
                    if fm.source_field:
                        source_val = source_row.get(fm.source_field)
                    elif fm.transform:
                        source_val = self.transform_value(None, fm.transform, source_row)
                    else:
                        source_val = None
                    
                    target_val = target_row.get(fm.target_field)
                    
                    is_match, error_msg = self.compare_values(source_val, target_val, fm)
                    if not is_match:
                        mismatched = True
                        result.errors.append({
                            'type': 'field_mismatch',
                            'key': key,
                            'field': fm.target_field,
                            'error': error_msg
                        })
                
                if mismatched:
                    result.mismatched_rows += 1
                else:
                    result.matched_rows += 1
            
            # 检查目标表有但源表没有的记录
            for key in target_index:
                if key not in source_index:
                    result.missing_in_source += 1
                    result.errors.append({
                        'type': 'missing_in_source',
                        'key': key
                    })
            
            result.status = "completed"
            
        except Exception as e:
            logger.error(f"校验表 {table_mapping.source_table} 时出错: {e}")
            result.status = "error"
            result.errors.append({
                'type': 'exception',
                'error': str(e)
            })
        
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result
    
    def run_validation(self, parallel: bool = False) -> List[ValidationResult]:
        """运行全部校验"""
        tables = self.config.get('tables', [])
        global_settings = self.config.get('global_settings', {})
        
        self.connect_databases()
        
        try:
            if parallel:
                max_workers = global_settings.get('parallel_workers', 4)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for table_config in tables:
                        mapping = self.load_table_mapping(table_config)
                        futures.append(executor.submit(self.validate_table, mapping))
                    
                    for future in as_completed(futures):
                        self.results.append(future.result())
            else:
                for table_config in tables:
                    mapping = self.load_table_mapping(table_config)
                    result = self.validate_table(mapping)
                    self.results.append(result)
                    logger.info(f"完成表 {mapping.source_table} 校验: {result.status}")
            
        finally:
            self.close_databases()
        
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
                        'errors': r.errors[:100]  # 限制错误数量
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


def load_config(config_path: str) -> Dict[str, Any]:
    """加载配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    if len(sys.argv) < 2:
        print("用法: python validator.py <config.json> [--parallel]")
        print("示例: python validator.py mapping_example.json --parallel")
        sys.exit(1)
    
    config_path = sys.argv[1]
    parallel = '--parallel' in sys.argv
    
    # 加载配置
    config = load_config(config_path)
    
    # 创建校验器
    validator = DataValidator(config)
    
    # 运行校验
    logger.info("开始数据一致性校验...")
    validator.run_validation(parallel=parallel)
    
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


if __name__ == '__main__':
    main()