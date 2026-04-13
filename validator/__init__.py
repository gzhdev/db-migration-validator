"""
数据库迁移数据一致性校验工具 - Spark 版本
Database Migration Data Consistency Validator - Spark Version

使用 PySpark 进行分布式计算，支持大数据量的迁移校验。
"""

import logging
import os

# Spark Connect 模式：设置环境变量 SPARK_CONNECT_URL 启用（如 sc://localhost:15002）
_SPARK_CONNECT_URL = os.environ.get("SPARK_CONNECT_URL", "").strip()

if _SPARK_CONNECT_URL:
    # Spark Connect 模式：必须使用 connect 专属的 functions 模块
    # 经典 pyspark.sql.functions 内部检查 SparkContext._active_spark_context，
    # 而 Spark Connect 无本地 SparkContext，调用 lit/when/col 等会抛 AssertionError
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect import functions as F
    from pyspark.sql.connect.functions import (
        col, when, lit, concat_ws, upper, lower, trim, substring,
        regexp_replace, coalesce, to_json, from_json, expr,
    )
    from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
else:
    from pyspark.sql import SparkSession, DataFrame, functions as F
    from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
    from pyspark.sql.functions import (
        col, when, lit, concat_ws, upper, lower, trim, substring,
        regexp_replace, coalesce, to_json, from_json, expr,
    )

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
