"""CLI 入口点 - 支持 python -m validator 运行"""

import json
import logging
import sys

from . import _SPARK_CONNECT_URL, SparkSession
from .core import SparkDataValidator

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """加载配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    if len(sys.argv) < 2:
        print("用法: python -m validator <config.json> [--master <spark-master>]")
        print("示例: python -m validator mapping_example.json --master spark://localhost:7077")
        sys.exit(1)

    config_path = sys.argv[1]
    master = None

    if '--master' in sys.argv:
        master_idx = sys.argv.index('--master')
        if master_idx + 1 < len(sys.argv):
            master = sys.argv[master_idx + 1]

    # 加载配置
    config = load_config(config_path)

    # 创建 Spark 会话（Spark Connect 优先）
    if _SPARK_CONNECT_URL:
        logger.info(f"使用 Spark Connect 模式: {_SPARK_CONNECT_URL}")
        if master:
            logger.warning("--master 参数在 Spark Connect 模式下被忽略，请通过 SPARK_CONNECT_URL 指定连接地址")
        spark = SparkSession.builder.remote(_SPARK_CONNECT_URL).getOrCreate()
    else:
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
