# Spark Connect 模式说明

## 概述

Spark Connect 是 Apache Spark 3.4+ 引入的客户端-服务端分离架构。校验工具通过设置环境变量 `SPARK_CONNECT_URL` 来启用该模式，无需修改任何配置文件。

**适用场景：**
- 已有独立运行的 Spark Connect Server，无需在本机安装完整 Spark
- 多个客户端共享同一 Spark 集群资源
- 在容器或 CI 环境中以轻量客户端模式运行校验任务

---

## 前置条件

| 要求 | 说明 |
|------|------|
| PySpark 版本 | >= 3.4.0 |
| Spark Connect Server | 需独立启动，默认端口 `15002` |
| Python 依赖 | `pyspark>=3.4.0`（已含客户端库） |

---

## 启动 Spark Connect Server

在 Spark 集群节点上执行：

```bash
# 使用 Spark 自带脚本启动（默认监听 0.0.0.0:15002）
$SPARK_HOME/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0

# 指定端口
$SPARK_HOME/sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.12:3.4.0 \
  --conf spark.connect.grpc.binding.port=15002

# YARN 模式
$SPARK_HOME/sbin/start-connect-server.sh \
  --master yarn \
  --packages org.apache.spark:spark-connect_2.12:3.4.0
```

停止服务：

```bash
$SPARK_HOME/sbin/stop-connect-server.sh
```

---

## 使用方式

### 基本用法

```bash
# 设置环境变量后直接运行，与普通模式命令完全相同
export SPARK_CONNECT_URL=sc://your-spark-host:15002
python -m validator mapping_example.json
```

### 一行命令

```bash
SPARK_CONNECT_URL=sc://your-spark-host:15002 python -m validator mapping_example.json
```

### 注意事项

- Spark Connect 模式下 `--master` 参数**被忽略**，连接地址由 `SPARK_CONNECT_URL` 决定；若同时传入，工具会打印警告。
- 不设置 `SPARK_CONNECT_URL` 时，工具回退到普通模式，行为与之前完全一致。

---

## URL 格式

```
sc://<host>:<port>[/<path>][?<key>=<value>[&<key>=<value>...]]
```

| 示例 | 说明 |
|------|------|
| `sc://localhost:15002` | 本机默认端口 |
| `sc://spark-server.internal:15002` | 内网主机 |
| `sc://spark-server:15002/;token=my-token` | 带认证 token |

---

## 与普通模式对比

| 特性 | 普通模式 | Spark Connect 模式 |
|------|----------|--------------------|
| 启动方式 | `--master` 或本地 | `SPARK_CONNECT_URL` 环境变量 |
| Spark 进程 | 本地 driver | 远程 Server |
| 退出时 stop | 自动停止本地 Session | **不停止**远程 Server |
| JDBC 读取 | 直接 | 委托给 Server 执行 |
| 多表 JOIN | 支持 | 支持 |
| Debug 导出 | 支持 | 支持 |

---

## 常见问题

**连接失败 `UNAVAILABLE: Connection refused`**

检查 Server 是否正常运行及防火墙端口（默认 15002）是否开放。

**`ModuleNotFoundError: pyspark.sql.connect`**

PySpark 版本低于 3.4.0，升级即可：
```bash
pip install "pyspark>=3.4.0"
```

**JDBC 驱动缺失**

Spark Connect Server 需要能访问数据库 JDBC 驱动。在启动 Server 时通过 `--jars` 或 `--packages` 添加驱动，而非客户端本地。

```bash
$SPARK_HOME/sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.12:3.4.0 \
  --jars /path/to/mysql-connector-j-8.x.x.jar,/path/to/postgresql-42.x.x.jar
```
