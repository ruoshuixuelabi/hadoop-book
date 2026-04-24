# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是《Hadoop: The Definitive Guide, Fourth Edition》的示例代码仓库，一个 Maven 多模块项目。每个章节（ch02-mr-intro、ch03-hdfs 等）都是独立的模块。

## 构建命令

```bash
# 全量构建（跳过测试）
mvn package -DskipTests

# 构建单个模块
mvn package -DskipTests -pl ch05-io -am

# 运行单个测试
mvn test -pl ch05-io -Dtest=IntPairTest

# 运行模块内所有测试
mvn test -pl ch05-io
```

## 模块结构

- `book/pom.xml` - 父 POM，定义所有组件版本（Hadoop 3.4.2, HBase 2.6.4, Hive 4.2.0, Pig 0.18.0 等）
- `hadoop-meta/pom.xml` - 依赖管理模块
- `common/` - 共享工具类（如 NcdcRecordParser）
- `ch02-mr-intro/` - MapReduce 入门示例
- `ch03-hdfs/` - HDFS 操作示例
- `ch05-io/` - IO 格式（Writable、SequenceFile、MapFile 等）
- `ch06-mr-dev/` - MapReduce 开发（v1/v2/v3/v4 API 示例）
- `ch08-mr-types/` - MR 类型与多种输入/输出格式
- `ch09-mr-features/` - MR 特性（计数器、排序、连接等）
- `ch12-avro/` - Avro 数据序列化
- `ch13-parquet/` - Parquet 列式存储
- `ch16-pig/` - Pig 脚本
- `ch17-hive/` - Hive 查询
- `ch18-crunch/` - Crunch 库（类似 Pig 的 Pipeline API）
- `ch19-spark/` - Spark 示例
- `ch20-hbase/` - HBase 操作
- `ch21-zk/` - ZooKeeper 示例
- `hadoop-examples/` - 聚合模块，打包所有示例到一个 JAR

## 样本数据

`input/` 目录包含样本数据集。完整天气数据需从 http://www.hadoopbook.com 获取。

## 关键依赖版本

从 `book/pom.xml` 查看：
- Hadoop: 3.4.2
- HBase: 2.6.4
- Hive: 4.2.0
- Pig: 0.18.0
- Avro: 1.12.1
- Parquet: 1.16.0
- Zookeeper: 3.9.4