#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
文件：MaxTemperature.py<br>
路径：src/main/python/MaxTemperature.py<br>
描述：使用PySpark计算最大气温程序<br>
<br>
功能说明：<br>
使用Python版Spark（PySpark）实现分布式最大气温计算。<br>
从输入文件中读取NCDC气象数据，过滤无效记录，<br>
按年份分组计算每年的最高气温，并将结果保存到输出目录。<br>
<br>
数据格式：<br>
输入数据为制表符分隔的文本文件，格式为：<br>
气象站ID\t年份\t气温\t质量标识<br>
示例输入：<br>
1949\t78\t01459<br>
1950\t-11\t01459<br>
<br>
算法流程：<br>
1. textFile(): 读取输入文件创建RDD<br>
2. map(): 将每行按制表符分割为列表<br>
3. filter(): 过滤无效记录（气温为9999或质量标识不符合）<br>
4. map(): 转换为(年份,气温)元组<br>
5. reduceByKey(): 按年份分组求最大气温<br>
6. saveAsTextFile(): 保存结果到输出目录<br>
<br>
运行命令：<br>
spark-submit MaxTemperature.py &lt;输入路径&gt; &lt;输出路径&gt;<br>
"""

# 导入SparkContext：PySpark的入口点
from pyspark import SparkContext

# 导入正则表达式模块（用于匹配质量标识）和系统模块（用于获取命令行参数）
import re, sys

# 创建SparkContext
# "local": 使用本地模式运行（单进程）
# "Max Temperature": 应用程序名称
sc = SparkContext("local", "Max Temperature")

# 从输入路径读取文本文件，每行作为一个元素
# 使用链式调用实现数据处理管道
sc.textFile(sys.argv[1]) \
    # map: 将每行按制表符分割为字符串列表
    # lambda s: s.split("\t") 匿名函数，输入字符串s，输出分割后的列表
    .map(lambda s: s.split("\t")) \
    # filter: 过滤无效记录
    # lambda rec: 匿名函数，rec是分割后的列表
    # rec[1] != "9999": 气温字段，9999表示缺失值
    # re.match("[01459]", rec[2]): 质量标识必须匹配正则表达式[01459]
    .filter(lambda rec: (rec[1] != "9999" and re.match("[01459]", rec[2]))) \
    # map: 将记录转换为(年份, 气温)的元组
    # int()将字符串转换为整数
    .map(lambda rec: (int(rec[0]), int(rec[1]))) \
    # reduceByKey: 按年份分组，聚合时取最大气温
    # max是Python内置函数，返回两个数中的最大值
    .reduceByKey(max) \
    # saveAsTextFile: 将结果保存为文本文件到指定输出目录
    # 每个RDD元素占一行，格式为"(年份, 最高气温)"
    .saveAsTextFile(sys.argv[2])
