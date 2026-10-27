#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sys

# 逐行读取标准输入，这就是 Map 阶段收到的输入：一行一条原始记录。
for line in sys.stdin:
  val = line.strip()
  # 定长文本格式的 NCDC 记录：年份在第 16 至 19 列，气温在第 88 至 92 列，质量位在第 93 列。
  # 切片是 0 起始的，所以起点各比列号小 1。
  (year, temp, q) = (val[15:19], val[87:92], val[92:93])
  # 加号加四个 9 表示缺测；质量位只认 0、1、4、5、9 这几种有效读数
  if (temp != "+9999" and re.match("[01459]", q)):
    # 输出「年份\t气温」，正是 Reduce 阶段用来分组的键值对
    print "%s\t%s" % (year, temp)
