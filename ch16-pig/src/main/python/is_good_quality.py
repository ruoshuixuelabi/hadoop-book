#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 这个脚本是一个 Pig 流式 UDF 的实现体。
# Pig 把每一行输入交给它处理，它再把处理后的行输出出去。
# 过滤规则是：温度不是缺测值 9999，并且质量码属于 0、1、4、5、9 这几种好天气。

import re
import sys

for line in sys.stdin:
  (year, temp, q) = line.strip().split()
  # 按空白把这一行拆成三段：年份、温度、质量码。
  # 温度正常而且质量码可接受时才输出，输出的仍然是制表符分隔的两列。
  if (temp != "9999" and re.match("[01459]", q)):
    print "%s\t%s" % (year, temp)
