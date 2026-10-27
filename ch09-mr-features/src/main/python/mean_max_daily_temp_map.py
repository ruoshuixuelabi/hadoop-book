#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

# Change date to month and day
# 这是「求月平均最高气温」作业的第一步：把上一步算出的日最高温
# 按「月+日」重新分桶，让同一年的同一个月日的数据落到同一个键上。
for line in sys.stdin:
  (station, date, temp) = line.strip().split("\t")
  # 日期字段是 YYYYMMDD 形式的 8 位串，date[4:8] 截出的是 MMDD，
  # 也就是「第几月第几日」——同一年的同一个月日对应的就是同一天。
  # 注意这里保留月日而丢掉年份，是为了让不同年份的同一天能聚到一起求平均。
  print "%s\t%s\t%s" % (station, date[4:8], temp) 