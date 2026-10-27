#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

# 这是「二次排序」作业的 Reducer。
# 因为分区器把同一年分到同一个 reducer，而比较器又让同一年内部按气温降序排列，
# 所以到达这里的数据已经是「年份升序、同年气温降序」，每个年份的第一行就是该年的最高温。
last_group = None
for line in sys.stdin:
  val = line.strip()
  (year, temp) = val.split("\t")
  # 这里的「组」就是年份；把整个年份的数据看作一组。
  group = year
  if last_group != group:
    # 新年份的第一条记录 —— 也就是该年的最高温 —— 被输出来。
    print val
    last_group = group
