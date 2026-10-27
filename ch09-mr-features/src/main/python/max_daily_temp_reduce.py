#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

# 这是求「每个站每一天的最高气温」的 Reducer。
# 这一步的键是「站点+日期」，Hadoop 会把同一个键的记录集中到同一个 Reducer，
# 且默认按键排序，因此这里只需要边扫边比较就能求出每组的最大值。
(last_key, max_val) = (None, 0)
for line in sys.stdin:
  (station, date, temp) = line.strip().split("\t")
  # 用制表符把站点和日期拼成一个复合键，和 Map 端输出的三列一一对应。
  key = "%s\t%s" % (station, date)
  if last_key and last_key != key:
    # 键变了，说明上一组的数据已经读完，可以把上一组的最高温输出掉。
    print "%s\t%s" % (last_key, max_val)
    (last_key, max_val) = (key, int(temp))
  else:
    # 键没变，还在同一组里，继续把当前最大值抬高。
    (last_key, max_val) = (key, max(max_val, int(temp)))

# 循环结束后最后一组还没被输出，这里补上，否则会丢掉最后一组的结果。
if last_key:
  print "%s\t%s" % (last_key, max_val)