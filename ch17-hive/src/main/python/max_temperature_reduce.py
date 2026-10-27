#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 这个脚本是 Hive 的一个外部 Reduce 脚本。
# 它假定输入已经按年份排好序：年份相同的行是连在一起的。
# 脚本逐行处理，记下当前年份和到目前为止的最高温度，
# 一旦年份变了，就把上一年的结果输出出去。

import sys

(last_key, max_val) = (None, 0)
# 记录上一个年份和它的最高温度，还没读到数据时都为空。
for line in sys.stdin:
  (key, val) = line.strip().split("\t")
  # 输入是「年份和温度」两列，中间用制表符隔开。
  if last_key and last_key != key:
    print "%s\t%s" % (last_key, max_val)
    # 年份变了，说明上一年的记录已经读完，把结果输出出去。
    (last_key, max_val) = (key, int(val))
  else:
    (last_key, max_val) = (key, max(max_val, int(val)))
    # 还是同一个年份，只保留更高的那个温度。

if last_key:
  # 最后一行还没来得及输出，在这里补上。
  print "%s\t%s" % (last_key, max_val)