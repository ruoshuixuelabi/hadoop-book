#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sys

# 这是「二次排序」作业的 Mapper：把每条 NCDC 记录变成「年份 + 气温」一对，
# 并顺手用计数器统计出被丢弃的记录数。
for line in sys.stdin:
  val = line.strip()
  # 第 16-19 列是年份，第 88-92 列是气温，第 93 列是质量标记。
  (year, temp, q) = (val[15:19], int(val[87:92]), val[92:93])
  if temp == 9999:
    # 气温缺失时不能参与排序比较，但这条记录本身是有价值的诊断信息，
    # 所以往 stderr 写一行 reporter:counter，让 Hadoop 把它记进作业计数器。
    # 计数器会在作业 UI 的 Counter 页签里显示，方便确认丢了多少数据。
    sys.stderr.write("reporter:counter:Temperature,Missing,1\n")
  elif re.match("[01459]", q):
    # 质量标记合格的记录才输出。
    # 键是年份、值是气温——但排序要按「年份升序、同年气温降序」来排，
    # 这个「同键内二次排序」需要靠 reducer 端的自定义比较器来实现。
    print "%s\t%s" % (year, temp)
