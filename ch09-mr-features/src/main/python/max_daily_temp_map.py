#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sys

# 逐行读取标准输入；Map 阶段拿到的每一行就是一条 NCDC 原始记录。
for line in sys.stdin:
  val = line.strip()
  # 定长文本格式的 NCDC 记录，按列号切片就能取出需要的字段：
  # 第 5-10 列是 USAF 站号，第 11-15 列是 WBAN 站号，
  # 第 16-23 列是日期（形如 YYYYMMDD），第 88-92 列是气温，第 93 列是质量标记。
  (usaf, wban, date, temp, q) = (val[4:10], val[10:15], val[15:23],
                                 int(val[87:92]), val[92:93])
  # 气温等于 9999 表示这条记录缺测，直接丢弃；
  # 质量标记只认 0/1/4/5/9 这几种「读数可信」的情况，其余一律不要。
  if (temp != 9999 and re.match("[01459]", q)):
    # 输出三列：站点号、日期、气温。
    # 站点号由 USAF 与 WBAN 用短横线拼成，两者合起来才能唯一确定一个气象站；
    # 这样同一站同一天的记录会带着相同的键进到同一个 Reducer，由它挑出当天的最高温。
    print "%s-%s\t%s\t%s" % (usaf, wban, date, temp)