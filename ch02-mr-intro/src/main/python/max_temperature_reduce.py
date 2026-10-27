#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

# 归约阶段：输入已经按年份排好序，同一年的记录是连续的。
# 因此只要扫一遍，边读边滚动维护当前年份的最大值即可，不必一次装进内存。
(last_key, max_val) = (None, -sys.maxint)
for line in sys.stdin:
  (key, val) = line.strip().split("\t")
  # 遇到新年份，说明上一年的数据已经读完，可以结算并输出了
  if last_key and last_key != key:
    print "%s\t%s" % (last_key, max_val)
    (last_key, max_val) = (key, int(val))
  else:
    # 还是同一年，继续滚动维护最大值
    (last_key, max_val) = (key, max(max_val, int(val)))

# 循环结束后别忘了输出最后一年
if last_key:
  print "%s\t%s" % (last_key, max_val)