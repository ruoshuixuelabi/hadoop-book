#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

# 这是「求月平均最高气温」作业的第二步：把上一步按「月日」分好的桶
# 汇总起来，对每个「站点+月日」求所有年份日最高温的平均值。
(last_key, count, sum) = (None, 0, 0)
for line in sys.stdin:
  (station, month_day, temp) = line.strip().split("\t")
  # 复合键由站点和月日拼成，与 Map 端输出的三列对应。
  key = "%s\t%s" % (station, month_day)
  if last_key and last_key != key:
    # 换组了：先把上一组的平均温输出掉，再用新记录重新起算 count 和 sum。
    # 这里的 sum / count 走的是 Python 2 的整数除法，结果会向下取整。
    print "%s\t%s" % (last_key, sum / count)
    (last_key, count, sum) = (key, 1, int(temp))
  else:
    # 同一组内：计数加一、总和加上当前温度。
    (last_key, count, sum) = (key, count + 1, sum + int(temp))

# 最后一组同样要在这里补输出。
if last_key:
  print "%s\t%s" % (last_key, sum / count)