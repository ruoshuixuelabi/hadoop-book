#!/usr/bin/env bash
# 用 awk 直接扫描 NCDC 原始记录、算出各年的最高气温，全程不经过 Hadoop。
# 这是进入 MapReduce 之前，先看清「按年分组求最大值」这件事本身的最直观版本。
for year in all/*
do
  # 依次处理 all/ 目录下各年份的压缩文件
  echo -ne `basename $year .gz`"\t"
  # 解压后交给 awk：一次扫描全文件，取出气温与质量位，只在有效读数里留最大值
  gunzip -c $year | \
    awk '{ temp = substr($0, 88, 5) + 0;
           q = substr($0, 93, 1);
           if (temp !=9999 && q ~ /[01459]/ && temp > max) max = temp }
         END { print max }'
done