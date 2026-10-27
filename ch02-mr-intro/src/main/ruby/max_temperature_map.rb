#!/usr/bin/env ruby

# 逐行读取标准输入，这就是 Map 阶段收到的输入：一行一条原始记录。
STDIN.each_line do |line|
  val = line
  # 定长文本格式的 NCDC 记录：年份在第 16 至 19 列，气温在第 88 至 92 列，质量位在第 93 列。
  # 字符串切片是 0 起始的，所以起点各比列号小 1。
  year, temp, q = val[15,4], val[87,5], val[92,1]
  # 加号加四个 9 表示缺测；质量位只认 0、1、4、5、9 这几种有效读数
  puts "#{year}\t#{temp}" if (temp != "+9999" && q =~ /[01459]/)
end