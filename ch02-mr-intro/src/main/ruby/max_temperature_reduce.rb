#!/usr/bin/env ruby

# 归约阶段：输入已经按年份排好序，同一年的记录是连续的，
# 因此只要扫一遍，边读边滚动维护当前年份的最大值即可。
last_key, max_val = nil, -1000000
STDIN.each_line do |line|
  key, val = line.split("\t")
  # 遇到新年份，说明上一年的数据已经读完，可以结算并输出了
  if last_key && last_key != key
    puts "#{last_key}\t#{max_val}"
    last_key, max_val = key, val.to_i
  else
    # 还是同一年，继续滚动维护最大值
    last_key, max_val = key, [max_val, val.to_i].max
  end
end
# 循环结束后别忘了输出最后一年
puts "#{last_key}\t#{max_val}" if last_key