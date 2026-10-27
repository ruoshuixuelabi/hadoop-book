-- max_temp.pig: Finds the maximum temperature by year
-- 这个脚本读入逐年的气温数据，过滤掉温度为缺测值的记录，
-- 按年份分组，再算出每一年的最高气温。
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);
filtered_records = FILTER records BY temperature != 9999 AND
  quality IN (0, 1, 4, 5, 9);
-- 温度 9999 是缺测值，质量码只保留 0、1、4、5、9 这几种好天气。
grouped_records = GROUP filtered_records BY year;
-- 这一步 GROUP 把每一年的记录放进一个组。
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
-- 这一步 MAX 在每个组里挑出最大的温度，group 是分组用的键。
DUMP max_temp;
-- 最后把结果打印出来。
