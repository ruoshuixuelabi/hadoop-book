-- max_temp_filter_stream.pig
-- 这个脚本演示用流式 UDF 过滤数据：
-- Python 脚本作为外部进程运行，Pig 把每一行交给它处理。
DEFINE is_good_quality `is_good_quality.py`
  SHIP ('ch16-pig/src/main/python/is_good_quality.py');
-- 这一句 SHIP 告诉 Pig 把这个脚本一起分发到各个节点上。
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);
filtered_records = STREAM records THROUGH is_good_quality
  AS (year:chararray, temperature:int);
-- 这一句 STREAM 之后的 AS 声明的是脚本输出那几列的类型和顺序。
grouped_records = GROUP filtered_records BY year;
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
DUMP max_temp;
-- 剩下的分组和求最大值的步骤与前面的脚本完全一样。
