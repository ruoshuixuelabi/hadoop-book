-- max_temp_filter_udf.pig
-- 这个脚本演示用 Java 写的 UDF 过滤数据。
REGISTER pig-examples.jar;
-- 先把写着 UDF 的 jar 包注册进来。
DEFINE isGood com.hadoopbook.pig.IsGoodQuality();
-- 这一句 DEFINE 给这个类起一个短名字，写在 FILTER 里就好用。
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);
filtered_records = FILTER records BY temperature != 9999 AND isGood(quality);
-- 过滤条件由两部分组成：先排掉缺测温度，再让 UDF 判断质量码。
grouped_records = GROUP filtered_records BY year;
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
DUMP max_temp;
-- 小结：Java 写的 UDF 比流式 UDF 快，但必须先编译成 jar 包。
