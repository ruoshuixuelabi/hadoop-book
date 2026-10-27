-- max_temp_station_name.pig
-- 这个脚本演示把「按站点求最高气温」的结果再和站点名称关联起来。
REGISTER pig-examples.jar;
DEFINE isGood com.hadoopbook.pig.IsGoodQuality();

stations = LOAD 'input/ncdc/metadata/stations-fixed-width.txt'
  USING com.hadoopbook.pig.CutLoadFunc('1-6,8-12,14-42')
  AS (usaf:chararray, wban:chararray, name:chararray);
-- 站点元数据是定宽文本，用 CutLoadFunc 按列的起止位置切出来。
  
trimmed_stations = FOREACH stations GENERATE usaf, wban, TRIM(name);
-- 站点名里有多余的空格，用 TRIM 去掉，方便后面做连接。

records = LOAD 'input/ncdc/all/191*'
  USING com.hadoopbook.pig.CutLoadFunc('5-10,11-15,88-92,93-93')
  AS (usaf:chararray, wban:chararray, temperature:int, quality:int);
-- 观测数据也是定宽格式，只是各列的位置和元数据文件不一样。
  
filtered_records = FILTER records BY temperature != 9999 AND isGood(quality);
-- 同样先过滤掉坏数据。
grouped_records = GROUP filtered_records BY (usaf, wban) PARALLEL 30;
-- 这里按站点编号分组，两个字段一起做分组键。
max_temp = FOREACH grouped_records GENERATE FLATTEN(group),
  MAX(filtered_records.temperature);
-- 求每个站点的最高气温。
max_temp_named = JOIN max_temp BY (usaf, wban), trimmed_stations BY (usaf, wban)
  PARALLEL 30;
max_temp_result = FOREACH max_temp_named GENERATE $0, $1, $5, $2;
-- 关联之后字段变多了，用位置引用挑出想要的四列重新排一下。

STORE max_temp_result INTO 'max_temp_by_station';
-- 小结：先算出聚合结果，再和维表关联，是很常见的数据处理套路。