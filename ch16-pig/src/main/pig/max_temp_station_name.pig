-- max_temp_station_name.pig: 计算各气象站的最高温度,并关联站名
-- 步骤1: 注册JAR包并定义UDF
REGISTER pig-examples.jar;
DEFINE isGood com.hadoopbook.pig.IsGoodQuality();

-- 步骤2: 加载气象站元数据(USAF和WBAN编码,站名)
stations = LOAD 'input/ncdc/metadata/stations-fixed-width.txt'
  USING com.hadoopbook.pig.CutLoadFunc('1-6,8-12,14-42')
  AS (usaf:chararray, wban:chararray, name:chararray);

-- 步骤3: 去除站名两端空白
trimmed_stations = FOREACH stations GENERATE usaf, wban, TRIM(name);

-- 步骤4: 加载1910年代的温度记录
records = LOAD 'input/ncdc/all/191*'
  USING com.hadoopbook.pig.CutLoadFunc('5-10,11-15,88-92,93-93')
  AS (usaf:chararray, wban:chararray, temperature:int, quality:int);

-- 步骤5: 过滤无效数据
filtered_records = FILTER records BY temperature != 9999 AND isGood(quality);

-- 步骤6: 按气象站(usaf, wban)分组,计算各站最高温度
grouped_records = GROUP filtered_records BY (usaf, wban) PARALLEL 30;
max_temp = FOREACH grouped_records GENERATE FLATTEN(group),
  MAX(filtered_records.temperature);

-- 步骤7: 将最高温度数据与气象站名称关联
max_temp_named = JOIN max_temp BY (usaf, wban), trimmed_stations BY (usaf, wban)
  PARALLEL 30;

-- 步骤8: 整理输出格式(站号,WBAN,站名,最高温度)并存储
max_temp_result = FOREACH max_temp_named GENERATE $0, $1, $5, $2;

STORE max_temp_result INTO 'max_temp_by_station';
