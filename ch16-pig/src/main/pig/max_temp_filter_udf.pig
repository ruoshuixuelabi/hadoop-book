-- max_temp_filter_udf.pig: 使用Java UDF过滤数据,计算每年最高温度
-- 步骤1: 注册包含UDF的JAR包,定义UDF实例
REGISTER pig-examples.jar;
DEFINE isGood com.hadoopbook.pig.IsGoodQuality();

-- 步骤2: 加载原始气象数据
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);

-- 步骤3: 过滤无效数据
-- 条件: 温度不为9999 且 质量码通过isGood检查
filtered_records = FILTER records BY temperature != 9999 AND isGood(quality);

-- 步骤4: 按年份分组
grouped_records = GROUP filtered_records BY year;

-- 步骤5: 计算每组最高温度并输出
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
DUMP max_temp;
