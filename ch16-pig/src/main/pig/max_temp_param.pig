-- max_temp_param.pig: 参数化脚本,根据输入输出路径计算每年最高温度
-- 参数:
--   $input: 输入数据路径
--   $output: 输出结果路径
-- 使用方式: pig -param input=xxx -param output=yyy max_temp_param.pig

-- 步骤1: 从参数指定的路径加载数据
records = LOAD '$input' AS (year:chararray, temperature:int, quality:int);

-- 步骤2: 过滤无效数据
filtered_records = FILTER records BY temperature != 9999 AND
  quality IN (0, 1, 4, 5, 9);

-- 步骤3: 按年份分组
grouped_records = GROUP filtered_records BY year;

-- 步骤4: 计算每组最高温度
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);

-- 步骤5: 将结果存储到参数指定的输出路径
STORE max_temp into '$output';
