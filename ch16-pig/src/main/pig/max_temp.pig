-- max_temp.pig: 计算每年的最高温度
-- 步骤1: 加载NCDC气象数据,包含年份、温度和质量码
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);

-- 步骤2: 过滤无效数据
-- 过滤条件: 温度不为9999(无效值), 质量码为良好值(0,1,4,5,9)
filtered_records = FILTER records BY temperature != 9999 AND
  quality IN (0, 1, 4, 5, 9);

-- 步骤3: 按年份分组
grouped_records = GROUP filtered_records BY year;

-- 步骤4: 计算每组的最高温度
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);

-- 步骤5: 输出结果
DUMP max_temp;
