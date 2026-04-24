-- max_temp_filter_stream.pig: 使用Python流式处理过滤数据,计算每年最高温度
-- 步骤1: 定义Python过滤脚本,使用SHIP将脚本分发到集群各节点
DEFINE is_good_quality `is_good_quality.py`
  SHIP ('ch16-pig/src/main/python/is_good_quality.py');

-- 步骤2: 加载原始气象数据
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);

-- 步骤3: 使用STREAM通过Python脚本过滤记录,只保留质量合格的记录
filtered_records = STREAM records THROUGH is_good_quality
  AS (year:chararray, temperature:int);

-- 步骤4: 按年份分组
grouped_records = GROUP filtered_records BY year;

-- 步骤5: 计算每组的最高温度并输出
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
DUMP max_temp;
