-- max_temp_macro_import.pig: 导入并使用宏计算每年最高温度
-- 步骤1: 从外部文件导入宏定义
IMPORT './ch16-pig/src/main/pig/max_temp.macro';

-- 步骤2: 加载NCDC气象数据
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);

-- 步骤3: 过滤无效数据(温度9999为无效,质量码需为良好值)
filtered_records = FILTER records BY temperature != 9999 AND
  quality IN (0, 1, 4, 5, 9);

-- 步骤4: 使用导入的宏按年份计算最高温度
max_temp = max_by_group(filtered_records, year, temperature);

-- 步骤5: 输出结果
DUMP max_temp
