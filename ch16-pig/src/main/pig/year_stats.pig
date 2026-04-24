-- year_stats.pig: 统计每年气象站数量、好记录数和总记录数
-- 步骤1: 注册JAR包并定义UDF
REGISTER pig-examples.jar;
DEFINE isGood com.hadoopbook.pig.IsGoodQuality();

-- 步骤2: 使用自定义加载函数读取1910-1950年的数据
-- 字段: USAF站码,WBAN站码,年份,温度,质量码
records = LOAD 'input/ncdc/all/19{1,2,3,4,5}0*'
  USING com.hadoopbook.pig.CutLoadFunc('5-10,11-15,16-19,88-92,93-93')
  AS (usaf:chararray, wban:chararray, year:int, temperature:int, quality:int);

-- 步骤3: 按年份分组,使用30个并行reducer
grouped_records = GROUP records BY year PARALLEL 30;

-- 步骤4: 对每年进行嵌套运算
year_stats = FOREACH grouped_records {
  -- 计算该年的唯一气象站数
  uniq_stations = DISTINCT records.usaf;
  -- 筛选质量合格的记录
  good_records = FILTER records BY isGood(quality);
  -- 生成统计: 年份, 气象站数, 好记录数, 总记录数
  GENERATE FLATTEN(group), COUNT(uniq_stations) AS station_count,
    COUNT(good_records) AS good_record_count, COUNT(records) AS record_count;
}

-- 步骤5: 输出统计结果
DUMP year_stats;

