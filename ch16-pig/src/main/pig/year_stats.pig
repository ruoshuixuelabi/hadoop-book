-- year_stats.pig
-- 这个脚本演示嵌套的 FOREACH：在一个分组内部再做多步计算。
REGISTER pig-examples.jar;
DEFINE isGood com.hadoopbook.pig.IsGoodQuality();
-- 还是用之前那个判断质量码的 UDF。
records = LOAD 'input/ncdc/all/19{1,2,3,4,5}0*'
  USING com.hadoopbook.pig.CutLoadFunc('5-10,11-15,16-19,88-92,93-93')
  AS (usaf:chararray, wban:chararray, year:int, temperature:int, quality:int);
-- 花括号里的通配表示读入 1910 到 1950 这几十年的观测数据。
  
grouped_records = GROUP records BY year PARALLEL 30;
-- 按年份分组。

year_stats = FOREACH grouped_records {
  uniq_stations = DISTINCT records.usaf;
  -- 组内先去重，得到这一年出现过的站点编号集合。
  good_records = FILTER records BY isGood(quality);
  -- 再筛出质量码合格的记录。
  GENERATE FLATTEN(group), COUNT(uniq_stations) AS station_count,
    COUNT(good_records) AS good_record_count, COUNT(records) AS record_count;
  -- 最后生成一行统计：站点数、好天气记录数和总记录数。
}

DUMP year_stats;

-- 小结：嵌套 FOREACH 里的中间变量按顺序定义，后面的步骤能引用前面的结果。
