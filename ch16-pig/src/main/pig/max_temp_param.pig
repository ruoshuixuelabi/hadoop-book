-- max_temp_param.pig
-- 这个脚本演示用参数文件给脚本里的变量赋值。
records = LOAD '$input' AS (year:chararray, temperature:int, quality:int);
-- 这里的 $input 是参数文件里的键，运行前用 -param_file 指定参数文件即可。
filtered_records = FILTER records BY temperature != 9999 AND
  quality IN (0, 1, 4, 5, 9);
grouped_records = GROUP filtered_records BY year;
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
STORE max_temp into '$output';
-- 这里的 $output 同理，是参数文件里定义的输出路径。
-- 小结：脚本本身不写死路径，换一份参数文件就能在别的环境上跑。
