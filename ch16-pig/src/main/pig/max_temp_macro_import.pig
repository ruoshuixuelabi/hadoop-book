-- 这个脚本演示用 IMPORT 把宏文件引进当前脚本。
IMPORT './ch16-pig/src/main/pig/max_temp.macro';
-- 引进之后就可以直接调用文件里定义的 max_by_group，
-- 不需要把宏的代码再抄一遍。
records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);
filtered_records = FILTER records BY temperature != 9999 AND
  quality IN (0, 1, 4, 5, 9);
max_temp = max_by_group(filtered_records, year, temperature);
-- 调用方式和在同一个脚本里定义宏时完全一样。
DUMP max_temp
-- 小结：宏放在独立文件里更适合多个脚本共用同一段查询逻辑。