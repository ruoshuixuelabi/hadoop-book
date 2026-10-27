-- 这个脚本演示宏（macro）：把一段常用的多步查询封装成一个可以复用的操作。
DEFINE max_by_group(X, group_key, max_field) RETURNS Y {
  A = GROUP $X by $group_key;
  $Y = FOREACH A GENERATE group, MAX($X.$max_field);
  -- 宏里的 $X、$group_key、$max_field 都是调用时传进来的参数，
  -- $Y 是宏返回的关系名，同样由调用方指定。
};

records = LOAD 'input/ncdc/micro-tab/sample.txt'
  AS (year:chararray, temperature:int, quality:int);
filtered_records = FILTER records BY temperature != 9999 AND
  quality IN (0, 1, 4, 5, 9);
max_temp = max_by_group(filtered_records, year, temperature);
-- 这一行就相当于把宏里的 GROUP 和 FOREACH 两步展开执行。
DUMP max_temp
-- 小结：宏适合封装「分组再聚合」这类固定套路。