-- max_temp_macro.pig: 定义计算分组最大值的宏
-- 宏定义: max_by_group(X, group_key, max_field)
-- 参数说明:
--   X: 输入关系
--   group_key: 分组字段名
--   max_field: 求最大值的字段名
-- 返回值: Y - 包含分组键和最大值的关系
DEFINE max_by_group(X, group_key, max_field) RETURNS Y {
  -- 按指定字段分组
  A = GROUP $X by $group_key;
  -- 生成结果: 分组键 + 最大值
  $Y = FOREACH A GENERATE group, MAX($X.$max_field);
};
