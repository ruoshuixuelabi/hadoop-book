# 求「每个站每一天的最高气温」以及「每个站每个月的平均最高气温」。
# 这是一个两阶段的作业：第一阶段求出日最高温，第二阶段把日最高温按月日分桶再求平均。
# 下面这些命令的参数都是用反斜杠续行连成一行的，
# 所以解释各参数的注释只能写在命令的前面，不能插在续行之间。
# 把 Streaming 启动命令存进变量 STREAM，两个阶段共用，后面就不用重复写 jar 路径。
# 配置里的 -conf 指定用哪份 Hadoop 配置；这里用本机的伪分布式配置。
STREAM="hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar -conf conf/hadoop-localhost.xml"

# 第一阶段：求每个站每一天的最高气温。
# 指定 Map 输出有两个键字段；把两个 python 脚本分发到计算节点；
# 输入是全部 NCDC 记录，输出目录必须事先不存在；
# 最后指定用哪两个脚本分别当 Mapper 和 Reducer。
$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -files ch09-mr-features/src/main/python/max_daily_temp_map.py,\
ch09-mr-features/src/main/python/max_daily_temp_reduce.py \
  -input input/ncdc/all \
  -output out_max_daily \
  -mapper ch09-mr-features/src/main/python/max_daily_temp_map.py \
  -reducer ch09-mr-features/src/main/python/max_daily_temp_reduce.py

# 第二阶段：求每个站每个月的平均最高气温。
# 输入换成第一阶段的输出 out_max_daily，输出目录 out_mean_max_daily 同样必须事先不存在。
# 这里 -files 列出的两个脚本名相同，是原文照录；
# 真正被 -mapper 与 -reducer 引用的是 mean_max_daily_temp_map.py 与 mean_max_daily_temp_reduce.py。
$STREAM \
  -D stream.num.map.output.key.fields=2 \
  -files ch09-mr-features/src/main/python/mean_max_daily_temp_map.py,\
ch09-mr-features/src/main/python/mean_max_daily_temp_map.py \
  -input out_max_daily \
  -output out_mean_max_daily \
  -mapper ch09-mr-features/src/main/python/mean_max_daily_temp_map.py \
  -reducer ch09-mr-features/src/main/python/mean_max_daily_temp_reduce.py

  