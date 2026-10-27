# 用 Hadoop Streaming 演示「二次排序」：同一年内按气温降序，
# 于是每个年份的第一条记录就是该年的最高温。
# 下面这条命令的参数是连成一行的（行尾用反斜杠续行），
# 所以解释各参数的注释只能写在命令的前面，不能插在续行之间。
# 各参数逐个说明如下：
# 指定 Map 输出有两个键字段，分隔符默认是制表符；
# 分区器只按第一个字段（年份）分区，保证同一年落到同一个 reducer；
# 比较器换成 KeyFieldBasedComparator，它能按字段逐个比较；
# 它的比较选项 "-k1n -k2nr" 表示：第 1 字段按数字升序，第 2 字段按数字降序；
# 把两个 python 脚本分发到计算节点；
# 输入是全部 NCDC 记录，输出目录必须事先不存在；
# 指定自定义分区器，它保证同一年分到同一个 reducer。
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D stream.num.map.output.key.fields=2 \
  -D mapreduce.partition.keypartitioner.options=-k1,1 \
  -D mapreduce.job.output.key.comparator.class=\
org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
  -D mapreduce.partition.keycomparator.options="-k1n -k2nr" \
  -files secondary_sort_map.py,secondary_sort_reduce.py \
  -input input/ncdc/all \
  -output output-secondarysort-streaming \
  -mapper ch09-mr-features/src/main/python/secondary_sort_map.py \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -reducer ch09-mr-features/src/main/python/secondary_sort_reduce.py
  