# Produce sorted seq file
# 这段脚本是 SequenceFileMapReduceSortResults 的「前置脚本」：
# 先造一个待排序的 SequenceFile，再交给 MapReduce 作业去排序。
# 用 SequenceFileWriteDemo 生成一个 SequenceFile，里面装的是 1 到 100 这 100 个整数。
hadoop SequenceFileWriteDemo numbers.seq

# 调用 examples jar 里的 sort 示例对 SequenceFile 做排序：
# -r 1 表示只用 1 个 reducer；
# 输入格式是 SequenceFileInputFormat，输出格式是 SequenceFileOutputFormat，
# 也就是说输入输出都还是 SequenceFile，而不是文本；
# 键是 IntWritable、值是 Text，这是 sort 示例默认的键值类型。
# 最后的两个参数分别是输入文件和输出目录（目录必须事先不存在）。
hadoop jar $HADOOP_INSTALL/hadoop-*-examples.jar sort -r 1 \
  -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat \
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat \
  -outKey org.apache.hadoop.io.IntWritable \
  -outValue org.apache.hadoop.io.Text \
  numbers.seq sorted