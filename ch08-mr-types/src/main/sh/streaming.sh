# 用 Hadoop Streaming 跑 MapReduce：不写一行 Java，直接把现成的命令指定成 Mapper 或 Reducer。
# 第一条只指定了 Mapper，Reducer 缺省就是 IdentityReducer，输出与输入一一对应。
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input input/ncdc/sample.txt \
  -output output \
  -mapper /bin/cat
  
# 第二条把各个部件都显式写出来，便于看清 Streaming 各个默认值的真面目。
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input input/ncdc/sample.txt \
  -output output \
  -inputformat org.apache.hadoop.mapred.TextInputFormat \
  -mapper /bin/cat \
  -partitioner org.apache.hadoop.mapred.lib.HashPartitioner \
  -numReduceTasks 1 \
  -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
  -outputformat org.apache.hadoop.mapred.TextOutputFormat