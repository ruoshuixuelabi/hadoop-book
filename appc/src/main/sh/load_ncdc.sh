# 用 Hadoop Streaming 把 NCDC 原始数据从 S3 拉下来并转存到 HDFS
# 输入是路径清单，每行一个 S3 地址；Streaming 不必写 Java 就能拿现成的脚本当 Mapper
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
# 把 reduce 任务数设为 0，因为这一步只需要下载与解包，没有需要聚合的键值
  -D mapred.reduce.tasks=0 \
# 关掉推测执行：推测出来的副本任务会重复下载和上传，导致同一份数据被写入两次
  -D mapred.map.tasks.speculative.execution=false \
# 单个任务的超时时间（毫秒）给得很宽，一个大压缩包下载加解包很容易超过默认值
  -D mapred.task.timeout=12000000 \
# 输入文件就是 create_ncdc_files.sh 生成的清单
  -input ncdc_files.txt \
# 选用 NLineInputFormat 是为了保证每个 map 任务只处理一行，因此一个压缩包正好由一个任务负责下载
  -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
# 输出目录由 Streaming 自动创建，作业要求它事先不存在
  -output output \
# 这个 Mapper 用 shell 脚本实现，靠下面的 -file 参数分发到各个计算节点
  -mapper load_ncdc_map.sh \
# 靠 -file 参数让脚本随作业一起分发到计算节点的本地目录，任务结束后会被清理
  -file load_ncdc_map.sh

