# 测试跑完之后，输出目录可能还留在 HDFS 上，
# 先看本地有没有，没有再从 HDFS 拉回来。
if [ ! -e output ]; then
  hadoop fs -get output .
# 把 HDFS 上的输出目录下载到当前目录。
fi
