#!/bin/sh
# 这个脚本在集成测试开始前准备 HDFS 上的输入和输出目录。
# 输入目录不存在就上传本地的 input。
if ! hadoop fs -test -e input; then
  hadoop fs -put input .
# 用 -put 把本地 input 目录传到 HDFS 上。
fi
# 输出目录如果已经存在，先在 HDFS 上删掉。
if hadoop fs -test -e output; then
  hadoop fs -rmr output
# 递归删除 HDFS 上的 output。
fi
# 本地的 output 也可能残留，一并清掉。
if [ -e output ]; then
  rm -r output
# 删掉本地的输出目录。
fi
