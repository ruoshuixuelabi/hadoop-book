#!/usr/bin/env bash
# 这是 Streaming 的 Mapper 脚本：把 S3 上的 NCDC 压缩包下载到本地解开，再整理成 HDFS 上的文本

# NLineInputFormat gives a single line: key is offset, value is S3 URI
# 每次只喂进来一行（这由 NLineInputFormat 保证），键是行偏移、值是那一行的内容，所以第二个字段就是 S3 地址
read offset s3file

# Retrieve file from S3 to local disk
# 先把压缩包抓到 map 任务所在节点的本地盘再解压，节点本地做这件事比反复走网络读 HDFS 快得多
# 以 reporter:status: 开头的行会被 Streaming 收集成任务进度上报到作业页面，
# 有了它才能看出是哪个压缩包卡住了
echo "reporter:status:Retrieving $s3file" >&2
$HADOOP_INSTALL/bin/hadoop fs -get $s3file .
# 这里的 $HADOOP_INSTALL 必须在每个计算节点上都指向同一套 Hadoop 安装，
# 与提交端用的 $HADOOP_HOME 不是同一个变量，别只在提交机上设置它

# Un-bzip and un-tar the local file
# 解压后得到一个以年份命名的目录，目录里是各气象站各自的压缩文件
target=`basename $s3file .tar.bz2`
mkdir -p $target
echo "reporter:status:Un-tarring $s3file to $target" >&2
tar jxf `basename $s3file` -C $target

# Un-gzip each station file and concat into one file
echo "reporter:status:Un-gzipping $target" >&2
# 两层通配：压缩包解开后是「年份/站点文件」的结构，所以要用两级通配才能拿到每个站点的文件
for file in $target/*/*
do
# 把各站点文件解压后的内容追加成一个文本文件，这样 HDFS 上每个年份只有一个文件
  gunzip -c $file >> $target.all
  echo "reporter:status:Processed $file" >&2
done

# Put gzipped version into HDFS
# 重新压缩再放回 HDFS：既省存储，又让后续的 MapReduce 作业只需要读一次这个文件
echo "reporter:status:Gzipping $target and putting in HDFS" >&2
# 用管道把压缩结果直接送进 hadoop fs -put，中间的临时文件不必落在磁盘上
gzip -c $target.all | $HADOOP_INSTALL/bin/hadoop fs -put - gz/$target.gz