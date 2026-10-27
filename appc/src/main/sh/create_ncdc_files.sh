# 生成 NCDC 原始数据在 S3 上的路径清单，作为 load_ncdc.sh 的输入文件
# 每年一个压缩包，命名规则是 isd-<年份>.tar.bz2；1901 到 2000 共 100 个
for ((i=1901;i<=2000;i+=1))
do
# 这里只输出路径而不真正下载，真正的下载交给 Map 任务并行去做
  echo s3n://hadoopbook/ncdc/raw/isd-$i.tar.bz2
done
