# 这段 R 脚本把 MapReduce 算出的温度分布画成一张 PNG 图片。
# 前提是已经跑过把结果排序写盘的作业，
# 按温度排序的结果就在当前目录下的 output_sorted 文件里。
# png() 打开一个 PNG 输出设备，并指定图片文件名。
# read.table 读入上面那个文件；默认第一行是表头，所以 header=TRUE 是默认值。
# plot 把数据画成直方图：横轴是气温，纵轴是落在每个区间里的读数条数。
# dev.off() 关闭绘图设备，把缓冲区里的内容真正写进 PNG 文件。
# 这一句必须放在最后，否则图片会是空的。
png("temperature_distribution.png")
data <- read.table("output_sorted")
plot(data, xlab="Temperature", ylab="Number of readings")
dev.off()