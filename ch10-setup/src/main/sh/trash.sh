# 演示 HDFS 的回收站机制。
# 回收站需要 fs.trash.interval 大于 0 才会启用，默认是 0（关闭）。
# 下面这一组命令演示「删除的文件其实没有立刻消失，而是被搬进了 .Trash」：
# 建一个空文件：-touchz 只创建时间戳、不写内容。
hadoop fs -touchz quangle
# 删除它。如果开了回收站，这个文件会被移动到 .Trash/Current/ 下。
hadoop fs -rm quangle
# 递归列出 .Trash，可以看到 TrashRoot、Current 等目录。
hadoop fs -lsr .Trash
# 把文件从回收站搬回当前目录。
hadoop fs -mv .Trash/Current/quangle .
# 确认文件已经回来了。
hadoop fs -ls .