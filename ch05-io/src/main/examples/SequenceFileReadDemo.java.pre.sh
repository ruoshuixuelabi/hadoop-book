# Make sure file is there to be read
# 这段脚本是 SequenceFileReadDemo 的「前置脚本」：先准备好待读的数据，
# 再由 Java 示例去读它。框架在跑示例之前会先执行同名的 .pre.sh。
# 用 SequenceFileWriteDemo 生成一个 SequenceFile，
# 里面装的是 1 到 100 这 100 个整数。
hadoop SequenceFileWriteDemo numbers.seq