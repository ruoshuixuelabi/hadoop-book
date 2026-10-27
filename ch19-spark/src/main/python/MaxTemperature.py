# 这是第 2 章「求每年最高气温」作业的 PySpark 版本，和 Scala 版做的是同一件事。
# 与 MapReduce 最大的不同是延迟计算：链式调用里的每一步都只是在描述计算过程，
# 真正把作业跑起来的是最后那行 saveAsTextFile。
from pyspark import SparkContext
import re, sys

# 创建 SparkContext 时，第一个参数是 master 模式，local 表示用本机的线程模拟一个集群，便于本地调试。
sc = SparkContext("local", "Max Temperature")
# 输入路径和输出路径都从命令行参数取：第一个参数是输入目录，第二个是输出目录。
# 下面这条链用反斜杠续行写成一条语句，避免 Python 的括号嵌套把代码缩进弄乱；
# 也正因为整条链靠续行拼起来，注释只能写在链的前后，不能插在链的中间。
#
# 链上每一步的含义：
#   第一步是读入：textFile 读入文本目录，得到每行一个字符串的 RDD。
#   第二步是切分：每行是制表符分隔的三个字段（年份、气温、质量位），切分后得到字符串列表。
#   第三步是过滤：丢掉无效读数，9999 表示温度缺测，质量位只认 0、1、4、5、9。
#     这里用 re.match 而不是 Java 里的 matches，因为 re.match 只从串首匹配，
#     而质量位本来就只有一个字符，所以够用。
#   第四步是映射：变成「年份 -> 气温」的键值对，两边都要转成整数才能参与数值比较。
#   第五步是聚合：按年份求最大值，max 直接作为归约函数传进去即可。
#   第六步是写出：saveAsTextFile 触发真正的计算，并把结果写成输出目录下的若干个文件。
sc.textFile(sys.argv[1]) \
  .map(lambda s: s.split("\t")) \
  .filter(lambda rec: (rec[1] != "9999" and re.match("[01459]", rec[2]))) \
  .map(lambda rec: (int(rec[0]), int(rec[1]))) \
  .reduceByKey(max) \
  .saveAsTextFile(sys.argv[2])
