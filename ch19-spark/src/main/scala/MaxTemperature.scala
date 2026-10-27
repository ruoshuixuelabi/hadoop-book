// 用 Spark 求每年的最高气温，这是第 2 章 MapReduce 版的 Scala 对照实现。
// 可以和 ch19-spark/src/main/java/MaxTemperatureSpark.java 逐行比对：
// 计算步骤完全一致，区别只是 Scala 的链式调用写起来更紧凑。
// 整条链路上只有最后一行的 saveAsTextFile 才是「动作」，
// 在它之前的所有 map/filter/reduceByKey 都只是把计算步骤记下来，并不会真的执行。
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object MaxTemperature {
  def main(args: Array[String]) {
    // 应用名会显示在 Spark 的网页界面上，方便一眼认出是哪个作业在跑
    val conf = new SparkConf().setAppName("Max Temperature")
    // 这里故意不设 master，而是沿用提交作业时给定的配置；
    // 本地跑的时候通常由启动命令把模式传进来，避免把运行方式写死在代码里
    val sc = new SparkContext(conf)

    // 输入用 textFile 读进来，是制表符分隔的气温记录，每行三个字段：年份、温度、质量位
    sc.textFile(args(0))
      // 按制表符切成字段数组：下划线是 Scala 的占位参数，等价于写 x => x.split("\t")
      .map(_.split("\t"))
      // 过滤掉不可用的读数：9999 是「温度缺测」的占位值，
      // 而质量位只接受 0/1/4/5/9 这几种取值，其余（缺测/可疑/事后校订等）一律丢弃
      .filter(rec => (rec(1) != "9999" && rec(2).matches("[01459]")))
      // 变成「年份 -> 温度」的键值对，注意 toInt 是必须的：
      // 而 split 出来的都是字符串，不转成整数就没法比较大小
      .map(rec => (rec(0).toInt, rec(1).toInt))
      // 按年聚合取最大值；reduceByKey 会先在 map 端做局部合并再洗牌，
      // 比先 groupByKey 再归约要省很多网络传输和内存
      .reduceByKey((a, b) => Math.max(a, b))
      // 这里是唯一真正触发计算的地方，输出的是一个目录而不是单个文件
      .saveAsTextFile(args(1))
  }
}