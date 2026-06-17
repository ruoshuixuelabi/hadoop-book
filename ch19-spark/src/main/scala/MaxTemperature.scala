/**
 * 文件：MaxTemperature.scala<br>
 * 路径：src/main/scala/MaxTemperature.scala<br>
 * 描述：计算气象数据中的最高温度程序<br>
 *<br>
 * 功能说明：<br>
 * 该程序从输入文件中读取气象数据（制表符分隔），<br>
 * 过滤掉无效温度记录（9999表示缺失），<br>
 * 提取年份和温度信息，<br>
 * 按年份分组计算每年的最高温度，<br>
 * 最终将结果保存到输出目录。<br>
 *<br>
 * 使用方式：<br>
 * spark-submit --class MaxTemperature <jar> <输入路径> <输出路径><br>
 *<br>
 * 输入数据格式：气象站ID\t年份\t温度\t...（制表符分隔）<br>
 * 输出结果：每个年份及其对应的最高温度<br>
 */
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object MaxTemperature {
  /**
   * 程序入口点<br>
   *<br>
   * @param args 命令行参数<br>
   *             args(0): 输入文件路径（HDFS或本地文件系统）<br>
   *             args(1): 输出目录路径（结果将保存至此）<br>
   */
  def main(args: Array[String]) {
    // 创建Spark配置对象，用于设置应用程序名称
    val conf = new SparkConf().setAppName("Max Temperature")

    // 基于配置创建SparkContext，这是与Spark集群交互的入口点
    val sc = new SparkContext(conf)

    // 从输入路径读取文本文件，每行作为一个元素创建RDD
    sc.textFile(args(0))
      // 将每行文本按制表符分割成字符串数组
      .map(_.split("\t"))
      /**
       * 过滤无效记录<br>
       * rec(1): 温度字段，9999表示数据缺失<br>
       * rec(2): 质量码字段，仅保留0、1、4、5、9（有效数据）<br>
       * matches("[01459]"): 正则匹配质量码是否为有效值<br>
       */
      .filter(rec => (rec(1) != "9999" && rec(2).matches("[01459]")))
      // 提取年份和温度，转换为(年份Int, 温度Int)的二元组
      .map(rec => (rec(0).toInt, rec(1).toInt))
      // 按年份分组，聚合时取同一年份中的最大温度值
      .reduceByKey((a, b) => Math.max(a, b))
      // 将结果保存为文本文件到指定输出目录
      .saveAsTextFile(args(1))
  }
}
