/**
 * 文件：MaxTemperatureWithPlacement.scala<br>
 * 路径：src/main/scala/MaxTemperatureWithPlacement.scala<br>
 * 描述：计算气象数据中的最高温度程序（带数据本地性优化）<br>
 *<br>
 * 功能说明：<br>
 * 该程序是MaxTemperature的增强版本，在创建SparkContext时，<br>
 * 通过InputFormatInfo.computePreferredLocations计算输入数据的最优位置，<br>
 * 使Spark能够将计算任务调度到数据所在的节点上执行，<br>
 * 从而减少网络传输，提升性能。<br>
 *<br>
 * 使用方式：<br>
 * spark-submit --class MaxTemperatureWithPlacement <jar> <输入路径> <输出路径><br>
 *<br>
 * 输入数据格式：气象站ID\t年份\t温度\t...（制表符分隔）<br>
 * 输出结果：每个年份及其对应的最高温度<br>
 */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.spark.{SparkConf, SparkContext}

object MaxTemperatureWithPlacement {
  /**
   * 程序入口点<br>
   *<br>
   * @param args 命令行参数<br>
   *             args(0): 输入文件路径<br>
   *             args(1): 输出目录路径<br>
   */
  def main(args: Array[String]) {
    // 获取输入路径参数
    val inputPath = args(0)

    // 创建Spark配置对象，设置应用程序名称
    val conf = new SparkConf().setAppName("Max Temperature")

    /**
     * 计算输入数据的最优位置<br>
     * InputFormatInfo.computePreferredLocations分析输入数据所在的HDFS块位置，<br>
     * 返回应该运行任务的节点列表，以实现数据本地性<br>
     */
    val preferredLocations = InputFormatInfo.computePreferredLocations(
      // 创建InputFormatInfo，指定使用TextInputFormat读取输入数据
      Seq(new InputFormatInfo(new Configuration(), classOf[TextInputFormat],
        inputPath)))

    // 使用计算出的最优位置创建SparkContext
    // 这样Spark会将第一个阶段的任务优先调度到包含数据的节点
    val sc = new SparkContext(conf, preferredLocations)

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
