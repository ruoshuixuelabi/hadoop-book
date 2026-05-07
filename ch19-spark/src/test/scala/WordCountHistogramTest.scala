/**
 * 文件：WordCountHistogramTest.scala<br>
 * 路径：src/test/scala/WordCountHistogramTest.scala<br>
 * 描述：词频统计直方图测试<br>
 *<br>
 * 功能说明：<br>
 * 测试Spark对文本进行词频统计并生成直方图的能力。<br>
 * 直方图展示了每个单词出现的频率分布，<br>
 * 例如：有几个单词出现1次、几个单词出现2次等。<br>
 */
import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.Map

/**
 * 词频统计直方图测试类<br>
 * 测试使用Spark进行分布式词频统计和频率分布分析<br>
 */
class WordCountHistogramTest extends FunSuite with BeforeAndAfterEach {

  // SparkContext变量
  var sc: SparkContext = _

  /**
   * 每个测试用例执行前的初始化方法<br>
   */
  override def beforeEach() {
    // 创建local模式的SparkContext
    sc = new SparkContext("local", "test")
  }

  /**
   * 每个测试用例执行后的清理方法<br>
   */
  override def afterEach() {
    // 停止SparkContext释放资源
    sc.stop()
  }

  /**
   * 测试：词频直方图<br>
   * 统计每个单词出现的次数，然后计算频率分布<br>
   * 例如：出现1次的单词有几个，出现2次的单词有几个<br>
   */
  test("word count histogram") {
    // 创建临时文件
    val input: File = File.createTempFile("input", "")

    // 复制set2.txt测试数据到临时文件
    // set2.txt包含用于词频测试的样本数据
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("set2.txt")), input)

    // 词频统计直方图计算流程：
    // Step 1: textFile读取文件，每行一个单词
    // Step 2: map将每个单词转换为(单词, 1)对，并转为小写
    // Step 3: reduceByKey统计每个单词出现的次数
    // Step 4: map(_.swap)交换(单词, 次数)为(次数, 单词)
    // Step 5: countByKey统计每个次数对应的单词数量
    val hist: Map[Int, Long] = sc.textFile(input.getPath)
      // 将每个单词转为(单词小写, 1)的key-value对
      .map(word => (word.toLowerCase(), 1))
      // 按单词分组，统计每个单词出现的总次数
      .reduceByKey((a, b) => a + b)
      // 交换key和value：变为(次数, 单词)
      .map(_.swap)
      // countByKey是action，统计每个次数（key）有多少个单词（value的个数）
      // 返回Map[次数, 该次数对应的单词数量]

    // 断言：直方图包含2种不同的出现次数（1次和2次）
    assert(hist.size === 2)

    // 断言：有3个单词只出现了1次
    assert(hist(1) === 3) // three elements occur once

    // 断言：有1个单词出现了2次
    assert(hist(2) === 1) // one element occurs twice
  }
}
