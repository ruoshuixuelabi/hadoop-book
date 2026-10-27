// 这个测试统计「每个词出现了几次」，再统计「出现 N 次的词有多少个」。
// 后者就是词频直方图，用来展示 countByKey 这类动作的用法。
import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.Map

class WordCountHistogramTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  test("word count histogram") {
    // 测试资源 set2.txt 有五个单词：b、c、a、e、b
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("set2.txt")), input)
    // 先算出「词 -> 出现次数」，
    // 再 swap 把键值对倒过来变成「出现次数 -> 词」，
    // 最后 countByKey 数一数每个出现次数对应多少个词，得到直方图。
    val hist: Map[Int, Long] = sc.textFile(input.getPath)
      .map(word => (word.toLowerCase(), 1))
      .reduceByKey((a, b) => a + b)
      .map(_.swap)
      .countByKey()
    // 共有两种不同的出现次数：1 次和 2 次
    assert(hist.size === 2)
    assert(hist(1) === 3) // three elements occur once
    assert(hist(2) === 1) // one element occurs twice
  }
}
