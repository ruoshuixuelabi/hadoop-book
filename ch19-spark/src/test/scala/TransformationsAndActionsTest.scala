// 这个测试对比 Spark 提供的几种按键聚合算子，
// 并用一个手写的 MapReduce 框架来解释这些算子底下到底做了什么。
import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.HashSet
import scala.reflect.ClassTag

class TransformationsAndActionsTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  // 转换是惰性的：map 只是把「要做什么」记下来，并不真的执行，
  // 所以 println 出现在 foreach 之前；foreach 是动作，才真正把任务跑起来。
  test("lazy transformations") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
      input)
    val text = sc.textFile(input.getPath)
    val lower: RDD[String] = text.map(_.toLowerCase())
    println("Called toLowerCase")
    lower.foreach(println(_))
  }

  // 用 MapReduce 框架的写法跑一遍字频统计，
  // 看清 map、分区、排序、归约这几步在 Spark 里对应哪些算子。
  test("map reduce") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("quangle.txt")),
      input)
    val text: RDD[String] = sc.textFile(input.getPath)

    // turn into input key-value pairs
    val in: RDD[(String, Int)] = text.flatMap(_.split(" ")).map(word => (word, 1))

    // 这里的 map 输出「一个键值对」，reduce 输入「一个键加上这个键的所有值」，输出「一个键值对」
    val mapFn = (kv: (String, Int)) => List(kv)
    val reduceFn = (kv: (String, Iterable[Int])) => List((kv._1, kv._2.sum))
    new MapReduce[String, Int, String, Int, String, Int]()
      .naiveMapReduce(in, mapFn, reduceFn).foreach(println(_))
  }

  // 算子 reduceByKey 是最常用的聚合：它会先在 map 端做局部合并再洗牌，
  // 因此网络上传输的数据量远小于 groupByKey 的做法。
  test("reduceByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sums: RDD[(String, Int)] = pairs.reduceByKey(_+_)
    assert(sums.collect().toSet === Set(("a", 9), ("b", 7)))
  }

  // 算子 foldByKey 和 reduceByKey 很像，只是要显式给出「零值」，
  // 当聚合函数不是结合律、或者需要区分空集合与零值时才必须用它。
  test("foldByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sums: RDD[(String, Int)] = pairs.foldByKey(0)(_+_)
    assert(sums.collect().toSet === Set(("a", 9), ("b", 7)))
  }

  // 算子 aggregateByKey 更通用：它允许每个键有自己的初始值，
  // 而且把「分区内的合并」和「分区间的合并」分成两个函数，
  // 因此可以实现 reduceByKey 表达不了的逻辑，比如这里按键求集合。
  test("aggregateByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    val sets: RDD[(String, HashSet[Int])] =
      pairs.aggregateByKey(new HashSet[Int])(_+=_, _++=_)
    assert(sets.collect().toSet === Set(("a", Set(1, 3, 5)), ("b", Set(7))))
  }

}

// 一个手写的 MapReduce 框架，用来对照理解 Spark 内部的数据流。
class MapReduce[K1, V1, K2, V2, K3, V3] {
  def naiveMapReduce[K2: Ordering: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
                                            mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
                                            reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
  RDD[(K3, V3)] = {
    // 这里的 map 阶段：flatMap 把每个输入展开成若干个中间键值对
    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
    // 随后的 shuffle 阶段：groupByKey 把同一个键的值聚到一起，sortByKey 只是为了让输出有序
    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput.groupByKey().sortByKey()
    // 最后的 reduce 阶段：flatMap 把每个键的一组值归约成一个键值对
    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)
    output
  }

// 下面这段被注释掉的代码是书里给出的「更好的」MapReduce 实现：
// 它把 sortByKey 换成了 repartitionAndSortWithinPartitions，
// 也就是用哈希分区加「分区内排序」来替代「按范围分区」的那次洗牌，
// 排序因此只在分区内做，不再需要为全局有序去划分区间。
// 但它依赖 Spark 1.2.0 才引入的新 API，所以书里保留着而没有启用；
// 文件开头那行 Partitioner 的导入就是为它准备的。
//  // Spark 1.2.0, see https://issues.apache.org/jira/browse/SPARK-2978
//  def betterMapReduce[K2: Ordering: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
//                                            mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
//                                            reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
//  RDD[(K3, V3)] = {
//    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
//    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput
//      .groupByKey()
//      .repartitionAndSortWithinPartitions(Partitioner.defaultPartitioner(mapOutput))
//    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)
//    output
//  }
}
