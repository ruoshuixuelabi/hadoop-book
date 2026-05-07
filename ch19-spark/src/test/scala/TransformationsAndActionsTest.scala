/**
 * 文件：TransformationsAndActionsTest.scala<br>
 * 路径：src/test/scala/TransformationsAndActionsTest.scala<br>
 * 描述：Spark Transformation与Action操作测试套件<br>
 *<br>
 * 功能说明：<br>
 * 测试Spark RDD的核心概念：<br>
 * 1. Transformation（转换）：延迟执行，定义计算逻辑<br>
 * 2. Action（动作）：触发实际计算，返回结果到driver<br>
 * 3. 各种key-value类型的transformation：reduceByKey、foldByKey、aggregateByKey<br>
 * 4. 自定义MapReduce框架实现<br>
 */
import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable.HashSet
import scala.reflect.ClassTag

/**
 * Transformation与Action测试类<br>
 */
class TransformationsAndActionsTest extends FunSuite with BeforeAndAfterEach {

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
   * 测试：懒执行的Transformation<br>
   * 演示Spark的lazy特性：Transformation不会立即执行，只有Action才会触发计算<br>
   */
  test("lazy transformations") {
    // 创建临时文件
    val input: File = File.createTempFile("input", "")

    // 复制fruit.txt测试数据到临时文件
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
      input)

    // 使用textFile读取文件，创建RDD
    val text = sc.textFile(input.getPath)

    // map是Transformation，不会立即执行，只是定义了转换逻辑
    // Spark会记录这个操作但不会立即计算
    val lower: RDD[String] = text.map(_.toLowerCase())

    // 打印提示信息，标记toLowerCase调用
    println("Called toLowerCase")

    // foreach是Action，它会触发之前所有懒执行的Transformation
    // 此时Spark才会真正执行textFile -> map -> foreach这条计算链
    lower.foreach(println(_))
  }

  /**
   * 测试：map-reduce模式<br>
   * 演示如何使用自定义MapReduce框架进行词频统计<br>
   */
  test("map reduce") {
    // 创建临时文件
    val input: File = File.createTempFile("input", "")

    // 复制测试数据文件
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("quangle.txt")),
      input)

    // 读取文本文件
    val text: RDD[String] = sc.textFile(input.getPath)

    // 将文本转换为(word, 1)格式的key-value对
    // flatMap: 先split分割每个单词，再将每个单词映射为(单词, 1)
    // flatMap会将所有结果展平为一个RDD
    val in: RDD[(String, Int)] = text.flatMap(_.split(" ")).map(word => (word, 1))

    // 定义map函数：将每个(key, value)对转换为List（此处是恒等变换）
    val mapFn = (kv: (String, Int)) => List(kv)

    // 定义reduce函数：对相同key的值求和
    val reduceFn = (kv: (String, Iterable[Int])) => List((kv._1, kv._2.sum))

    // 创建自定义MapReduce实例，执行naiveMapReduce并触发foreach打印
    new MapReduce[String, Int, String, Int, String, Int]()
      .naiveMapReduce(in, mapFn, reduceFn).foreach(println(_))
  }

  /**
   * 测试：reduceByKey<br>
   * reduceByKey是针对key-value RDD的transformation，将相同key的值聚合<br>
   */
  test("reduceByKey") {
    // 创建key-value对的RDD
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))

    // reduceByKey：对相同key的值使用_+_进行求和
    // ("a", 3) + ("a", 1) + ("a", 5) = 9
    // ("b", 7) = 7
    val sums: RDD[(String, Int)] = pairs.reduceByKey(_+_)

    // 断言验证结果
    assert(sums.collect().toSet === Set(("a", 9), ("b", 7)))
  }

  /**
   * 测试：foldByKey<br>
   * foldByKey与reduceByKey类似，但支持提供初始值（zero value）<br>
   * 初始值必须满足结合律和单位元性质<br>
   */
  test("foldByKey") {
    // 创建key-value对的RDD
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))

    // foldByKey(0)(_+_): 提供初始值0，用于分区内聚合
    // 效果与reduceByKey相同，但对空分区会返回初始值
    val sums: RDD[(String, Int)] = pairs.foldByKey(0)(_+_)

    // 断言验证结果
    assert(sums.collect().toSet === Set(("a", 9), ("b", 7)))
  }

  /**
   * 测试：aggregateByKey<br>
   * aggregateByKey是最通用的key-value聚合函数，<br>
   * 支持返回与输入类型不同的聚合结果<br>
   */
  test("aggregateByKey") {
    // 创建key-value对的RDD
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))

    // aggregateByKey有三个参数：
    // 1. initialValue: HashSet[Int]() - 每分区内聚合的初始值（空集合）
    // 2. seqOp: (_+=_) - 分区内聚合函数，将值添加到HashSet
    // 3. combOp: (_++=_) - 分区间聚合函数，合并两个HashSet
    val sets: RDD[(String, HashSet[Int])] =
      pairs.aggregateByKey(new HashSet[Int])(_+=_, _++=_)

    // 断言：相同key的值被聚合为一个集合
    // "a"对应的值{3, 1, 5}聚合为Set(1, 3, 5)
    // "b"对应的值{7}聚合为Set(7)
    assert(sets.collect().toSet === Set(("a", Set(1, 3, 5)), ("b", Set(7))))
  }

}

/**
 * 自定义MapReduce框架类<br>
 * 演示MapReduce的基本工作流程：map -> shuffle -> reduce<br>
 *
 * @tparam K1 输入key类型
 * @tparam V1 输入value类型
 * @tparam K2 中间key类型
 * @tparam V2 中间value类型
 * @tparam K3 输出key类型
 * @tparam V3 输出value类型
 */
class MapReduce[K1, V1, K2, V2, K3, V3] {
  /**
   * 朴素MapReduce实现<br>
   * 工作流程：map -> groupByKey -> sortByKey -> reduce -> output<br>
   *<br>
   * @param input 输入RDD<br>
   * @param mapFn map函数，将(K1, V1)转换为TraversableOnce[(K2, V2)]<br>
   * @param reduceFn reduce函数，将(K2, Iterable[V2])转换为TraversableOnce[(K3, V3)]<br>
   * @tparam K2 中间key类型，需要Ordering和ClassTag<br>
   * @tparam V2 中间value类型，需要ClassTag<br>
   * @return 包含最终结果的RDD[(K3, V3)]<br>
   */
  def naiveMapReduce[K2: Ordering: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
                                            mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
                                            reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
  RDD[(K3, V3)] = {
    // Step 1: Map阶段
    // flatMap：对每个输入元素应用mapFn，将结果展平
    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)

    // Step 2: Shuffle阶段
    // groupByKey：根据key分组，将相同key的所有value聚合成Iterable
    // sortByKey：对结果按键排序（需要K2实现Ordering）
    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput.groupByKey().sortByKey()

    // Step 3: Reduce阶段
    // flatMap：对每个(key, Iterable[values])应用reduceFn
    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)

    // 返回结果RDD（仍然是lazy的，需要Action触发）
    output
  }

//  /**
//   * 改进版MapReduce实现（Spark 1.2.0）<br>
//   * 使用repartitionAndSortWithinPartitions优化shuffle阶段<br>
//   * 注释掉的代码因为SPARK-2978问题在早期版本不可用<br>
//   */
//  def betterMapReduce[K2: Ordering: ClassTag, V2: ClassTag](input: RDD[(K1, V1)],
//                                            mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)],
//                                            reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
//  RDD[(K3, V3)] = {
//    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
//    // 使用repartitionAndSortWithinPartitions在shuffle时排序
//    // 比groupByKey + sortByKey更高效，因为排序和分区可以合并
//    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput
//      .groupByKey()
//      .repartitionAndSortWithinPartitions(Partitioner.defaultPartitioner(mapOutput))
//    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)
//    output
//  }
}
