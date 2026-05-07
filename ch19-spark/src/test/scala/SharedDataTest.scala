/**
 * 文件：SharedDataTest.scala<br>
 * 路径：src/test/scala/SharedDataTest.scala<br>
 * 描述：Spark共享变量测试套件<br>
 *<br>
 * 功能说明：<br>
 * 测试Spark的两种共享变量机制：<br>
 * 1. Broadcast（广播变量）：高效分发只读数据到所有节点<br>
 * 2. Accumulator（累加器）：跨节点聚合计算结果的机制<br>
 * 同时验证不使用共享变量时的行为以及Naive累加器的局限性。<br>
 */
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
 * 共享变量测试类<br>
 * 测试Spark分布式环境下的数据共享机制<br>
 */
class SharedDataTest extends FunSuite with BeforeAndAfterEach {

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
   * 测试：不使用共享变量<br>
   * 直接在闭包中使用外部Map会导致每个任务都拷贝一份数据，效率低下<br>
   */
  test("without shared variable") {
    // 定义查找表（Vowel元音字母映射）
    val lookup = Map(1 -> "a", 2 -> "e", 3 -> "i", 4 -> "o", 5 -> "u")

    // 并行化数组，map操作中引用外部lookup
    // 问题：lookup会被序列化发送到每个节点，每个任务执行时都会拷贝一份
    // 这在小数据量时不明显，但大数据量时会显著增加网络传输和内存开销
    val result = sc.parallelize(Array(2, 1, 3)).map(lookup(_))

    // 断言：验证结果正确（虽然功能正确但效率不佳）
    assert(result.collect().toSet === Set("a", "e", "i"))
  }

  /**
   * 测试：广播变量<br>
   * 使用broadcast将只读数据广播到所有节点，避免每个任务重复拷贝<br>
   */
  test("broadcast variable") {
    // 使用broadcast方法将只读Map广播到所有节点
    // 广播变量只会在每个节点存储一份，所有任务共享同一个实例
    val lookup: Broadcast[Map[Int, String]] =
      sc.broadcast(Map(1 -> "a", 2 -> "e", 3 -> "i", 4 -> "o", 5 -> "u"))

    // 并行化数组，通过lookup.value访问广播变量的值
    // .value属性返回广播数据的只读引用
    val result = sc.parallelize(Array(2, 1, 3)).map(lookup.value(_))

    // 断言：验证结果正确
    assert(result.collect().toSet === Set("a", "e", "i"))
  }

  /**
   * 测试：Naive累加器（不推荐的方式）<br>
   * 演示使用普通var变量作为累加器的问题：闭包中的修改不会反映到driver<br>
   */
  test("naive accumulator (doesn't work)") {
    // 声明普通var变量用于计数
    var count = 0

    // 并行化数组，在map闭包中累加count
    // 问题：闭包中的count是对driver端变量的引用（值拷贝）
    // 每个节点的计算都是独立的，driver端的count永远不会改变
    val result = sc.parallelize(Array(1, 2, 3))
      .map(i => { count += 1; i })  // 这里的count是闭包内的局部拷贝
      .reduce((x, y) => x + y)

    // 断言：count仍然是0，因为闭包内的修改没有反映到driver
    assert(count === 0) // not updated to 3

    // reduce结果6是正确的，因为reduce是行动操作，结果会返回到driver
    assert(result === 6)
  }

  /**
   * 测试：Spark Accumulator（累加器）<br>
   * 使用Spark提供的Accumulator实现跨节点安全聚合<br>
   */
  test("accumulator") {
    // 使用SparkContext.accumulator创建累加器，初始值为0
    // Accumulator是Spark提供的特殊共享变量，支持跨节点安全累加
    val count: Accumulator[Int] = sc.accumulator(0)

    // 并行化数组，在map闭包中使用累加器
    // 每个节点执行map时都会调用count += 1
    // Spark保证累加操作只会被执行一次（即使task重试）
    val result = sc.parallelize(Array(1, 2, 3))
      .map(i => { count += 1; i })  // 累加操作会被安全地聚合
      .reduce((x, y) => x + y)

    // 断言：count.value可以获取累加后的值（3，因为有3个元素）
    assert(count.value === 3)

    // 断言：reduce结果仍然是6
    assert(result === 6)
  }

}
