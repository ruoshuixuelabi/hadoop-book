// 这个测试讲的是 Spark 里「共享数据」的三种做法与它们的差别：
// 直接把变量捕获进闭包、广播变量、以及累加器。
// 关键点在于：计算跑在集群的各个执行器上，
// 驱动端（driver）和执行器（executor）是不同的进程、不同的 JVM。
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SharedDataTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  // 最朴素的做法：把查找表放进闭包里。
  // 闭包会被复制到每个执行器，每个执行器各持一份完整的表，
  // 表越大浪费越多，而且执行器之间无法互相看到对方的数据。
  test("without shared variable") {
    val lookup = Map(1 -> "a", 2 -> "e", 3 -> "i", 4 -> "o", 5 -> "u")
    val result = sc.parallelize(Array(2, 1, 3)).map(lookup(_))
    assert(result.collect().toSet === Set("a", "e", "i"))
  }

  // 广播变量：只把数据发一份到每个执行器，并在这份数据上加锁，
  // 多个任务读它是安全的；适合「大的只读查找表」这类场景。
  test("broadcast variable") {
    val lookup: Broadcast[Map[Int, String]] =
      sc.broadcast(Map(1 -> "a", 2 -> "e", 3 -> "i", 4 -> "o", 5 -> "u"))
    // 通过 lookup.value 拿到真正的数据，broadcast 本身只是个包装
    val result = sc.parallelize(Array(2, 1, 3)).map(lookup.value(_))
    assert(result.collect().toSet === Set("a", "e", "i"))
  }

  // 朴素累加的做法为什么不行：闭包被复制到各个执行器，
  // 每个副本里的 count 都是各自独立的局部变量，
  // 执行器里的自增根本不会回写到驱动端那个 count 上。
  test("naive accumulator (doesn't work)") {
    var count = 0
    val result = sc.parallelize(Array(1, 2, 3))
      .map(i => { count += 1; i })
      .reduce((x, y) => x + y)
    assert(count === 0) // not updated to 3
    assert(result === 6)
  }

  // 换成累加器就对了：Spark 为它准备了专门的回传通道，
  // 任务结束时执行器上的累加值会被合并回驱动端的那个对象。
  // 但它只适合计数、求和这类可合并的累加，不能拿来传复杂结果。
  test("accumulator") {
    val count: Accumulator[Int] = sc.accumulator(0)
    val result = sc.parallelize(Array(1, 2, 3))
      .map(i => { count += 1; i })
      .reduce((x, y) => x + y)
    assert(count.value === 3)
    assert(result === 6)
  }

}
