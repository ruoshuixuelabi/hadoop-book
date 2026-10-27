// 这个测试回答一个很实际的问题：把函数传给 Spark 之后，它到底能不能被送到执行器上去。
// 关键在于闭包会「捕获」它引用到的外部变量，
// 被捕获的东西必须可序列化，否则作业在把闭包发出去时就会失败。
import org.apache.spark.{SparkContext, SparkException}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class FunctionSerializationTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    sc = new SparkContext("local", "test")
  }

  override def afterEach() {
    sc.stop()
  }

  // 匿名函数没有捕获任何外部变量，所以闭包里是空的，可以直接序列化。
  test("serializable anonymous function") {
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce((x: Int, y: Int) => x + y)
    assert(result === 6)
  }

  // 局部函数同样是匿名函数的别名，捕获情况一样，因此也能通过。
  test("serializable local function") {
    val myAddFn = (x: Int, y: Int) => x + y
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce(myAddFn)
    assert(result === 6)
  }

  // 全局单例的方法：Scala 把它编译成一个不带任何捕获字段的闭包，
  // 运行时直接通过静态字段 MODULE$ 去调用，所以闭包里没有不可序列化的对象。
  test("serializable global singleton function") {
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce(NonSerializableSingleton.myAddFn)
    assert(result === 6)
  }

  // 普通实例的方法就不行了：闭包捕获的是这个实例，
  // 而 NonSerializableClass 没有实现 Serializable，于是序列化时抛异常。
  test("non-serializable class function") {
    val nonSerializable = new NonSerializableClass()
    val params = sc.parallelize(Array(1, 2, 3))
    intercept[SparkException] {
      val result = params.reduce(nonSerializable.myAddFn)
    }
  }

  // 只要把实例所在的类改成可序列化的，同样的写法就能跑通。
  test("serializable class function") {
    val serializable = new SerializableClass()
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce(serializable.myAddFn)
    assert(result === 6)
  }

}

// 单例对象本身没有实现 Serializable，但它的方法被闭包引用时
// 编译成的是对静态字段 MODULE$ 的访问，因此闭包不必序列化这个对象。
object NonSerializableSingleton {
  def myAddFn(x: Int, y: Int): Int = { x + y }
}

// 这个类故意不实现 Serializable，用来触发默认序列化器的失败。
class NonSerializableClass {
  def myAddFn(x: Int, y: Int): Int = { x + y }
}

// 加上 Serializable 之后，捕获这个实例的闭包就能被送到执行器上。
class SerializableClass extends Serializable {
  def myAddFn(x: Int, y: Int): Int = { x + y }
}
