/**
 * 文件：FunctionSerializationTest.scala<br>
 * 路径：src/test/scala/FunctionSerializationTest.scala<br>
 * 描述：Spark函数序列化能力测试套件<br>
 *<br>
 * 功能说明：<br>
 * 测试Spark在分布式环境下对不同类型函数的序列化支持能力。<br>
 * 包括匿名函数、局部函数、全局单例对象方法、可序列化类和不可序列化类的方法。<br>
 * 理解函数的序列化特性对于编写正确的Spark分布式程序至关重要。<br>
 */
import org.apache.spark.{SparkContext, SparkException}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
 * 函数序列化测试类<br>
 * 使用ScalaTest框架验证函数在Spark分布式环境中的序列化行为<br>
 */
class FunctionSerializationTest extends AnyFunSuite with BeforeAndAfterEach {

  // SparkContext变量，用于在测试中与Spark交互
  var sc: SparkContext = _

  /**
   * 每个测试用例执行前的初始化方法<br>
   * 创建local模式的SparkContext用于测试<br>
   */
  override def beforeEach(): Unit = {
    // 创建local模式的SparkContext，模拟单机环境进行测试
    sc = new SparkContext("local", "test")
  }

  /**
   * 每个测试用例执行后的清理方法<br>
   * 停止SparkContext释放资源<br>
   */
  override def afterEach(): Unit = {
    // 停止SparkContext
    sc.stop()
  }

  /**
   * 测试：可序列化的匿名函数<br>
   * 匿名函数（lambda）可以被序列化发送到其他节点执行<br>
   */
  test("serializable anonymous function") {
    // 并行化数组创建RDD
    val params = sc.parallelize(Array(1, 2, 3))

    // 使用匿名函数作为reduce操作的处理逻辑
    // 匿名函数 (x: Int, y: Int) => x + y 可以被序列化
    val result = params.reduce((x: Int, y: Int) => x + y)

    // 断言结果为6
    assert(result === 6)
  }

  /**
   * 测试：可序列化的局部函数<br>
   * 将匿名函数赋值给val变量后仍然可以序列化<br>
   */
  test("serializable local function") {
    // 将匿名函数赋值给局部变量
    val myAddFn = (x: Int, y: Int) => x + y

    // 并行化数组创建RDD
    val params = sc.parallelize(Array(1, 2, 3))

    // 使用局部函数变量作为reduce参数
    val result = params.reduce(myAddFn)

    assert(result === 6)
  }

  /**
   * 测试：可序列化的全局单例对象方法<br>
   * 定义在object中的方法（类似静态方法）可以被序列化<br>
   */
  test("serializable global singleton function") {
    // 并行化数组创建RDD
    val params = sc.parallelize(Array(1, 2, 3))

    // 使用全局单例对象的方法作为reduce参数
    // NonSerializableSingleton.myAddFn是object中定义的方法
    val result = params.reduce(NonSerializableSingleton.myAddFn)

    assert(result === 6)
  }

  /**
   * 测试：不可序列化的类方法<br>
   * 普通类（非Serializable）的方法无法被序列化<br>
   */
  test("non-serializable class function") {
    // 创建普通类的实例（未实现Serializable）
    val nonSerializable = new NonSerializableClass()

    // 并行化数组创建RDD
    val params = sc.parallelize(Array(1, 2, 3))

    // 使用intercept捕获预期的SparkException
    // 尝试序列化nonSerializable.myAddFn时会失败
    intercept[SparkException] {
      val result = params.reduce(nonSerializable.myAddFn)
    }
  }

  /**
   * 测试：可序列化的类方法<br>
   * 继承Serializable的类其实例方法可以被序列化<br>
   */
  test("serializable class function") {
    // 创建可序列化类的实例
    val serializable = new SerializableClass()

    // 并行化数组创建RDD
    val params = sc.parallelize(Array(1, 2, 3))

    // SerializableClass继承了Serializable接口
    // 其方法可以被序列化并发送到其他节点执行
    val result = params.reduce(serializable.myAddFn)

    assert(result === 6)
  }

}

/**
 * 不可序列化的单例对象<br>
 * object中定义的函数理论上可以被序列化（作为函数引用），<br>
 * 但这里指的是NonSerializableSingleton本身作为对象时的序列化问题<br>
 */
object NonSerializableSingleton {
  /**
   * 加法函数<br>
   *<br>
   * @param x 第一个整数<br>
   * @param y 第二个整数<br>
   * @return 两个整数的和<br>
   */
  def myAddFn(x: Int, y: Int): Int = { x + y }
}

/**
 * 不可序列化的类<br>
 * 该类未实现Serializable接口，其方法无法被序列化发送到其他节点<br>
 */
class NonSerializableClass {
  /**
   * 加法方法<br>
   *<br>
   * @param x 第一个整数<br>
   * @param y 第二个整数<br>
   * @return 两个整数的和<br>
   */
  def myAddFn(x: Int, y: Int): Int = { x + y }
}

/**
 * 可序列化的类<br>
 * 继承Serializable接口后，类的方法可以被序列化<br>
 * 在Spark分布式执行时可以发送到其他节点调用<br>
 */
class SerializableClass extends Serializable {
  /**
   * 加法方法<br>
   *<br>
   * @param x 第一个整数<br>
   * @param y 第二个整数<br>
   * @return 两个整数的和<br>
   */
  def myAddFn(x: Int, y: Int): Int = { x + y }
}
