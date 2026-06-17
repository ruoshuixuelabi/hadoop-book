/**
 * 文件：DataSerializationTest.scala<br>
 * 路径：src/test/scala/DataSerializationTest.scala<br>
 * 描述：Spark序列化机制测试套件<br>
 *<br>
 * 功能说明：<br>
 * 测试Spark在不同序列化器（Java默认序列化器 vs Kryo序列化器）下，<br>
 * 对可序列化类、不可序列化类、Avro GenericRecord、Avro SpecificRecord的处理能力。<br>
 * 验证Kryo序列化器能够正确处理Java序列化无法处理的某些类型。<br>
 */
import example.IntWrapper;
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
 * 数据序列化测试类<br>
 * 使用ScalaTest框架，继承AnyFunSuite特质提供测试用例编写能力，<br>
 * 混入BeforeAndAfterEach在每个测试前后执行生命周期方法<br>
 */
class DataSerializationTest extends AnyFunSuite with BeforeAndAfterEach {

  /**
   * Kryo序列化器配置<br>
   * 设置spark.serializer为KryoSerializer替代默认的JavaSerializer<br>
   * Kryo序列化速度更快、生成体积更小，且支持一些JavaSerializer不支持的类型<br>
   */
  val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  // SparkContext变量，用于在测试中与Spark交互
  var sc: SparkContext = _

  /**
   * 每个测试用例执行前的初始化方法<br>
   * 当前实现为空，等待后续扩展<br>
   */
  override def beforeEach(): Unit = {
  }

  /**
   * 每个测试用例执行后的清理方法<br>
   * 停止SparkContext释放资源，避免上下文泄露<br>
   */
  override def afterEach(): Unit = {
    // 停止SparkContext，释放连接和资源
    sc.stop()
  }

  /**
   * 测试：可序列化数据类（Java序列化器）<br>
   * 验证基本的Int类型在分布式环境下的reduce操作<br>
   */
  test("serializable data class java") {
    // 使用默认的Java序列化器创建SparkContext（local模式）
    sc = new SparkContext("local", "test")

    // 并行化数组创建RDD，执行reduce操作求和
    val result = sc.parallelize(Array(1, 2, 3)).reduce((x, y) => x + y)

    // 断言结果为6（1+2+3=6）
    assert(result === 6)
  }

  /**
   * 测试：可序列化数据类（Kryo序列化器）<br>
   * 验证Kryo序列化器处理基本类型的能力<br>
   */
  test("serializable data class kyro") {
    // 使用Kryo序列化器配置创建SparkContext
    sc = new SparkContext("local", "test", conf)

    // 并行化数组创建RDD，执行reduce操作求和
    val result = sc.parallelize(Array(1, 2, 3)).reduce((x, y) => x + y)

    // 断言结果为6
    assert(result === 6)
  }

  /**
   * 测试：不可序列化数据类（Java序列化器）<br>
   * 验证JavaSerializer无法序列化NonSerializableInt，抛出SparkException<br>
   */
  test("non-serializable data class java") {
    // 使用默认Java序列化器
    sc = new SparkContext("local", "test")

    // 使用intercept捕获预期的SparkException
    // NonSerializableInt未实现Serializable接口，JavaSerializer无法处理
    try {
      sc.parallelize(Array(1, 2, 3))
        .map(new NonSerializableInt(_))
        .reduce((x, y) => x + y)
        .toInt()
      assert(false, "Expected SparkException was not thrown")
    } catch {
      case _: SparkException => // Expected
    }
  }

  /**
   * 测试：不可序列化数据类（Kryo序列化器）<br>
   * 验证Kryo能够序列化NonSerializableInt（通过反射或无参构造器）<br>
   */
  test("non-serializable data class kyro") {
    // 使用Kryo序列化器
    sc = new SparkContext("local", "test", conf)

    // Kryo可以通过反射创建对象实例，即使类未实现Serializable
    val result = sc.parallelize(Array(1, 2, 3))
      .map(new NonSerializableInt(_))
      .reduce((x, y) => x + y)
      .toInt()

    // 断言结果为6
    assert(result === 6)
  }

  /**
   * 测试：Avro GenericRecord（Java序列化器）<br>
   * 验证JavaSerializer无法处理Avro GenericRecord类型<br>
   */
  test("avro generic java") {
    sc = new SparkContext("local", "test")

    // 使用intercept捕获预期的SparkException
    // GenericRecord是Avro的通用记录类型，未实现Serializable
    try {
      val records = Array(toGenericRecord(1), toGenericRecord(2), toGenericRecord(3))
      sc.parallelize(records)
        .map(_.get("value").asInstanceOf[Int])
        .reduce((x, y) => x + y)
      assert(false, "Expected SparkException was not thrown")
    } catch {
      case _: SparkException => // Expected
    }
  }

  /**
   * 测试：Avro GenericRecord（Kryo序列化器）<br>
   * 验证Kryo可以正确序列化GenericRecord<br>
   */
  test("avro generic kyro") {
    sc = new SparkContext("local", "test", conf)

    // 定义Avro schema：类型为record，名字为IntWrapper，包含一个int类型的value字段
    val schema = Schema.create(Schema.Type.INT)

    // 并行化GenericRecord数组，提取value并求和
    val records: Array[GenericRecord] = Array(toGenericRecord(1), toGenericRecord(2), toGenericRecord(3))
    val result = sc.parallelize(records)
      .map(_.get("value").asInstanceOf[Int])
      .reduce((x, y) => x + y)

    assert(result === 6)
  }

  /**
   * 测试：Avro SpecificRecord（Java序列化器）<br>
   * 验证JavaSerializer无法处理SpecificRecord类型<br>
   */
  test("avro specific java") {
    sc = new SparkContext("local", "test")

    // 使用intercept捕获预期的SparkException
    // IntWrapper是Avro生成的SpecificRecord，虽然有getValue方法但仍无法被JavaSerializer处理
    try {
      val wrappers: Array[IntWrapper] = Array(new IntWrapper(1), new IntWrapper(2), new IntWrapper(3))
      sc.parallelize(wrappers)
        .map(_.getValue())
        .reduce((x, y) => x + y)
      assert(false, "Expected SparkException was not thrown")
    } catch {
      case _: SparkException => // Expected
    }
  }

  /**
   * 测试：Avro SpecificRecord（Kryo序列化器）<br>
   * 验证Kryo可以正确序列化SpecificRecord<br>
   */
  test("avro specific kyro") {
    sc = new SparkContext("local", "test", conf)

    // 并行化IntWrapper数组，调用getValue()提取值并求和
    val wrappers: Array[IntWrapper] = Array(new IntWrapper(1), new IntWrapper(2), new IntWrapper(3))
    val result = sc.parallelize(wrappers)
      .map(_.getValue())
      .reduce((x, y) => x + y)

    assert(result === 6)
  }

  /**
   * 将Int转换为Avro GenericRecord<br>
   *<br>
   * @param i 要转换的整数值<br>
   * @return 包含value字段的GenericRecord实例<br>
   */
  def toGenericRecord(i: Int): GenericRecord = {
    // 定义Avro schema
    val avroSchema = Schema.createRecord("IntWrapper", null, null, false)
    avroSchema.addProp("type", "record")
    val field = new org.apache.avro.Schema.Field("value", Schema.create(Schema.Type.INT), null, null)
    avroSchema.setFields(java.util.Collections.singletonList(field))

    // 创建GenericData.Record实例
    val datum = new GenericData.Record(avroSchema)
    datum.put("value", i)
    datum
  }
}

/**
 * 不可序列化的Int包装类<br>
 * 该类未实现Serializable接口，JavaSerializer无法处理<br>
 * 但Kryo可以通过反射方式创建其实例<br>
 *
 * @param value 封装的整数值
 */
class NonSerializableInt(val value: Int) {
  // 重载+运算符，实现与另一个NonSerializableInt的相加
  def +(that: NonSerializableInt) = new NonSerializableInt(value + that.value)
  // 返回封装的整数值
  def toInt() = value
}
