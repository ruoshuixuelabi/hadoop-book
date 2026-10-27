// 这个测试比较 Spark 两种序列化器对「数据类」的处理差异：
// 默认的 Java 序列化器（JDK 自带，要求类实现 Serializable）
// 与 Kryo 序列化器（Spark 通过配置项 spark.serializer 切换过去）。
// 同一个数据类在两种序列化器下往往一个能跑、一个报错，
// 搞清楚这个差别，是排查作业在集群上报序列化错误的关键。
import example.IntWrapper;
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DataSerializationTest extends FunSuite with BeforeAndAfterEach {

  // 这份配置只带序列化器一项，用在名字里带 kyro 的那批测试上；
  // 名字里带 java 的那批测试则不传这份配置，走 Spark 的默认序列化器。
  val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  var sc: SparkContext = _

  override def beforeEach() {
  }

  override def afterEach() {
    // 每个测试各自新建 SparkContext，所以必须在这里停掉它；
    // 否则上一个测试留下的线程与端口会拖慢甚至干扰后面的测试。
    sc.stop()
  }

  // 基准情形：数据类是可序列化的，两种序列化器都能跑通，结果也一样。
  test("serializable data class java") {
    sc = new SparkContext("local", "test")

    val result = sc.parallelize(Array(1, 2, 3)).reduce((x, y) => x + y)
    assert(result === 6)
  }

  test("serializable data class kyro") {
    sc = new SparkContext("local", "test", conf)

    val result = sc.parallelize(Array(1, 2, 3)).reduce((x, y) => x + y)
    assert(result === 6)
  }

  // 换成默认的 Java 序列化器后同一个类就失败了：
  // 原因在于 Spark 要把闭包送到各个执行器上去执行，而 Java 序列化要求类实现 Serializable；
  // 而 NonSerializableInt 没有实现，于是任务还没跑起来就抛 SparkException。
  test("non-serializable data class java") {
    sc = new SparkContext("local", "test")

    intercept[SparkException] {
      sc.parallelize(Array(1, 2, 3))
        .map(new NonSerializableInt(_))
        .reduce((x, y) => x + y)
        .toInt()
    }
  }

  // 换回 Kryo 就又能跑了：Kryo 不要求类实现 Serializable，
  // 它按字段反射式地读写对象，只要字段本身能处理就行。
  test("non-serializable data class kyro") {
    sc = new SparkContext("local", "test", conf)

    val result = sc.parallelize(Array(1, 2, 3))
      .map(new NonSerializableInt(_))
      .reduce((x, y) => x + y)
      .toInt()
    assert(result === 6)
  }

  // 这里说的是 Avro 的「通用」模型：GenericData.Record 本身没有实现 Serializable，
  // 所以用 Java 序列化器时同样会失败。
  test("avro generic java") {
    sc = new SparkContext("local", "test")

    intercept[SparkException] {
      sc.parallelize(Array(toGenericRecord(1), toGenericRecord(2), toGenericRecord(3)))
        .map(_.get("value").asInstanceOf[Int])
        .reduce((x, y) => x + y)
    }
  }

  test("avro generic kyro") {
    sc = new SparkContext("local", "test", conf)

    val schema = Schema.create(Schema.Type.INT)
    val result = sc.parallelize(Array(toGenericRecord(1), toGenericRecord(2), toGenericRecord(3)))
      .map(_.get("value").asInstanceOf[Int])
      .reduce((x, y) => x + y)
    assert(result === 6)
  }

  // 这里说的是 Avro 的「具体」模型：由 avsc 生成的具体类实现了 Externalizable，
  // 而 Externalizable 继承自 Serializable，所以用 Java 序列化器也能通过。
  test("avro specific java") {
    sc = new SparkContext("local", "test")

    val result = sc.parallelize(Array(new IntWrapper(1), new IntWrapper(2), new IntWrapper(3)))
      .map(_.getValue())
      .reduce((x, y) => x + y)
    assert(result === 6)
  }

  test("avro specific kyro") {
    sc = new SparkContext("local", "test", conf)

    val result = sc.parallelize(Array(new IntWrapper(1), new IntWrapper(2), new IntWrapper(3)))
      .map(_.getValue())
      .reduce((x, y) => x + y)
    assert(result === 6)
  }

  // 手工构造一条通用记录：先把 schema 解析出来，再按字段名往里塞值。
  // 通用模型的好处是不需要预先生成 Java 类，schema 变了代码也不用重编译。
  def toGenericRecord(i: Int): GenericRecord = {
    val avroSchema = new Schema.Parser().parse("{\n  \"type\": \"record\"," +
      "\n  \"name\": \"IntWrapper\"," +
      "\n  \"fields\": [\n    {\"name\": \"value\", \"type\": \"int\"}\n  ]\n}")

    val datum = new GenericData.Record(avroSchema)
    datum.put("value", i)
    datum
  }
}

// 故意不实现 Serializable 的数据类，用来演示默认 Java 序列化器的失败。
class NonSerializableInt(val value: Int) {
  def +(that: NonSerializableInt) = new NonSerializableInt(value + that.value)
  def toInt() = value
}