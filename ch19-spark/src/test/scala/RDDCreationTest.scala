
import java.io.File

import com.google.common.io.{Resources, Files}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.DatumWriter
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import specific.WeatherRecord
/**
 * 文件：RDDCreationTest.scala<br>
 * 路径：src/test/scala/RDDCreationTest.scala<br>
 * 描述：RDD创建方式测试套件<br>
 *<br>
 * 功能说明：<br>
 * 测试Spark创建RDD（弹性分布式数据集）的多种方式，<br>
 * 包括：并行集合、文本文件、SequenceFile、Avro数据文件（Generic、Specific、Reflect三种模式）。<br>
 * 同时验证Kryo序列化器对这些数据类型的支持。<br>
 * RDD创建测试类<br>
 * 使用ScalaTest框架，测试各种RDD创建方式<br>
 */
class RDDCreationTest extends AnyFunSuite with BeforeAndAfterEach {

  // SparkContext变量
  var sc: SparkContext = _

  /**
   * 每个测试用例执行前的初始化方法<br>
   * 配置Kryo序列化器和自定义注册器<br>
   */
  override def beforeEach(): Unit = {
    // 创建Spark配置
    val conf = new SparkConf()
    // 设置使用KryoSerializer替代JavaSerializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 设置Kryo的自定义注册器，用于注册WeatherRecord等类型
    conf.set("spark.kryo.registrator", "CustomKryoRegistrator")
    // 创建local模式的SparkContext
    sc = new SparkContext("local", "test", conf)
  }

  /**
   * 每个测试用例执行后的清理方法<br>
   */
  override def afterEach(): Unit = {
    // 停止SparkContext释放资源
    sc.stop()
  }

  /**
   * 测试：并行化集合创建RDD<br>
   * 验证parallelize方法将本地集合转换为分布式RDD的能力<br>
   */
  test("parallelized collection") {
    // 定义一个计算函数（类似三角形数的计算）<br>
    // 实际应用中通常会是更耗时的计算
    val performExpensiveComputation = (x: Int) => x * (x + 1) / 2

    // 使用parallelize将本地Range集合转换为RDD
    val params = sc.parallelize(1 to 10)

    // 对RDD中的每个元素应用计算函数
    val result = params.map(performExpensiveComputation)

    // 断言：分布式计算结果与本地计算结果一致
    assert(result.collect === (1 to 10).map(performExpensiveComputation))
  }

  /**
   * 测试：文本文件创建RDD<br>
   * 验证textFile方法读取外部文本文件的能力<br>
   */
  test("text file") {
    // 创建临时文件用于测试
    val input: File = File.createTempFile("input", "")

    // 从classpath复制fruit.txt资源到临时文件
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
      input)

    // 使用textFile读取文件，每行作为RDD的一个元素
    val text: RDD[String] = sc.textFile(input.getPath)

    // 断言：验证读取的文件内容正确
    assert(text.collect().toList === List("cherry", "apple", "banana"))
  }

//  /**
//   * 测试：WholeTextFiles读取整个文件内容<br>
//   * 注释掉的测试用例，展示另一种读取文件的方式<br>
//   * 返回(filename, content)元组的RDD<br>
//   */
//  test("whole text file") {
//    val inputPath = "ch19-spark/src/test/resources/fruit.txt"
//    // wholeTextFiles将每个文件作为一个(key, value)对读取
//    // key是文件名，value是文件全部内容
//    val files: RDD[(String, String)] = sc.wholeTextFiles(inputPath)
//    assert(files.first._1.endsWith(inputPath))
//    assert(files.first._2 === "cherry\napple\nbanana\n")
//  }

  /**
   * 测试：SequenceFile（Hadoop Writable类型）<br>
   * 验证sequenceFile方法读取Hadoop SequenceFile的能力<br>
   * 使用Hadoop Writable类型（IntWritable, Text）<br>
   */
  test("sequence file writable") {
    // 创建临时文件
    val input: File = File.createTempFile("input", "")

    // 复制SequenceFile测试数据到临时文件
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("numbers.seq")),
      input)

    // 使用sequenceFile读取，指定key和value的Hadoop Writable类型
    val data = sc.sequenceFile[IntWritable, Text](input.getPath)

    // 断言：验证读取的第一个(key, value)对内容正确
    assert(data.first._1 === new IntWritable(100))
    assert(data.first._2 === new Text("One, two, buckle my shoe"))
  }

  /**
   * 测试：SequenceFile（Java原生类型）<br>
   * sequenceFile支持自动将Hadoop Writable类型转换为Java原生类型<br>
   */
  test("sequence file java") {
    // 创建临时文件
    val input: File = File.createTempFile("input", "")

    // 复制SequenceFile测试数据
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("numbers.seq")),
      input)

    // 使用sequenceFile读取，指定Java原生类型（Int, String）
    // Spark会自动进行Writable类型到原生类型的转换
    val data = sc.sequenceFile[Int, String](input.getPath)

    // 断言：验证读取结果已转换为Java原生类型
    assert(data.first._1 === 100)
    assert(data.first._2 === "One, two, buckle my shoe")
  }

  /**
   * 测试：Avro Generic数据文件<br>
   * 验证Avro GenericRecord通过AvroKeyInputFormat读取的能力<br>
   * GenericRecord是Avro的通用记录类型，不需要预定义POJO<br>
   */
  test("avro generic data file") {
    // Avro输出文件路径
    val inputPath = "target/data.avro"

    // 定义Avro schema：record类型，包含left和right两个string字段
    val avroSchema = new Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"StringPair\",\n  \"doc\": \"A pair of strings.\",\n  \"fields\": [\n    {\"name\": \"left\", \"type\": \"string\"},\n    {\"name\": \"right\", \"type\": \"string\"}\n  ]\n}")

    // 创建GenericRecord实例并填充数据
    val datum: GenericRecord = new GenericData.Record(avroSchema)
    datum.put("left", "L")
    datum.put("right", "R")

    // 创建Avro DataFileWriter写入数据文件
    val file: File = new File(inputPath)
    val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter: DataFileWriter[GenericRecord] = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(avroSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    // 创建Hadoop Job配置Avro输入
    val job = new Job()
    AvroJob.setInputKeySchema(job, avroSchema)

    // 使用newAPIHadoopFile读取Avro文件
    // AvroKeyInputFormat: Avro键的输入格式
    // AvroKey[GenericRecord]: 键的类型包装
    // NullWritable: 值类型（此例中未使用）
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[GenericRecord]],
      classOf[AvroKey[GenericRecord]], classOf[NullWritable],
      job.getConfiguration)

    // 读取第一条记录并验证内容
    val record = data.first._1.datum
    assert(record.get("left").toString === "L")
    assert(record.get("right").toString === "R")

    // 测试Avro记录是否可以正确序列化（在map操作中）
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2.get("left").toString === "L")
    assert(record2.get("right").toString === "R")
  }

  /**
   * 测试：Avro Specific数据文件<br>
   * 验证Avro SpecificRecord（代码生成的POJO）的读写能力<br>
   * SpecificRecord相比GenericRecord有更好的类型安全性和性能<br>
   */
  test("avro specific data file") {
    // Avro输出文件路径
    val inputPath = "target/data.avro"

    // 创建SpecificRecord实例（WeatherRecord是Avro根据schema生成的类）
    val datum = new WeatherRecord(2000, 10, "id")

    // 创建Avro DataFileWriter写入SpecificRecord
    val file: File = new File(inputPath)
    val writer: DatumWriter[WeatherRecord] = new SpecificDatumWriter[WeatherRecord]()
    val dataFileWriter: DataFileWriter[WeatherRecord] = new DataFileWriter[WeatherRecord](writer)
    dataFileWriter.create(WeatherRecord.getClassSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    // 配置Avro输入Job
    val job = new Job()
    AvroJob.setInputKeySchema(job, WeatherRecord.getClassSchema)

    // 使用newAPIHadoopFile读取Avro Specific文件
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[WeatherRecord]],
      classOf[AvroKey[WeatherRecord]], classOf[NullWritable],
      job.getConfiguration)

    // 读取并验证记录
    val record = data.first._1.datum
    assert(record === datum)

    // 测试SpecificRecord是否可以正确序列化
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2 === datum)
  }

  /**
   * 测试：Avro Reflect数据文件<br>
   * 验证Avro Reflect模式读写普通Java/Scala POJO的能力<br>
   * Reflect模式通过Java反射自动处理POJO与Avro类型的映射<br>
   */
  test("avro reflect data file") {
    // Avro输出文件路径
    val inputPath = "target/data.avro"

    // 创建普通POJO实例（ReflectWeatherRecord是普通类，不是Avro生成的）
    val datum: ReflectWeatherRecord = new ReflectWeatherRecord(2000, 10, "id")

    // 创建Avro DataFileWriter，使用ReflectDatumWriter处理POJO序列化
    val file: File = new File(inputPath)
    val writer: DatumWriter[ReflectWeatherRecord] = new ReflectDatumWriter[ReflectWeatherRecord]()
    val dataFileWriter: DataFileWriter[ReflectWeatherRecord] = new DataFileWriter[ReflectWeatherRecord](writer)
    // 使用ReflectData获取POJO对应的Avro schema
    dataFileWriter.create(ReflectData.get().getSchema(classOf[ReflectWeatherRecord]), file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    // 配置Avro输入Job，指定使用ReflectData模型
    val job = new Job()
    AvroJob.setDataModelClass(job, classOf[ReflectData])
    AvroJob.setInputKeySchema(job, ReflectData.get().getSchema(classOf[ReflectWeatherRecord]))

    // 使用newAPIHadoopFile读取Avro Reflect文件
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[ReflectWeatherRecord]],
      classOf[AvroKey[ReflectWeatherRecord]], classOf[NullWritable],
      job.getConfiguration)

    // 读取并验证记录
    val record = data.first._1.datum
    assert(record === datum)

    // 测试Reflect记录是否可以正确序列化
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2 === datum)
  }

}
