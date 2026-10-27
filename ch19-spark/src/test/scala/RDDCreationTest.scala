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
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import specific.WeatherRecord

// 这个测试演示从各种外部数据源创建 RDD 的几种方式：
// 内存集合、文本文件、Hadoop 的 SequenceFile，以及 Avro 数据文件。
// 重点是看清每种输入格式对应的 RDD 元素类型是什么，
// 因为类型决定了后面能对它做哪些操作。
class RDDCreationTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach() {
    val conf = new SparkConf()
    // 这个测试把序列化器切到 Kryo，并额外挂上自定义注册器，
    // 用来说明这两项配置怎么写；与 DataSerializationTest 里对比两种序列化器的做法相呼应。
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 注册器是按类名字符串反射加载的，所以这里填的是类名，
    // 对应的是 src/test/scala/CustomKryoRegistrator.scala。
    conf.set("spark.kryo.registrator", "CustomKryoRegistrator")
    sc = new SparkContext("local", "test", conf)
  }

  override def afterEach() {
    sc.stop()
  }

  test("parallelized collection") {
    // triangle numbers - but would normally be more compute intensive
    val performExpensiveComputation = (x: Int) => x * (x + 1) / 2

    // 方法 parallelize 把本地集合直接变成 RDD，默认按并行度切成若干个分区。
    val params = sc.parallelize(1 to 10)
    val result = params.map(performExpensiveComputation)

    assert(result.collect === (1 to 10).map(performExpensiveComputation))
  }

  test("text file") {
    // 测试资源在 classpath 上，Spark 只能直接读文件系统路径，
    // 所以先复制到一个临时文件，再把临时文件路径交给 textFile。
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
      input)
    // 这里 textFile 每一行是一个元素，元素类型是 String。
    val text: RDD[String] = sc.textFile(input.getPath)
    assert(text.collect().toList === List("cherry", "apple", "banana"))
  }

//  test("whole text file") {
//    val inputPath = "ch19-spark/src/test/resources/fruit.txt"
//    val files: RDD[(String, String)] = sc.wholeTextFiles(inputPath)
//    assert(files.first._1.endsWith(inputPath))
//    assert(files.first._2 === "cherry\napple\nbanana\n")
//  }

  test("sequence file writable") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("numbers.seq")),
      input)
    // 这种 SequenceFile 是 Hadoop 原生的二进制格式，能存任意类型；
    // 这里声明成 [IntWritable, Text]，所以拿到的键值都是 Hadoop 的 Writable 包装类。
    val data = sc.sequenceFile[IntWritable, Text](input.getPath)
    assert(data.first._1 === new IntWritable(100))
    assert(data.first._2 === new Text("One, two, buckle my shoe"))
  }

  test("sequence file java") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("numbers.seq")),
      input)
    // 同一个文件也可以按 Java 类型来读，Spark 会在底层做包装与拆包；
    // 好处是处理数据时不必一直和 Writable 类打交道。
    val data = sc.sequenceFile[Int, String](input.getPath)
    assert(data.first._1 === 100)
    assert(data.first._2 === "One, two, buckle my shoe")
  }

  test("avro generic data file") {
    // 三个 Avro 测试都写同一个文件名，所以它们不能同时跑，
    // 这里每个测试写完立刻读回，读的就是自己刚写的内容。
    val inputPath = "target/data.avro"
    val avroSchema = new Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"StringPair\",\n  \"doc\": \"A pair of strings.\",\n  \"fields\": [\n    {\"name\": \"left\", \"type\": \"string\"},\n    {\"name\": \"right\", \"type\": \"string\"}\n  ]\n}")

    // 通用模型：数据以 GenericRecord 的形式存在，字段按名字存取。
    val datum: GenericRecord = new GenericData.Record(avroSchema)
    datum.put("left", "L")
    datum.put("right", "R")

    val file: File = new File(inputPath)
    val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter: DataFileWriter[GenericRecord] = new DataFileWriter[GenericRecord](writer)
    // 写入必须先 create 之后才能 append，最后必须 close，
    // 否则文件尾部的元数据写不出去，读回来的文件就是残缺的。
    dataFileWriter.create(avroSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    val job = new Job()
    // 必须把 schema 告诉 Avro 的 InputFormat，否则它不知道该怎么切分与反序列化。
    AvroJob.setInputKeySchema(job, avroSchema)
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[GenericRecord]],
      classOf[AvroKey[GenericRecord]], classOf[NullWritable],
      job.getConfiguration)

    // 这个 AvroKey 是个包装类，真正的数据要用 datum() 这个访问器才取得到。
    val record = data.first._1.datum
    assert(record.get("left").toString === "L")
    assert(record.get("right").toString === "R")

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2.get("left").toString === "L")
    assert(record2.get("right").toString === "R")
  }

  test("avro specific data file") {
    val inputPath = "target/data.avro"

    // 具体模型：直接用 avsc 生成出来的强类型类，字段是方法而不是字符串键。
    val datum = new WeatherRecord(2000, 10, "id")

    val file: File = new File(inputPath)
    val writer: DatumWriter[WeatherRecord] = new SpecificDatumWriter[WeatherRecord]()
    val dataFileWriter: DataFileWriter[WeatherRecord] = new DataFileWriter[WeatherRecord](writer)
    dataFileWriter.create(WeatherRecord.getClassSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    val job = new Job()
    // 具体类的 schema 已经编译进类里，直接取静态方法即可。
    AvroJob.setInputKeySchema(job, WeatherRecord.getClassSchema)
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[WeatherRecord]],
      classOf[AvroKey[WeatherRecord]], classOf[NullWritable],
      job.getConfiguration)

    val record = data.first._1.datum
    assert(record === datum)

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2 === datum)
  }

  test("avro reflect data file") {
    val inputPath = "target/data.avro"

    // 反射模型：直接拿一个普通的 Scala 类当数据结构，不生成任何专门的类。
    val datum: ReflectWeatherRecord = new ReflectWeatherRecord(2000, 10, "id")

    val file: File = new File(inputPath)
    val writer: DatumWriter[ReflectWeatherRecord] = new ReflectDatumWriter[ReflectWeatherRecord]()
    val dataFileWriter: DataFileWriter[ReflectWeatherRecord] = new DataFileWriter[ReflectWeatherRecord](writer)
    // 反射模型没有内置的 schema，要向 ReflectData 现问一个。
    dataFileWriter.create(ReflectData.get().getSchema(classOf[ReflectWeatherRecord]), file)
    dataFileWriter.append(datum)
    dataFileWriter.close

    val job = new Job()
    // 反射模型还必须告诉 Avro 用哪个数据模型，否则默认按通用模型处理。
    AvroJob.setDataModelClass(job, classOf[ReflectData])
    AvroJob.setInputKeySchema(job, ReflectData.get().getSchema(classOf[ReflectWeatherRecord]))
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[ReflectWeatherRecord]],
      classOf[AvroKey[ReflectWeatherRecord]], classOf[NullWritable],
      job.getConfiguration)

    val record = data.first._1.datum
    assert(record === datum)

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2 === datum)
  }

}
