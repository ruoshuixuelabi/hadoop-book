package crunch;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Tuple3;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;

import static crunch.PCollections.dump;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Crunch 类型系统（Type System）的测试
 *
 * <p>Crunch 用 {@code PType} 描述一个集合里元素是什么类型，
 * 这个类型决定了数据能怎么存、怎么取、能不能写进 Avro 文件或 SequenceFile。
 * 本测试类围绕两条主线展开：
 *
 * <p>第一条是 Avro 反射类型（{@code Avros.records}）的读写与排序，
 * 也就是 {@code WeatherRecord} 这种 POJO 如何被映射成 Avro 记录、
 * 以及排序时如何指定次级排序键。
 *
 * <p>第二条是 Avro 类型与 Hadoop Writable 类型之间的互斥关系：
 * Crunch 不允许把 Writable 集合直接写成 Avro 文件，
 * 也不允许把 Avro 集合直接写成 SequenceFile，
 * 要跨类型必须先经过一次显式转换。
 */
public class TypesTest implements Serializable {

  /**
   * 测试期间使用的临时目录，测试结束后会被自动清理
   *
   * <p>{@code transient} 表示这个字段不参与序列化：它只是 JUnit 的运行环境对象，
   * 只在测试进程内有意义，不能随作业被序列化到 MapReduce 任务里去。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 用 Avro 反射类型读取天气记录，并按「年份 + 气温」排序
   *
   * <p>流程分成四步：先把文本行用 {@code DoFn} 解析成 {@code WeatherRecord}；
   * 再用 {@code by} 把每条记录映射成键为 {@code Pair(年份, 气温)} 的表；
   * 然后用 {@code Sort.sort} 对这张表排序（键是 {@code Pair}，按分量依次比较，
   * 也就是先比年份、同年再比气温）；最后取回 value 部分并断言顺序。
   *
   * @throws Exception 解析、排序或作业执行失败
   */
  @Test
  public void testAvroReflect() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("sample.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    // DoFn 的第二个参数 Avros.records(...) 声明输出的类型：
    // Crunch 依据这个类反射生成 Avro Schema，不需要手写 .avsc
    PCollection<WeatherRecord> records = lines.parallelDo(
        new DoFn<String, WeatherRecord>() {
      // 解析器复用同一个实例：它是有状态的，parse 会覆盖上一次的结果
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<WeatherRecord> emitter) {
        parser.parse(input);
        // 温度缺失（9999）或格式非法的记录直接丢弃，不 emit 任何结果
        if (parser.isValidTemperature()) {
          emitter.emit(new WeatherRecord(parser.getYearInt(),
              parser.getAirTemperature(), parser.getStationId()));
        }
      }
    }, Avros.records(WeatherRecord.class));

    // by 把集合变成表：键是 (年份, 气温) 的 Pair，值仍是原来的记录
    // 键的类型也要用 Avro 描述：Pair 的两个分量分别声明
    PTable<Pair<Integer, Integer>, WeatherRecord> table = records.by(
        new MapFn<WeatherRecord, Pair<Integer, Integer>>() {
      @Override
      public Pair<Integer, Integer> map(WeatherRecord input) {
        return Pair.of(input.getYear(), input.getTemperature());
      }
    }, Avros.pairs(Avros.ints(), Avros.ints()));

    // Sort.sort 对表按键排序；键是 Pair，比较时先比分量再比第二个分量
    PCollection<WeatherRecord> sortedRecords = Sort.sort(table).values();

    // materialize 触发真正的 MapReduce 作业，并把排序后的结果取回本地
    Iterable<WeatherRecord> materialized = sortedRecords.materialize();

    // 期望顺序：先按年份升序，同一年内再按气温升序
    List<WeatherRecord> expectedContent = Lists.newArrayList(
        new WeatherRecord(1949, 78, "012650-99999"),
        new WeatherRecord(1949, 111, "012650-99999"),
        new WeatherRecord(1950, -11, "011990-99999"),
        new WeatherRecord(1950, 0, "011990-99999"),
        new WeatherRecord(1950, 22, "011990-99999")
    );
    assertEquals(expectedContent, Lists.newArrayList(materialized));

    pipeline.done();
  }

  /**
   * 次级排序：年份升序、同年气温降序
   *
   * <p>Crunch 的表在 MapReduce 上默认只能按一个键排序。要做「次级排序」，
   * 做法是把多个排序字段都塞进键里：这里先把每条记录变成
   * {@code Tuple3(年份, 气温, 记录)}，再用 {@code Sort.sortTriples} 指定
   * 第 1 列升序、第 2 列降序，最后把第三列（真正的记录）取回来。
   *
   * @throws Exception 解析、排序或作业执行失败
   */
  @Test
  public void testAvroReflectSecondarySort() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("sample.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    PCollection<WeatherRecord> records = lines.parallelDo(
        new DoFn<String, WeatherRecord>() {
          NcdcRecordParser parser = new NcdcRecordParser();
          @Override
          public void process(String input, Emitter<WeatherRecord> emitter) {
            parser.parse(input);
            if (parser.isValidTemperature()) {
              emitter.emit(new WeatherRecord(parser.getYearInt(),
                  parser.getAirTemperature(), parser.getStationId()));
            }
          }
        }, Avros.records(WeatherRecord.class));

    // 把排序所需的多个字段打包成一个 Tuple3，排序键就是它的前两列
    PCollection<Tuple3<Integer, Integer, WeatherRecord>> triples = records.parallelDo(
        new DoFn<WeatherRecord, Tuple3<Integer, Integer, WeatherRecord>>() {
          @Override
          public void process(WeatherRecord input,
              Emitter<Tuple3<Integer, Integer, WeatherRecord>> emitter) {
            emitter.emit(Tuple3.of(input.getYear(), input.getTemperature(), input));
          }
        }, Avros.triples(Avros.ints(), Avros.ints(), Avros.records(WeatherRecord.class))
    );

    PCollection<Tuple3<Integer, Integer, WeatherRecord>> sorted = Sort
        // sortTriples 的列序号从 1 开始：第 1 列年份升序，第 2 列气温降序
        .sortTriples(triples, Sort.ColumnOrder.by(1, Sort.Order.ASCENDING),
            Sort.ColumnOrder.by(2, Sort.Order.DESCENDING));
    // 排序后只保留第三列（原始记录），把 Tuple3 还原回 WeatherRecord
    PCollection<WeatherRecord> sortedRecords = sorted.parallelDo(new DoFn<Tuple3<Integer,
        Integer, WeatherRecord>, WeatherRecord>() {
      @Override
      public void process(Tuple3<Integer, Integer, WeatherRecord> input,
          Emitter<WeatherRecord> emitter) {
        emitter.emit(input.third());
      }
    }, Avros.records(WeatherRecord.class));

    // materialize 触发真正的 MapReduce 作业，并把排序后的结果取回本地
    Iterable<WeatherRecord> materialized = sortedRecords.materialize();

    // 期望顺序：同年份内气温从高到低，与上一个用例正好相反
    List<WeatherRecord> expectedContent = Lists.newArrayList(
        new WeatherRecord(1949, 111, "012650-99999"),
        new WeatherRecord(1949, 78, "012650-99999"),
        new WeatherRecord(1950, 22, "011990-99999"),
        new WeatherRecord(1950, 0, "011990-99999"),
        new WeatherRecord(1950, -11, "011990-99999")
    );
    assertEquals(expectedContent, Lists.newArrayList(materialized));

    pipeline.done();
  }

  /**
   * Writable 类型的集合不能直接写成 Avro 文件
   *
   * <p>Crunch 在建立输出阶段时就要为集合的 {@code PType} 找一个 Avro 序列化器，
   * 而 {@code WritableType} 根本没有对应的序列化器，
   * 于是抛出 {@link org.apache.crunch.CrunchRuntimeException}。
   *
   * @throws Exception 复制测试资源失败
   */
  @Test(expected = CrunchRuntimeException.class)
  public void testWriteWritableToAvroFileFails() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> a = pipeline.read(From.textFile(inputPath));
    // 这里没指定 Avro 类型，所以集合是 Writable 家族，写 Avro 文件时类型冲突
    a.write(To.avroFile(outputPath)); // fails with cannot serialize PType of class: class org.apache.crunch.types.writable.WritableType
    pipeline.done();
  }

  /**
   * Avro 类型的集合不能直接写成 SequenceFile
   *
   * <p>SequenceFile 的 key 必须是能被 Hadoop 序列化的 Writable，
   * 而 Avro 集合的 key 类型是 {@code org.apache.avro.mapred.AvroKey}，
   * 找不到对应的序列化器。这个失败不会抛异常，而是让作业运行失败，
   * 所以这里检查 {@code result.succeeded()} 为假。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testWriteAvroToSequenceFileFails() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    // 读的时候指定了 Avro 类型，于是集合的 key 类型是 AvroKey，SequenceFile 不认它
    PCollection<String> a = pipeline.read(From.textFile(inputPath, Avros.strings()));
    a.write(To.sequenceFile(outputPath)); // fails with Could not find a serializer for the Key class: 'org.apache.avro.mapred.AvroKey'.
    // done() 返回 PipelineResult，用它判断作业是否成功
    PipelineResult result = pipeline.done();
    assertFalse(result.succeeded());
  }

  /**
   * 把 Writable 类型的集合转成 Avro 类型后再写出
   *
   * <p>{@code IdentityFn} 什么也不做，它的作用只是「换一层类型」：
   * {@code parallelDo} 的第二个参数 {@code Avros.strings()} 给结果集合打上 Avro 标记，
   * 于是这个集合就能合法地写成 Avro 文件了。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testConvertWritableToAvro() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> a = pipeline.read(From.textFile(inputPath));
    // 数据原封不动，只是输出集合的类型从 Writable 换成了 Avro
    PCollection<String> b = a.parallelDo(IdentityFn.<String>getInstance(), Avros.strings());
    b.write(To.avroFile(outputPath));
    pipeline.done();
  }

  /**
   * 文本写出 SequenceFile，再读回来验证内容
   *
   * <p>这个用例走了一个来回：先把纯文本写成 SequenceFile，
   * 再用 {@code From.sequenceFile} 把同一个文件读成 {@code PTable}，
   * 取其中的值与原始内容比对。SequenceFile 里的 key 是 {@code NullWritable}，
   * 也就是「只有值没有键」的序列文件格式。
   *
   * @throws Exception 复制测试资源或作业执行失败
   */
  @Test
  public void testWriteTextToSequenceFile() throws Exception {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    // 不指定 Avro 类型时，读到的集合属于 Writable 类型族
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    assertEquals(WritableTypeFamily.getInstance(), lines.getPType().getFamily());

    String outputPath = tmpDir.getFileName("out");
    // Writable 集合可以写 SequenceFile，因为 SequenceFile 本来就是 Hadoop 的格式
    lines.write(To.sequenceFile(outputPath));
    pipeline.done();

    Pipeline pipeline2 = new MRPipeline(getClass());
    // 读回来时 key 用 NullWritable，值用 Text，与写出时的格式对应
    PTable<NullWritable, Text> d = pipeline2.read(From.sequenceFile(outputPath,
        NullWritable.class, Text.class));
    // values() 丢掉空键，只留下文本内容
    PCollection<Text> e = d.values();
    assertEquals("{2,3,1,3}", dump(e));

    pipeline2.done();
  }

}
