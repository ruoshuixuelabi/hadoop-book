package crunch;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Crunch 的数据源（Source）与数据目标（Target）测试
 *
 * <p>Crunch 把「数据从哪里来」抽象成 {@code Source}，把「数据到哪里去」抽象成
 * {@code Target}，中间的 {@code Pipeline} 负责把二者串成一张可执行的数据流图。
 *
 * <p>本测试类主要验证两件事：一方面是各种 Source（文本文件、SequenceFile）
 * 读出来的 {@code PCollection} 与 {@code PTable} 内容是否符合预期；另一方面是
 * 类型系统（Writables 与 Avros）之间的兼容性规则，例如 Writable 类型不能直接
 * 写成 Avro 文件，Avro 类型也不能直接写成 SequenceFile。
 *
 * <p>另外，文件末尾被注释掉的 testReadFromAvroFile 用例依赖一份额外的 Avro
 * 测试数据文件，仓库里没有提供，因此暂时关闭。
 */
public class SourcesAndTargetsTest implements Serializable {

  /**
   * 测试期间使用的临时目录，测试结束后会被自动清理
   *
   * <p>{@code transient} 表示这个字段不参与序列化：它只是 JUnit 的运行环境对象，
   * 只在测试进程内有意义，不能随作业被序列化到 MapReduce 任务里去。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 用快捷方法 {@code readTextFile} 读入纯文本文件
   *
   * <p>它相当于 {@code read(From.textFile(path))}：每行文本成为一个
   * {@code String} 元素，行尾换行符被去掉。这里用 {@code MemPipeline} 在内存中
   * 执行，省去启动 MapReduce 作业的开销。
   *
   * @throws IOException 复制测试资源或读取输入文件失败
   */
  @Test
  public void testReadTextFile() throws IOException {
    List<String> expectedContent = Lists.newArrayList("2", "3", "1", "3");
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    Pipeline pipeline = MemPipeline.getInstance();
    // readTextFile 返回的是惰性的 PCollection，此时数据还没有被真正读进来
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    // materialize 才真正触发读取，把结果拉回本地 JVM
    Iterable<String> materialized = lines.materialize();
    assertEquals(expectedContent, Lists.newArrayList(materialized));
    pipeline.done();
  }

  /**
   * 用显式构造的 {@code From.textFile} 数据源读取
   *
   * <p>与上一个用例读的是同一个文件，区别在于这里先把「数据源」描述成一个
   * {@code Source} 对象再交给 {@code pipeline.read}。这正是 Crunch 推荐的写法：
   * Source 只负责描述数据格式，Pipeline 负责调度与执行。
   *
   * @throws IOException 复制测试资源或读取输入文件失败
   */
  @Test
  public void testReadFromTextFile() throws IOException {
    List<String> expectedContent = Lists.newArrayList("2", "3", "1", "3");
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    Pipeline pipeline = MemPipeline.getInstance();
    // From.textFile 返回 Source，pipeline.read 接受这个 Source 并返回 PCollection
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    Iterable<String> materialized = lines.materialize();
    // materialize 出来的是 Iterable，先转成 List 才能和期望列表逐项比较
    assertEquals(expectedContent, Lists.newArrayList(materialized));
    pipeline.done();
  }

  /**
   * 读取文本文件时直接指定 Avro 类型
   *
   * <p>{@code From.textFile(path, Avros.strings())} 让这个 {@code PCollection}
   * 从一开始就是 Avro 类型，这样后面才能把它写成 Avro 文件；若不指定类型，
   * Crunch 默认按 Writable 处理，类型信息也就此确定。
   *
   * @throws IOException 复制测试资源或读取输入文件失败
   */
  @Test
  public void testReadFromTextFileAsAvro() throws IOException {
    List<String> expectedContent = Lists.newArrayList("2", "3", "1", "3");
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    Pipeline pipeline = MemPipeline.getInstance();
    // 第二个参数 Avros.strings() 声明了输出元素的类型
    PCollection<String> lines = pipeline.read(From.textFile(inputPath, Avros.strings()));
    Iterable<String> materialized = lines.materialize();
    assertEquals(expectedContent, Lists.newArrayList(materialized));
    pipeline.done();
  }

  /**
   * 从 SequenceFile 中只读取 value 部分
   *
   * <p>{@code From.sequenceFile(path, Writables.strings())} 只声明了 value 的类型，
   * key 交给 Crunch 自己推断（它假定是 {@code NullWritable}）。该用例目前被
   * {@code @Ignore} 掉：对这份测试数据而言这个假定并不成立，框架会读不出预期内容。
   *
   * @throws IOException 复制测试资源或读取输入文件失败
   */
  @Test
  @Ignore("fails since Crunch (incorrectly) assumes key is NullWritable")
  public void testReadValuesFromSequenceFile() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("numbers.seq");
    Pipeline pipeline = new MRPipeline(getClass());
    // SequenceFile 的输入格式必须跑在真正的 MapReduce 管线上，所以这里用 MRPipeline
    PCollection<String> lines = pipeline.read(From.sequenceFile(inputPath,
        Writables.strings()));
    // 只给出 value 的类型，key 类型由 Crunch 推断
    Iterable<String> materialized = lines.materialize();
    assertTrue(Lists.newArrayList(materialized).contains("One, two, buckle my shoe"));
    pipeline.done();
  }

  /**
   * 把 SequenceFile 读成带键的 {@code PTable}
   *
   * <p>与上一个用例不同，这里同时给出了 key 和 value 的类型
   * （{@code Writables.ints()} 与 {@code Writables.strings()}），所以
   * {@code pipeline.read} 返回的是 {@code PTable} 而不是 {@code PCollection}。
   *
   * @throws IOException 复制测试资源或读取输入文件失败
   */
  @Test
  public void testReadPTableFromSequenceFile() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("numbers.seq");
    Pipeline pipeline = new MRPipeline(getClass());
    TableSource<Integer, String> source =
        From.sequenceFile(inputPath, Writables.ints(), Writables.strings());
    // key 与 value 的类型都写清楚，才能得到一个真正的表
    PTable<Integer, String> table = pipeline.read(source);
    // materializeToMap 把表求值成 Java Map，方便按 key 取值断言
    Map<Integer, String> map = table.materializeToMap();
    assertEquals("Nine, ten, a big fat hen", map.get(1));
    pipeline.done();
  }

  /**
   * 直接以 Hadoop Writable 类型读取 SequenceFile
   *
   * <p>{@code From.sequenceFile(path, IntWritable.class, Text.class)} 不做任何
   * 类型转换，读出来的键值就是 Hadoop 原生的 Writable 对象；与前一个用例的
   * {@code Writables.ints()} 相比，少了一层「Java 类型 ↔ Writable」的包装。
   *
   * @throws IOException 复制测试资源或读取输入文件失败
   */
  @Test
  public void testReadPTableFromSequenceFileAsWritables() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("numbers.seq");
    Pipeline pipeline = new MRPipeline(getClass());
    // 直接给出 Writable 类，Crunch 不再负责挑选包装类型
    TableSource<IntWritable, Text> source =
        From.sequenceFile(inputPath, IntWritable.class, Text.class);
    PTable<IntWritable, Text> table = pipeline.read(source);
    Map<IntWritable, Text> map = table.materializeToMap();
    // Text 的 equals 按内容比较，所以可以直接用 new Text(...) 作为期望值
    assertEquals(new Text("Nine, ten, a big fat hen"), map.get(new IntWritable(1)));
    pipeline.done();
  }

//  @Test
//  public void testReadFromAvroFile() throws IOException {
//    String inputPath = tmpDir.copyResourceFileName("weather.avro");
//    Pipeline pipeline = new MRPipeline(getClass());
//    Source<WeatherRecord> source =
//        From.avroFile(inputPath, Avros.specifics(WeatherRecord.class));
//    PCollection<WeatherRecord> records = pipeline.read(source);
//    assertEquals((Long) 10L, records.length().getValue());
//    pipeline.done();
//  }

  /**
   * 用 Writable 类型写 Avro 文件应当失败
   *
   * <p>Crunch 要求写出 Avro 文件的集合本身就是 Avro 类型；用默认的 Writable 类型
   * 去写时，框架在建立输出阶段就会抛出
   * {@link org.apache.crunch.CrunchRuntimeException}。
   * 用 {@code expected} 断言异常类型，说明这种失败是被设计出来的、可预期的。
   *
   * @throws IOException 复制测试资源失败
   */
  @Test(expected = CrunchRuntimeException.class)
  public void testWriteWritablesToAvroFileFails() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    String outputPath = tmpDir.getFileName("out");
    Pipeline pipeline = new MRPipeline(getClass());
    PCollection<String> lines = pipeline.read(From.textFile(inputPath));
    // 关键的一行：类型不兼容的错误就是在这次 write 规划时暴露出来的
    lines.write(To.avroFile(outputPath));
    pipeline.done();
  }

  /**
   * 在内存管道中把 {@code PTable} 写成 Avro 文件
   *
   * <p>{@code typedCollectionOf} 直接造出一个带 Avro 类型的集合；{@code by} 把它
   * 转成以「字符串长度」为键的 {@code PTable}；最后写入 Avro 文件，
   * {@code Target.WriteMode.OVERWRITE} 表示允许覆盖已存在的输出目录。
   *
   * @throws IOException 写出 Avro 文件失败
   */
  @Test
  public void testWritePTableToAvroFileInMem() throws IOException {
    // 造一个内容为 2、3、1、3 的字符串集合，并指定它使用 Avro 类型
    PCollection<String> lines = MemPipeline.typedCollectionOf(Avros.strings(), "2", "3", "1", "3");
    // by 的第一个参数是 MapFn（把每个元素映射成一个键），第二个参数声明键的类型
    PTable<Integer, String> table = lines.by(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        return input.length();
      }
    }, Avros.ints());
    // 表本身是 Avro 类型（值来自 Avros.strings()、键来自 Avros.ints()），所以能写 Avro 文件
    table.write(To.avroFile("/tmp/out"), Target.WriteMode.OVERWRITE);
    lines.getPipeline().done();
  }

  /**
   * 在 MapReduce 管道中把 {@code PTable} 写成 Avro 文件
   *
   * <p>与内存版本相比，输入来自真实文件、输出也落到真实路径，走的是完整的
   * {@code MRPipeline}，会真正提交 MapReduce 作业；读取时就指定
   * {@code Avros.strings()}，是让后面能写 Avro 文件的前提。
   *
   * @throws IOException 写出 Avro 文件失败
   */
  @Test
  public void testWritePTableToAvroFile() throws IOException {
    String inputPath = tmpDir.copyResourceFileName("ints.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    // 读文本时就把类型定成 Avro，后面写 Avro 文件才不会有类型冲突
    PCollection<String> lines = pipeline.read(From.textFile(inputPath, Avros.strings()));
    PTable<Integer, String> table = lines.by(new MapFn<String, Integer>() {
      @Override
      public Integer map(String input) {
        return input.length();
      }
    }, Avros.ints());
    // OVERWRITE 允许覆盖上一次的输出目录，使本用例可以重复执行
    table.write(To.avroFile("/tmp/out2"), Target.WriteMode.OVERWRITE);
    pipeline.done();
  }

}
