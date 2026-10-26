package v1;
// cc MaxTemperatureMapperTestV1 Unit test for MaxTemperatureMapper
// == MaxTemperatureMapperTestV1Missing
// vv MaxTemperatureMapperTestV1
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

/**
 * v1 版 Mapper 的单元测试
 *
 * <p>用 MRUnit 的 {@code MapDriver} 驱动 Mapper：
 * 喂进一条输入记录，再断言它写出的键值对
 * <p>其中「缺失温度」那条用例标了 {@code @Ignore}——
 * 因为 v1 恰恰处理不了这种情况，这条失败的测试被保留下来
 * 作为后续版本的改进目标
 */
public class MaxTemperatureMapperTest {

  /**
   * 验证正常记录被解析成「年份 + 气温」
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void processesValidRecord() throws IOException, InterruptedException {
    // NCDC 记录是定长文本：年份固定在第 15~19 位，气温在第 87~91 位
    // 这一行的年份是 1950，气温是 -11
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    // MRUnit 的 MapDriver：把 Mapper 包起来，喂输入、收输出
    // withMapper 指定被测的 Mapper，withInput 给一条输入，
    // withOutput 声明期望的输出，runTest 才真正执行
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      // 输入的键是行号，值是整行记录
      .withInput(new LongWritable(0), value)
      // 期望输出：年份 1950 对应气温 -11
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }
// ^^ MaxTemperatureMapperTestV1
  // 这条例子里故意保留一个「失败」的测试：
  // v1 还没做校验，遇到 9999 会直接当成正常温度输出
  @Ignore // since we are showing a failing test in the book
// vv MaxTemperatureMapperTestV1Missing
  /**
   * 验证缺失温度的记录应当被忽略
   *
   * <p>这条测试目前是失败的：v1 的 Mapper 不做校验，
   * 会把 9999 当成正常温度输出。
   * <p>作者故意用 {@code @Ignore} 把它挂起，
   * 作为后续版本的改进目标
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException,
      InterruptedException {
    // 同一条记录，只是气温换成了 9999，表示「温度缺失」
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    // 这里没有 withOutput，等于断言「什么也没输出」
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      // 喂进去的是温度缺失的那条记录
      .withInput(new LongWritable(0), value)
      .runTest();
  }
// ^^ MaxTemperatureMapperTestV1Missing
  /**
   * 验证「数值离谱但格式合法」的记录也能照常处理
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void processesMalformedTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                  // Year ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                              // Temperature ^^^^^
    // 这条记录的年份是 1957，而它的气温恰好也是 1957
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      // 喂进去的是「格式合法但数值离谱」的那条记录
      .withInput(new LongWritable(0), value)
      // 期望输出：年份 1957 对应气温 1957
      .withOutput(new Text("1957"), new IntWritable(1957))
      .runTest();
  }
// vv MaxTemperatureMapperTestV1
}
// ^^ MaxTemperatureMapperTestV1
