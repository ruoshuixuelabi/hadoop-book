package v2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * v2 版 Mapper 的单元测试
 *
 * <p>用 MRUnit 的 {@code MapDriver} 驱动 Mapper，
 * 逐条验证「有效记录被输出、无效记录被忽略」的行为
 * <p>与 v1 的测试相比，多了正号温度与可疑质量位两种情况
 */
public class MaxTemperatureMapperTest {

  /**
   * 验证正常的负温度记录被正确输出
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void processesValidRecord() throws IOException, InterruptedException {
    // NCDC 记录是定长文本：年份在第 15~19 位，气温在第 87~91 位
    // 这一行的年份是 1950，气温是 -11
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    // MRUnit 的 MapDriver，链式写法见 v1 的同名测试
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      // 输入的键是行号，值是整行记录
      .withInput(new LongWritable(0), value)
      // 期望输出：年份 1950 对应气温 -11
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }
  
  /**
   * 验证带正号的温度被正确解析
   *
   * <p>这正是 v1 会出错的情况：v1 直接 parseInt，遇到 '+' 会抛异常
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void processesPositiveTemperatureRecord() throws IOException,
      InterruptedException {
    // 与上一条不同，气温带正号 "+00111"——v1 在这里会抛异常
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+00111+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      // 期望输出：年份 1950 对应气温 11（正号被正确剥掉了）
      .withOutput(new Text("1950"), new IntWritable(11))
      .runTest();
  }
  
  /**
   * 验证温度缺失（9999）的记录被忽略
   *
   * <p>没有 withOutput 断言，等于断言「什么也没输出」
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException,
      InterruptedException {
    // 气温是 9999，表示「温度缺失」，应当被忽略
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .runTest();
  }
  
  /**
   * 验证质量位可疑的记录被忽略
   *
   * <p>温度是 11 度，但质量位是 2，不在可信集合 {0,1,4,5,9} 内
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void ignoresSuspectQualityRecord() throws IOException,
      InterruptedException {
    // 温度本身正常，但质量位可疑，应当被忽略
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+00112+99999999999");
                              // Temperature ^^^^^
                               // Suspect quality ^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .runTest();
  }
  
}
