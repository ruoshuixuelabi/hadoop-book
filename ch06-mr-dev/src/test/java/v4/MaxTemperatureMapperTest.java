package v4;
// == MaxTemperatureMapperTestV4Malformed
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * v4 版 Mapper 的单元测试
 *
 * <p>除了验证正常输出和缺失温度被忽略之外，
 * 重点验证了 v4 新增的能力：识别并统计「格式损坏」的记录
 * <p>做法是构造一个空的 {@code Counters} 传给 {@code MapDriver}，
 * 跑完作业后再从里面查计数器的值
 */
public class MaxTemperatureMapperTest {

  /**
   * 验证正常的负温度记录被正确输出
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void parsesValidRecord() throws IOException, InterruptedException {
    // NCDC 记录是定长文本：年份在第 15~19 位，气温在第 87~91 位
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
   * 验证温度缺失（9999）的记录被忽略
   *
   * <p>注意：温度缺失并不算「损坏」，所以计数器不增加
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void parsesMissingTemperature() throws IOException,
      InterruptedException {
    // 气温是 9999，表示「温度缺失」
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .runTest();
  }
//vv MaxTemperatureMapperTestV4Malformed
  /**
   * 验证格式损坏的记录被识别，并由计数器记上一笔
   *
   * <p>这条记录的温度位置既不是 '+' 也不是 '-'，
   * 因此解析器把它判为「损坏」
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void parsesMalformedTemperature() throws IOException,
      InterruptedException {
    // 这条记录的温度位置（第 87~91 位）不是 '+' 或 '-'，属于格式损坏
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                  // Year ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                              // Temperature ^^^^^
    // 建一个空的计数器集合交给 MapDriver，跑完后从中查值
    Counters counters = new Counters();
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      // 把计数器交给 Driver，作业运行时的计数会被收集进来
      .withCounters(counters)
      .runTest();
    // 按枚举分组取出「格式损坏」这一类的计数
    Counter c = counters.findCounter(MaxTemperatureMapper.Temperature.MALFORMED);
    // 应当正好记到 1 条损坏记录
    assertThat(c.getValue(), is(1L));
  }
// ^^ MaxTemperatureMapperTestV4Malformed
}
