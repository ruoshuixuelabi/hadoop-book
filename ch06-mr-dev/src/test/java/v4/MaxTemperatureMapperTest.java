package v4;
// == MaxTemperatureMapperTestV4Malformed

/**
 * MaxTemperatureMapper（v4版本）单元测试
 *
 * 测试v4版本的增强功能，包括：
 * - 处理有效记录
 * - 处理缺失温度记录
 * - 处理格式错误的温度记录并使用Counter统计
 *
 * v4版本增强点：
 * - 使用增强版NcdcRecordParser
 * - 能检测格式错误的温度（isMalformedTemperature()）
 * - 通过Counter统计MALFORMED记录数
 *
 * @see MaxTemperatureMapper
 * @see NcdcRecordParser
 */
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThat;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.jupiter.api.Test;

/**
 * MaxTemperatureMapper单元测试类（v4版本）
 */
public class MaxTemperatureMapperTest {

  /**
   * 测试解析有效记录
   *
   * 测试数据：1950年记录，温度-11，质量码N（有效）
   * 预期输出：(1950, -11)
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void parsesValidRecord() throws IOException, InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                            // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }

  /**
   * 测试解析缺失温度记录
   *
   * 测试数据：温度值为99991（接近缺失标记9999）
   * 预期输出：无输出（记录被过滤）
   *
   * 注意：99991不是9999，所以不是缺失标记，
   * 但此记录仍被过滤，因为质量码或其他原因不合法
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void parsesMissingTemperature() throws IOException,
      InterruptedException {
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
   * 测试解析格式错误的温度记录
   *
   * 测试数据：温度字段无法正确解析
   * 预期行为：
   * 1. 无正常输出
   * 2. MALFORMED计数器值为1
   *
   * Counter验证说明：
   * - 使用Counters获取运行时计数器
   * - 通过findCounter找到Temperature.MALFORMED
   * - 验证计数器值为1
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void parsesMalformedTemperature() throws IOException,
      InterruptedException {
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                // Year ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                            // Temperature ^^^^^
    // 创建计数器集合
    Counters counters = new Counters();
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .withCounters(counters)
      .runTest();
    // 查找MALFORMED计数器并验证值为1
    Counter c = counters.findCounter(MaxTemperatureMapper.Temperature.MALFORMED);
    assertThat(c.getValue(), is(1L));
  }
// ^^ MaxTemperatureMapperTestV4Malformed
}