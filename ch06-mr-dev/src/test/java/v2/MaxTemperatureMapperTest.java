package v2;

/**
 * MaxTemperatureMapper（v2版本）单元测试
 *
 * 测试v2版本的Mapper，该版本使用NcdcRecordParser进行数据解析。
 *
 * 测试场景：
 * 1. 处理有效记录 - 正常温度数据应被正确解析输出
 * 2. 处理正温度记录 - 带前导'+'的温度值
 * 3. 忽略缺失温度记录 - 温度为9999的记录
 * 4. 忽略可疑质量记录 - 质量码不为[01459]的记录
 *
 * NCDC测试数据格式说明：
 * - 位置15-18：年份
 * - 位置87-92：温度值
 * - 位置92：质量码
 *
 * 解析器增强点（相比v1）：
 * - 正确处理带'+'前缀的温度值
 * - 验证质量码合法性
 * - 过滤缺失温度（9999）
 * - 过滤无效质量码
 *
 * @see MaxTemperatureMapper
 * @see NcdcRecordParser
 */
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.jupiter.api.Test;

/**
 * MaxTemperatureMapper单元测试类
 */
public class MaxTemperatureMapperTest {

  /**
   * 测试处理有效记录
   *
   * 测试数据：1950年记录，温度为-11，质量码为N（有效）
   * 预期输出：(1950, -11)
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void processesValidRecord() throws IOException, InterruptedException {
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
   * 测试处理正温度记录
   *
   * 测试数据：1950年记录，温度为+11（带前导'+'号）
   * 预期输出：(1950, 11)
   *
   * 此测试验证NcdcRecordParser能正确处理带'+'前缀的温度值
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void processesPositiveTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+00111+99999999999");
              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .withOutput(new Text("1950"), new IntWritable(11))
      .runTest();
  }

  /**
   * 测试忽略缺失温度记录
   *
   * 测试数据：温度值为9999（缺失标记）
   * 预期输出：无（记录被过滤）
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException,
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

  /**
   * 测试忽略可疑质量记录
   *
   * 测试数据：质量码为2（可疑质量）
   * 预期输出：无（记录被过滤）
   *
   * NCDC质量码说明：
   * - 0, 1, 4, 5, 9 为合法质量码
   * - 2, 3, 6, 7, A 等为可疑或无效质量码
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void ignoresSuspectQualityRecord() throws IOException,
      InterruptedException {
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