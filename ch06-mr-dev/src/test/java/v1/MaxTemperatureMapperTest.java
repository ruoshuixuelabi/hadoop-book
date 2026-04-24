package v1;
// cc MaxTemperatureMapperTestV1 Unit test for MaxTemperatureMapper
// == MaxTemperatureMapperTestV1Missing

/**
 * MaxTemperatureMapper版本1的单元测试
 *
 * 测试场景：
 * 1. processesValidRecord - 处理有效气象记录
 * 2. ignoresMissingTemperatureRecord - 忽略缺失温度记录（被@Ignore注解跳过）
 * 3. processesMalformedTemperatureRecord - 处理格式错误的温度记录
 *
 * 测试框架：
 * - JUnit 4 用于测试注解和断言
 * - MRUnit 用于MapReduce组件测试
 *
 * NCDC测试数据格式说明：
 * - 0043011990999991950051518004+68750+023550FM-12+0382... - 气象站数据
 * - 位置15-18：年份（1950）
 * - 位置87-92：温度值（-00111表示-11.1度）
 *
 * @see MaxTemperatureMapper
 * @see org.apache.hadoop.mrunit.mapreduce.MapDriver
 */
// vv MaxTemperatureMapperTestV1

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

/**
 * MaxTemperatureMapper单元测试类
 */
public class MaxTemperatureMapperTest {

  /**
   * 测试有效记录的处理
   *
   * 测试数据：1950年的气象记录，温度为-11
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

  // ^^ MaxTemperatureMapperTestV1

  /**
   * 测试忽略缺失温度的记录
   *
   * 测试数据：温度值为9999（缺失标记）
   * 预期输出：无输出（记录被忽略）
   *
   * 注意：此测试被@Ignore注解跳过，
   * 因为书中演示的是一个失败的测试用例
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Ignore // since we are showing a failing test in the book
// vv MaxTemperatureMapperTestV1Missing
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

  // ^^ MaxTemperatureMapperTestV1Missing

  /**
   * 测试处理格式错误的温度记录
   *
   * 测试数据：1957年的记录
   * 预期输出：(1957, 1957) - 注意这里输出的是年份值作为温度
   *
   * 此测试展示了v1版本的一个bug：
   * 当温度字段格式错误时，会将年份值当作温度输出
   * v2及以后版本通过NcdcRecordParser修复了这个问题
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void processesMalformedTemperatureRecord() throws IOException,
          InterruptedException {
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                // Year ^^^^
                "RJSN V02011359003150070356999999433201957010100005+353");
        // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
            .withMapper(new MaxTemperatureMapper())
            .withInput(new LongWritable(0), value)
            .withOutput(new Text("1957"), new IntWritable(1957))
            .runTest();
  }
// vv MaxTemperatureMapperTestV1
}
// ^^ MaxTemperatureMapperTestV1