package v1;
// == MaxTemperatureReducerTestV1
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

/**
 * v1 版 Reducer 的单元测试
 *
 * <p>用 MRUnit 的 {@code ReduceDriver} 驱动 Reducer：
 * 喂进同一个键下的多个值，断言它输出其中最大的一个
 */
public class MaxTemperatureReducerTest {
  
  //vv MaxTemperatureReducerTestV1
  /**
   * 验证 Reducer 输出的是这组值中的最大值
   *
   * @throws IOException I/O 出错
   * @throws InterruptedException 任务被中断
   */
  @Test
  public void returnsMaximumIntegerInValues() throws IOException,
      InterruptedException {
    // MRUnit 的 ReduceDriver：喂入键和一组值，收集输出
    new ReduceDriver<Text, IntWritable, Text, IntWritable>()
      // 指定被测的 Reducer
      .withReducer(new MaxTemperatureReducer())
      // 输入：键是年份 1950，值是这一年的若干个温度（这里是 10 和 5）
      .withInput(new Text("1950"),
          Arrays.asList(new IntWritable(10), new IntWritable(5)))
      // 期望输出：键不变，值为较大的 10
      .withOutput(new Text("1950"), new IntWritable(10))
      .runTest();
  }
  //^^ MaxTemperatureReducerTestV1
}
