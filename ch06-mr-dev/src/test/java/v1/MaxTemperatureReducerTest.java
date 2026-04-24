package v1;
// == MaxTemperatureReducerTestV1

/**
 * MaxTemperatureReducer版本1的单元测试
 *
 * 测试Reducer的核心功能：在一组值中找出最大值
 *
 * 测试框架：
 * - JUnit 4
 * - MRUnit的ReduceDriver
 *
 * Reducer测试要点：
 * - 输入键（年份）被正确传递到输出
 * - 输入值中的最大值被正确计算并输出
 *
 * @see MaxTemperatureReducer
 * @see org.apache.hadoop.mrunit.mapreduce.ReduceDriver
 */

// vv MaxTemperatureReducerTestV1
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

/**
 * MaxTemperatureReducer单元测试类
 */
public class MaxTemperatureReducerTest {

  /**
   * 测试Reducer返回值的最大值
   *
   * 测试场景：
   * - 输入键：1950
   * - 输入值列表：[10, 5]
   * - 预期输出：(1950, 10)
   *
   * 验证点：
   * 1. 输出键与输入键相同（1950）
   * 2. 输出值是输入值列表中的最大值（10）
   *
   * @throws IOException          IO异常
   * @throws InterruptedException 中断异常
   */
  @Test
  public void returnsMaximumIntegerInValues() throws IOException,
          InterruptedException {
    new ReduceDriver<Text, IntWritable, Text, IntWritable>()
            .withReducer(new MaxTemperatureReducer())
            .withInput(new Text("1950"),
                Arrays.asList(new IntWritable(10), new IntWritable(5)))
            .withOutput(new Text("1950"), new IntWritable(10))
            .runTest();
  }
  //^^ MaxTemperatureReducerTestV1
}