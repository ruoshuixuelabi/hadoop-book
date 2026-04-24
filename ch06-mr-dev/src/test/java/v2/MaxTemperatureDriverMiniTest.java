package v2;

/**
 * MaxTemperatureDriver迷你集群集成测试
 *
 * 此测试使用ClusterMapReduceTestCase在真实的HDFS和MapReduce集群环境中运行测试。
 *
 * 与Local模式测试的区别：
 * - 使用真实的HDFS（不是本地文件系统）
 * - 使用真实的MapReduce集群（不是本地执行器）
 * - 可以测试更多集群特定功能
 *
 * 测试流程：
 * 1. 创建临时HDFS目录
 * 2. 复制本地测试数据到HDFS
 * 3. 执行Driver作业
 * 4. 验证作业退出码
 * 5. 读取并验证输出结果
 *
 * ClusterMapReduceTestCase说明：
 * - JUnit 3风格的测试基类（不是JUnit 4）
 * - 自动创建和清理临时集群环境
 * - 提供getFileSystem()、getInputDir()等辅助方法
 *
 * @see MaxTemperatureDriver
 * @see org.apache.hadoop.mapred.ClusterMapReduceTestCase
 */
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;

/**
 * MaxTemperatureDriver迷你集群测试类
 *
 * 注意：此类继承自JUnit 3风格的ClusterMapReduceTestCase，
 * 因此测试方法不需要@Test注解
 */
public class MaxTemperatureDriverMiniTest extends ClusterMapReduceTestCase {

  /**
   * 输出文件过滤器
   *
   * 过滤掉MapReduce产生的元数据文件（如_PARTITIONS）
   */
  public static class OutputLogFilter implements PathFilter {
    /**
     * 判断文件是否应该包含在结果中
     *
     * @param path 文件路径
     * @return true表示包含，false表示排除
     */
    public boolean accept(Path path) {
      // 排除以下划线开头的文件
      return !path.getName().startsWith("_");
    }
  }

  /**
   * 测试设置方法
   *
   * 设置测试所需的系统属性：
   * - test.build.data - 测试数据目录
   * - hadoop.log.dir - Hadoop日志目录
   *
   * @throws Exception 设置异常
   */
  @Override
  protected void setUp() throws Exception {
    if (System.getProperty("test.build.data") == null) {
      // 设置默认测试数据目录为/tmp
      System.setProperty("test.build.data", "/tmp");
    }
    if (System.getProperty("hadoop.log.dir") == null) {
      // 设置默认Hadoop日志目录为/tmp
      System.setProperty("hadoop.log.dir", "/tmp");
    }
    super.setUp();
  }

  /**
   * 测试Driver在真实集群中的执行
   *
   * 注意：此方法没有@Test注解，
   * 因为ClusterMapReduceTestCase是JUnit 3测试类，
   * 测试方法名为"test()"会被自动执行
   *
   * @throws Exception 作业执行异常
   */
  // Not marked with @Test since ClusterMapReduceTestCase is a JUnit 3 test case
  public void test() throws Exception {
    // 创建作业配置
    Configuration conf = createJobConf();

    // 设置本地输入路径和HDFS上的路径
    Path localInput = new Path("input/ncdc/micro");
    Path input = getInputDir();
    Path output = getOutputDir();

    // 将本地测试数据复制到测试HDFS
    getFileSystem().copyFromLocalFile(localInput, input);

    // 创建并执行Driver
    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    driver.setConf(conf);

    // 执行作业并验证退出码
    int exitCode = driver.run(new String[] {
        input.toString(), output.toString() });
    assertThat(exitCode, is(0));

    // 验证输出文件
    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(output, new OutputLogFilter()));
    assertThat(outputFiles.length, is(1));

    // 读取并验证输出内容
    InputStream in = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    // 验证1949年最高温度为111
    assertThat(reader.readLine(), is("1949\t111"));
    // 验证1950年最高温度为22
    assertThat(reader.readLine(), is("1950\t22"));
    // 验证没有更多输出
    assertThat(reader.readLine(), nullValue());
    reader.close();
  }
}