package v2;
// cc MaxTemperatureDriverTestV2 A test for MaxTemperatureDriver that uses a local,
// in-process job runner

/**
 * MaxTemperatureDriver集成测试（本地执行模式）
 *
 * 测试目标：
 * - 验证MaxTemperatureDriver能否正确执行MapReduce作业
 * - 使用本地（in-process）执行器运行作业
 * - 验证输出结果是否符合预期
 *
 * 本地执行模式配置：
 * - fs.defaultFS=file:/// - 使用本地文件系统
 * - mapreduce.framework.name=local - 使用本地MapReduce框架
 *
 * 测试数据：
 * - 输入：input/ncdc/micro（小规模测试数据）
 * - 输出：output目录
 *
 * 测试验证点：
 * 1. 作业退出码为0（成功）
 * 2. 输出文件数量正确
 * 3. 输出内容与期望值匹配（1949\t111, 1950\t22）
 *
 * @see MaxTemperatureDriver
 * @see MaxTemperatureMapper
 * @see MaxTemperatureReducer
 */
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.jupiter.api.Test;

/**
 * MaxTemperatureDriver本地测试类
 */
public class MaxTemperatureDriverTest {

  /**
   * 输出文件过滤器
   *
   * 排除以下划线开头的文件（如_PARTITIONS等元数据文件）
   */
  public static class OutputLogFilter implements PathFilter {
    /**
     * 判断路径是否应该包含在输出中
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
   * 测试Driver执行完整作业流程
   *
   * 测试流程：
   * 1. 配置本地执行环境
   * 2. 准备输入输出路径
   * 3. 执行Driver.run()
   * 4. 验证作业退出码
   * 5. 验证输出结果
   *
   * @throws Exception 作业执行异常
   */
  //vv MaxTemperatureDriverTestV2
  @Test
  public void test() throws Exception {
    // 创建本地模式配置
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("mapreduce.framework.name", "local");
    // 限制排序内存，优化测试性能
    conf.setInt("mapreduce.task.io.sort.mb", 1);

    // 设置输入输出路径
    Path input = new Path("input/ncdc/micro");
    Path output = new Path("output");

    // 获取本地文件系统并清理旧输出
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(output, true); // 删除旧输出目录

    // 创建Driver并执行作业
    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    driver.setConf(conf);

    // 执行作业并验证退出码
    int exitCode = driver.run(new String[]{
            input.toString(), output.toString()});
    assertThat(exitCode, is(0));

    // 验证输出结果
    checkOutput(conf, output);
  }
  //^^ MaxTemperatureDriverTestV2

  /**
   * 验证作业输出内容
   *
   * @param conf   配置对象
   * @param output 输出目录
   * @throws IOException IO异常
   */
  private void checkOutput(Configuration conf, Path output) throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    // 获取输出文件列表（排除元数据文件）
    Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
    // 验证只有一个输出文件
    assertThat(outputFiles.length, is(1));

    // 打开输出文件并逐行验证
    BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
    BufferedReader expected = asBufferedReader(getClass().getResourceAsStream("/expected.txt"));
    String expectedLine;
    // 逐行比较实际输出与期望输出
    while ((expectedLine = expected.readLine()) != null) {
      assertThat(actual.readLine(), is(expectedLine));
    }
    // 验证没有额外行
    assertThat(actual.readLine(), nullValue());
    actual.close();
    expected.close();
  }

  /**
   * 将InputStream转换为BufferedReader
   *
   * @param in 输入流
   * @return BufferedReader包装
   * @throws IOException IO异常
   */
  private BufferedReader asBufferedReader(InputStream in) throws IOException {
    return new BufferedReader(new InputStreamReader(in));
  }
}