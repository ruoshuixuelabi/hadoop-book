package v2;
// cc MaxTemperatureDriverTestV2 A test for MaxTemperatureDriver that uses a local,
// in-process job runner
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

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
import org.junit.Test;

/**
 * 用「本地模式」跑完整作业的端到端测试
 *
 * <p>把 {@code mapreduce.framework.name} 设成 {@code local}，
 * Map 和 Reduce 就在同一个 JVM 里以线程方式跑，不需要真正的集群
 * <p>与 {@code MaxTemperatureDriverMiniTest} 的区别：
 * 一个用本地模式（快），一个用迷你集群（更接近真实环境）
 */
public class MaxTemperatureDriverTest {
  
  /**
   * 过滤掉以 {@code _} 开头的隐藏文件
   *
   * <p>输出目录里除了真正的结果文件，还有 {@code _SUCCESS} 之类的标记文件
   */
  public static class OutputLogFilter implements PathFilter {
      /**
       * 判断某个路径是否要保留
       *
       * @param path 待判断的路径
       * @return 文件名不以 {@code _} 开头时返回 true
       */
    public boolean accept(Path path) {
      // 下划线开头的是 Hadoop 自己放的隐藏文件
      return !path.getName().startsWith("_");
    }
  }
  
//vv MaxTemperatureDriverTestV2
  /**
   * 跑一次作业并校验输出结果
   *
   * @throws Exception 作业执行失败
   */
  @Test
  public void test() throws Exception {
    // 手工搭一份配置，不依赖任何外部配置文件
    Configuration conf = new Configuration();
    // 默认文件系统用本地文件系统
    conf.set("fs.defaultFS", "file:///");
    // 关键：用本地模式，Map/Reduce 在本进程内跑
    conf.set("mapreduce.framework.name", "local");
    // 把排序缓冲区调到最小，省内存
    conf.setInt("mapreduce.task.io.sort.mb", 1);
    
    // 样本数据目录
    Path input = new Path("input/ncdc/micro");
    // 输出目录
    Path output = new Path("output");
    
    // 拿到本地文件系统句柄
    FileSystem fs = FileSystem.getLocal(conf);
    // 删掉上一次的输出——输出目录必须不存在，作业才能成功
    fs.delete(output, true); // delete old output
    
    // 直接用被测的 Driver
    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    // 把配置交给 Driver
    driver.setConf(conf);
    
    // 输入、输出两个路径
    int exitCode = driver.run(new String[] {
        input.toString(), output.toString() });
    // 0 表示作业成功
    assertThat(exitCode, is(0));
    
    // 再把输出和期望结果逐行比对
    checkOutput(conf, output);
  }
//^^ MaxTemperatureDriverTestV2

  /**
   * 把实际输出与测试资源里的期望结果逐行比对
   *
   * @param conf 配置
   * @param output 输出目录
   * @throws IOException 读取失败
   */
  private void checkOutput(Configuration conf, Path output) throws IOException {
    // 复用本地文件系统
    FileSystem fs = FileSystem.getLocal(conf);
    // 滤掉隐藏文件后应当只剩一个结果文件
    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(output, new OutputLogFilter()));
    // 确认只有一个结果文件
    assertThat(outputFiles.length, is(1));
    
    // 实际输出
    BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
    // 期望输出放在测试资源里，跟着 classpath 走
    BufferedReader expected = asBufferedReader(
        getClass().getResourceAsStream("/expected.txt"));
    // 逐行读期望文件，遇到文件尾就停
    String expectedLine;
    // 每一行期望值都要在实际输出里找到对应的一行
    while ((expectedLine = expected.readLine()) != null) {
      // 比对当前这一行
      assertThat(actual.readLine(), is(expectedLine));
    }
    // 期望文件读完了，实际输出也必须正好读完
    assertThat(actual.readLine(), nullValue());
    // 关闭流
    actual.close();
    // 关闭流
    expected.close();
  }
  
  /**
   * 把字节流包装成带缓冲的字符流
   *
   * @param in 字节流
   * @return 可按行读取的字符流
   * @throws IOException 包装失败
   */
  private BufferedReader asBufferedReader(InputStream in) throws IOException {
    // 默认字符集与 Hadoop 保持一致
    return new BufferedReader(new InputStreamReader(in));
  }
}
