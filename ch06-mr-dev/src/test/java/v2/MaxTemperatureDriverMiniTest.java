package v2;

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

// A test for MaxTemperatureDriver that runs in a "mini" HDFS and MapReduce cluster
/**
 * 在「迷你集群」里跑完整作业的端到端测试
 *
 * <p>{@code ClusterMapReduceTestCase} 会在本地起一个迷你 HDFS
 * 和 MapReduce 集群，作业真的会被调度和执行
 * <p>它继承的是 JUnit 3 的 TestCase，所以方法名必须以 {@code test} 开头，
 * 也不需要 {@code @Test} 注解
 */
public class MaxTemperatureDriverMiniTest extends ClusterMapReduceTestCase {
  
  /**
   * 过滤掉以 {@code _} 开头的隐藏文件
   *
   * <p>输出目录里除了真正的结果文件，还有 {@code _SUCCESS} 之类的标记文件，
   * 统计结果文件个数时要把它们排除掉
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
  
  /**
   * 在启动迷你集群之前指定数据目录和日志目录
   *
   * @throws Exception 启动集群失败
   */
  @Override
  protected void setUp() throws Exception {
    // 迷你集群的数据目录，先看看调用方有没有指定
    if (System.getProperty("test.build.data") == null) {
      // 没指定就用 /tmp，别把测试数据写进项目目录
      System.setProperty("test.build.data", "/tmp");
    }
    // 日志目录同理
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "/tmp");
    }
    // 交给父类真正把集群拉起来
    super.setUp();
  }
  
  // Not marked with @Test since ClusterMapReduceTestCase is a JUnit 3 test case
  /**
   * 跑完整作业并检查输出
   *
   * @throws Exception 作业执行失败
   */
  public void test() throws Exception {
    // 父类提供的作业配置，指向迷你集群
    Configuration conf = createJobConf();
    
    // 样本数据放在本地文件系统里
    Path localInput = new Path("input/ncdc/micro");
    // 集群里的输入目录
    Path input = getInputDir();
    // 集群里的输出目录
    Path output = getOutputDir();
    
    // Copy input data into test HDFS
    // 先把样本数据复制进集群的 HDFS
    getFileSystem().copyFromLocalFile(localInput, input);
    
    // 直接用被测的 Driver 跑作业
    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    // 把集群的配置交给 Driver
    driver.setConf(conf);
    
    // Driver 需要输入、输出两个路径
    int exitCode = driver.run(new String[] {
        input.toString(), output.toString() });
    // 0 表示作业执行成功
    assertThat(exitCode, is(0));
    
    // Check the output is as expected
    // 列出输出目录，滤掉 _SUCCESS 之类的隐藏文件
    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(output, new OutputLogFilter()));
    // 应当恰好只有一个结果文件
    assertThat(outputFiles.length, is(1));
    
    // 打开结果文件
    InputStream in = getFileSystem().open(outputFiles[0]);
    // 包成字符流，方便按行读
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    // 结果按年份排序，每行是「年份<TAB>最高气温」
    assertThat(reader.readLine(), is("1949\t111"));
    // 1949 年的最高气温是 111 度，1950 年是 22 度
    assertThat(reader.readLine(), is("1950\t22"));
    // 读完了，后面没有更多行
    assertThat(reader.readLine(), nullValue());
    // 关闭流
    reader.close();
  }
}
