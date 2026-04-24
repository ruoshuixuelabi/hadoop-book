// cc ConfigurationPrinter An example Tool implementation for printing the properties in a Configuration

/**
 * Configuration属性打印工具
 *
 * 这是一个Tool接口实现示例，用于演示如何：
 * - 实现Configured接口和Tool接口
 * - 遍历并打印Configuration中的所有属性
 * - 使用ToolRunner统一执行
 *
 * 功能说明：
 * - 加载Hadoop默认配置文件（hdfs-default.xml, yarn-default.xml等）
 * - 遍历Configuration中的所有键值对
 * - 打印每个属性的键和值
 *
 * Tool接口优势：
 * - 支持GenericOptions（如-conf指定配置文件）
 * - 支持-D设置配置属性
 * - 统一的作业执行方式
 *
 * 运行示例：
 * # 打印默认配置
 * hadoop jar ch06-mr-dev.jar ConfigurationPrinter
 *
 * # 指定配置文件打印
 * hadoop jar ch06-mr-dev.jar ConfigurationPrinter -conf custom.xml
 *
 * # 设置属性并打印
 * hadoop jar ch06-mr-dev.jar ConfigurationPrinter -D property=value
 *
 * @see org.apache.hadoop.conf.Configured
 * @see org.apache.hadoop.util.Tool
 */
import java.util.Map.Entry;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

// vv ConfigurationPrinter
public class ConfigurationPrinter extends Configured implements Tool {

  /**
   * 静态代码块，添加Hadoop默认配置文件
   *
   * 这些文件按顺序加载，后续文件可以覆盖先前的默认值
   */
  static {
    // HDFS相关默认配置
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    // YARN相关默认配置
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
    // MapReduce相关默认配置
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  /**
   * 执行配置属性打印
   *
   * @param args 命令行参数（此实现不使用任何参数）
   * @return 始终返回0（成功）
   * @throws Exception 异常
   */
  @Override
  public int run(String[] args) throws Exception {
    // 获取Configuration实例
    Configuration conf = getConf();
    // 遍历所有属性并打印
    for (Entry<String, String> entry : conf) {
      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
    }
    return 0;
  }

  /**
   * 程序入口点
   *
   * @param args 命令行参数
   * @throws Exception 异常
   */
  public static void main(String[] args) throws Exception {
    // 使用ToolRunner运行，支持GenericOptions
    int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
    System.exit(exitCode);
  }
}
// ^^ ConfigurationPrinter