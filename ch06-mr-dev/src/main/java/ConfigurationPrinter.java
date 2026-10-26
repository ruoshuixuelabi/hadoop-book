// cc ConfigurationPrinter An example Tool implementation for printing the properties in a Configuration
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

// vv ConfigurationPrinter
/**
 * 打印 {@link Configuration} 中所有属性的示例工具
 *
 * <p>{@link Configuration} 是 Hadoop 的配置中枢：
 * core-site.xml、hdfs-site.xml、mapred-site.xml 等配置文件里的键值对，
 * 最终都汇总进同一个 Configuration 对象
 * <p>本类实现 {@link Tool} 接口，可以直接交给 {@link ToolRunner} 运行，
 * 从而自动支持 {@code -D} 之类的通用命令行选项
 */
public class ConfigurationPrinter extends Configured implements Tool {
  
  // 静态初始化块：把各组件的默认配置文件登记进 Configuration 的全局资源列表
  // 登记顺序即优先级——后登记的会覆盖先登记的同名属性
  static {
    // 逐个登记各组件的默认配置文件
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  /**
   * 逐条打印配置项
   *
   * @param args 命令行参数，本例未用
   * @return 0 表示执行成功
   * @throws Exception 读取配置失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // getConf() 拿回的是 Configured 持有的那份 Configuration
    Configuration conf = getConf();
    // Configuration 实现了 Iterable，可以直接遍历所有键值对
    for (Entry<String, String> entry: conf) {
      // 按 properties 的格式输出，便于直接粘贴回配置文件
      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
    }
    // 返回 0 表示成功
    return 0;
  }
  
  /**
   * 工具类的标准入口
   *
   * @param args 通用命令行选项
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会先解析通用选项（如 -Dproperty=value），再调用 run()
    int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
    // 把退出码交给 shell，便于脚本判断成败
    System.exit(exitCode);
  }
}
// ^^ ConfigurationPrinter
