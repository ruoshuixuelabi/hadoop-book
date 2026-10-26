package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 帮各个示例程序组装 MapReduce 作业的辅助类（老 API 版）
 *
 * <p>这份是 {@code common} 模块里给「旧版 {@code mapred} API」准备的副本，
 * 放在 {@code oldapi} 包下
 * <p>和新 API 版唯一的区别是：作业用 {@code JobConf} 表示，
 * 而不是新 API 的 {@code Job}，
 * 所以输入输出路径是加到 {@code JobConf} 上的
 */
public class JobBuilder {
  
  // 驱动类，用来告诉集群作业的主类在哪个 jar 里
  private final Class<?> driverClass;
  // 组装中的作业配置
  private final JobConf conf;
  // 除输入输出之外，还要额外几个命令行参数
  private final int extraArgCount;
  // 额外参数在用法说明里怎么写
  private final String extrArgsUsage;
  
  // 真正解析出来的额外参数
  private String[] extraArgs;
  
  /**
   * 只带驱动类地新建一个构造器
   *
   * @param driverClass 驱动类
   */
  public JobBuilder(Class<?> driverClass) {
    // 委托给另一个构造器：不要额外参数
    this(driverClass, 0, "");
  }
  
  /**
   * 新建一个构造器，并说明要额外几个命令行参数
   *
   * @param driverClass 驱动类
   * @param extraArgCount 额外参数的个数
   * @param extrArgsUsage 额外参数的用法说明
   */
  public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) {
    this.driverClass = driverClass;
    this.extraArgCount = extraArgCount;
    // 建一个空的作业配置
    this.conf = new JobConf(driverClass);
    this.extrArgsUsage = extrArgsUsage;
  }

  /**
   * 按「输入路径 输出路径」两个参数建一个作业
   *
   * <p>老 API 里作业本身就是 {@code JobConf}，
   * 所以这里返回的是 {@code JobConf} 而不是 {@code Job}
   *
   * @param tool 工具类，用来取类名打用法说明
   * @param conf Hadoop 配置
   * @param args 命令行参数
   * @return 组装好的作业配置；参数个数不对时返回 {@code null}
   */
  public static JobConf parseInputAndOutput(Tool tool, Configuration conf,
      String[] args) {
    
    // 必须正好两个参数
    if (args.length != 2) {
      // 参数不对就把用法打出来
      printUsage(tool, "<input> <output>");
      // 返回 null 表示没建成功
      return null;
    }
    // 用给定的配置和主类建作业
    JobConf jobConf = new JobConf(conf, tool.getClass());
    // 第 0 个参数是输入路径
    FileInputFormat.addInputPath(jobConf, new Path(args[0]));
    // 第 1 个参数是输出路径
    FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
    return jobConf;
  }

  /**
   * 打印用法说明
   *
   * @param tool 工具类，用来取类名
   * @param extraArgsUsage 额外参数的用法说明
   */
  public static void printUsage(Tool tool, String extraArgsUsage) {
    System.err.printf("Usage: %s [genericOptions] %s\n\n",
        tool.getClass().getSimpleName(), extraArgsUsage);
    // Hadoop 自带的通用参数（-D、-fs 等）的用法
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  
  /**
   * 解析命令行参数，并把输入输出路径配到作业上
   *
   * <p>支持的格式是
   * {@code [genericOptions] [-overwrite] <输入路径> <输出路径> [额外参数...]}
   * <p>{@code -overwrite} 的意思是先删掉已有的输出目录再写
   *
   * @param args 命令行参数
   * @return 自己，方便链式调用
   * @throws IOException 出错
   */
  public JobBuilder withCommandLineArgs(String... args) throws IOException {
    // 先把 Hadoop 的通用参数挑走
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    // 剩下的才是我们自己定义的参数
    String[] otherArgs = parser.getRemainingArgs();
    // 参数个数不对
    if (otherArgs.length < 2 && otherArgs.length > 3 + extraArgCount) {
      // 把用法打出来
      System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output path> %s\n\n",
          driverClass.getSimpleName(), extrArgsUsage);
      // 再打一遍通用参数的用法
      GenericOptionsParser.printGenericCommandUsage(System.err);
      // 参数错误，直接退出
      System.exit(-1);
    }
    // 记录已经消费到第几个参数了
    int index = 0;
    // 默认不覆盖已有输出
    boolean overwrite = false;
    // 看看第一个参数是不是 -overwrite
    if (otherArgs[index].equals("-overwrite")) {
      // 记下来
      overwrite = true;
      // 跳过这个参数
      index++;
    }
    // 接下来两个分别是输入路径和输出路径
    Path input = new Path(otherArgs[index++]);
    // 输出路径
    Path output = new Path(otherArgs[index++]);
    
    // 还有剩下的，那就是额外参数
    if (index < otherArgs.length) {
      // 按剩下的个数开数组
      extraArgs = new String[otherArgs.length - index];
      // 把剩下的参数搬进数组
      System.arraycopy(otherArgs, index, extraArgs, 0, otherArgs.length - index);
    }
    
    // 要覆盖的话，先把旧输出删掉
    if (overwrite) {
      // 递归删除整个输出目录
      output.getFileSystem(conf).delete(output, true);
    }
    
    // 输入路径
    FileInputFormat.addInputPath(conf, input);
    // 输出路径
    FileOutputFormat.setOutputPath(conf, output);
    return this;
  }
  
  /**
   * 取出组装好的作业
   *
   * @return 作业配置对象
   */
  public JobConf build() {
    return conf;
  }
  
  /**
   * 取额外参数
   *
   * @return 额外参数；没有则为 {@code null}
   */
  public String[] getExtraArgs() {
    return extraArgs;
  }
}
