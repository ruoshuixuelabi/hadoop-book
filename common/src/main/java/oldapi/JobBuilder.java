package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * MapReduce 作业构建辅助类（旧 API / Hadoop 1.x）
 *
 * 功能说明：
 * - 简化 MapReduce 作业的创建和配置过程
 * - 支持解析标准命令行参数（输入路径、输出路径等）
 * - 提供 fluent 风格的作业构建接口
 *
 * 与新 API JobBuilder 的区别：
 * - 使用 org.apache.hadoop.mapred 包（旧 API）
 * - 返回 JobConf 和 JobConf 类型
 * - 不支持 Yarn，功能受限
 *
 * @since 4.0
 */
public class JobBuilder {

  /** 驱动类（用于设置 JAR 包和作业名称） */
  private final Class<?> driverClass;

  /** MapReduce 作业配置（旧 API 使用 JobConf） */
  private final JobConf conf;

  /** 额外参数的数量（用于验证命令行参数个数） */
  private final int extraArgCount;

  /** 额外参数的用法说明字符串 */
  private final String extrArgsUsage;

  /** 解析后的额外参数数组 */
  private String[] extraArgs;

  /**
   * 构造函数：创建 JobBuilder 实例
   *
   * @param driverClass 驱动类（包含 main 方法的类）
   */
  public JobBuilder(Class<?> driverClass) {
    this(driverClass, 0, "");
  }

  /**
   * 构造函数：创建带有额外参数配置的 JobBuilder 实例
   *
   * @param driverClass 驱动类
   * @param extraArgCount 期望的额外参数数量
   * @param extrArgsUsage 额外参数的用法说明
   */
  public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) {
    this.driverClass = driverClass;
    this.extraArgCount = extraArgCount;
    // 创建作业配置实例
    this.conf = new JobConf(driverClass);
    this.extrArgsUsage = extrArgsUsage;
  }

  /**
   * 解析输入路径和输出路径参数
   *
   * 静态便捷方法，用于快速创建仅包含输入/输出路径的作业。
   *
   * @param tool Tool 接口实现（通常为驱动类）
   * @param conf Hadoop 配置对象
   * @param args 命令行参数数组（应包含输入路径和输出路径）
   * @return 配置好的 JobConf 对象，如果参数个数不正确则返回 null
   */
  public static JobConf parseInputAndOutput(Tool tool, Configuration conf,
      String[] args) {

    // 验证参数个数：必须恰好 2 个（输入路径和输出路径）
    if (args.length != 2) {
      printUsage(tool, "<input> <output>");
      return null;
    }
    // 创建作业配置并设置路径
    JobConf jobConf = new JobConf(conf, tool.getClass());
    FileInputFormat.addInputPath(jobConf, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
    return jobConf;
  }

  /**
   * 打印作业用法说明到标准错误流
   *
   * @param tool Tool 接口实现
   * @param extraArgsUsage 额外参数的用法说明
   */
  public static void printUsage(Tool tool, String extraArgsUsage) {
    // 打印程序名称和用法
    System.err.printf("Usage: %s [genericOptions] %s\n\n",
        tool.getClass().getSimpleName(), extraArgsUsage);
    // 打印 Hadoop 通用选项帮助信息
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }

  /**
   * 解析命令行参数并配置作业
   *
   * 支持的参数格式：
   * - genericOptions：Hadoop 通用选项（如 -D property=value）
   * - -overwrite：覆盖输出目录（可选）
   * - 输入路径：HDFS 或本地文件系统路径
   * - 输出路径：HDFS 或本地文件系统路径
   * - 额外参数：自定义参数（数量由 extraArgCount 指定）
   *
   * @param args 命令行参数
   * @return this（支持 fluent 风格链式调用）
   * @throws IOException 如果文件系统操作失败
   */
  public JobBuilder withCommandLineArgs(String... args) throws IOException {
    // 解析 generic options
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    // 获取解析后剩余的参数
    String[] otherArgs = parser.getRemainingArgs();

    // 验证参数个数
    if (otherArgs.length < 2 && otherArgs.length > 3 + extraArgCount) {
      System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output path> %s\n\n",
          driverClass.getSimpleName(), extrArgsUsage);
      GenericOptionsParser.printGenericCommandUsage(System.err);
      System.exit(-1);
    }

    // 解析可选的 -overwrite 标志和路径参数
    int index = 0;
    boolean overwrite = false;
    if (otherArgs[index].equals("-overwrite")) {
      overwrite = true;
      index++;
    }
    // 解析输入路径
    Path input = new Path(otherArgs[index++]);
    // 解析输出路径
    Path output = new Path(otherArgs[index++]);

    // 收集额外的自定义参数（如果有）
    if (index < otherArgs.length) {
      extraArgs = new String[otherArgs.length - index];
      System.arraycopy(otherArgs, index, extraArgs, 0, otherArgs.length - index);
    }

    // 如果指定了 -overwrite，删除已存在的输出目录
    if (overwrite) {
      output.getFileSystem(conf).delete(output, true);
    }

    // 设置作业的输入/输出路径
    FileInputFormat.addInputPath(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    return this;
  }

  /**
   * 构建并返回配置的作业配置对象
   *
   * @return 配置完成的 JobConf 对象，可以提交到集群执行
   */
  public JobConf build() {
    return conf;
  }

  /**
   * 获取解析后的额外参数
   *
   * @return 额外参数数组，如果没有额外参数则返回 null
   */
  public String[] getExtraArgs() {
    return extraArgs;
  }
}
