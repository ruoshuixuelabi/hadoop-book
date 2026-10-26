// == JobBuilder
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * 帮各个示例程序组装 MapReduce 作业的辅助类
 *
 * <p>每个示例都要做同样几件事：建作业、设输入输出路径、解析命令行参数。
 * 这里把这些重复的活收拢起来，示例代码里就只剩下业务逻辑了
 * <p>用法是：先 {@code new JobBuilder(...)}，再 {@code withCommandLineArgs(args)}，
 * 最后用 {@code build()} 拿到组装好的作业
 */
public class JobBuilder {
  
  // 驱动类，用来告诉集群作业的主类在哪个 jar 里
  private final Class<?> driverClass;
  // 组装中的作业
  private final Job job;
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
   * @throws IOException 创建作业失败
   */
  public JobBuilder(Class<?> driverClass) throws IOException {
    // 委托给另一个构造器：不要额外参数
    this(driverClass, 0, "");
  }
  
  /**
   * 新建一个构造器，并说明要额外几个命令行参数
   *
   * @param driverClass 驱动类
   * @param extraArgCount 额外参数的个数
   * @param extrArgsUsage 额外参数的用法说明
   * @throws IOException 创建作业失败
   */
  public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) throws IOException {
    this.driverClass = driverClass;
    this.extraArgCount = extraArgCount;
    // 先建一个空作业
    this.job = new Job();
    // 指定 jar 包里的主类，集群据此定位作业类
    this.job.setJarByClass(driverClass);
    this.extrArgsUsage = extrArgsUsage;
  }

  // vv JobBuilder
  /**
   * 按「输入路径 输出路径」两个参数建一个作业
   *
   * <p>这是给命令行里只带两个参数的简单程序用的，
   * 不想自己解析参数的话直接调它就行
   *
   * @param tool 工具类，用来取类名打用法说明
   * @param conf Hadoop 配置
   * @param args 命令行参数
   * @return 组装好的作业；参数个数不对时返回 {@code null}
   * @throws IOException 创建作业失败
   */
  public static Job parseInputAndOutput(Tool tool, Configuration conf,
      String[] args) throws IOException {
    
    // 必须正好两个参数
    if (args.length != 2) {
      // 参数不对就把用法打出来
      printUsage(tool, "<input> <output>");
      // 返回 null 表示没建成功
      return null;
    }
    // 用给定的配置建作业
    Job job = new Job(conf);
    // 指明主类
    job.setJarByClass(tool.getClass());
    // 第 0 个参数是输入路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 第 1 个参数是输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job;
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
  // ^^ JobBuilder
  
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
    // 取作业自己的配置
    Configuration conf = job.getConfiguration();
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
    FileInputFormat.addInputPath(job, input);
    // 输出路径
    FileOutputFormat.setOutputPath(job, output);
    return this;
  }
  
  /**
   * 取出组装好的作业
   *
   * @return 作业对象
   */
  public Job build() {
    return job;
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
