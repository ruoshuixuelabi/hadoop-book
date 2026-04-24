/**
 * 日志记录演示驱动程序
 *
 * 此驱动程序演示如何在MapReduce中使用日志记录功能。
 *
 * 功能说明：
 * - 配置一个简单的MapReduce作业
 * - 使用IdentityMapper（不做任何转换，直接传递键值对）
 * - 禁用Reducer（只运行Map阶段）
 *
 * 日志记录方式：
 * 1. System.out.println() - 输出到stdout文件
 * 2. Apache Commons Logging - 输出到syslog文件
 * 3. Log.isDebugEnabled() - 仅在DEBUG启用时记录详细日志
 *
 * 使用场景：
 * - 调试MapReduce作业
 * - 查看输入数据的键值内容
 * - 演示日志配置方法
 *
 * @see LoggingIdentityMapper
 * @see org.apache.hadoop.util.Tool
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 日志演示Driver
 *
 * 运行方式：
 * hadoop jar ch06-mr-dev.jar LoggingDriver [input] [output]
 */
public class LoggingDriver extends Configured implements Tool {

  /**
   * 配置并执行MapReduce作业
   *
   * @param args 命令行参数，[input, output]
   * @return 作业退出码
   * @throws Exception 异常
   */
  @Override
  public int run(String[] args) throws Exception {
    // 参数校验
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
              getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    // 创建作业并设置名称
    Job job = new Job(getConf(), "Logging job");
    job.setJarByClass(getClass());

    // 设置输入输出路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 设置Mapper类
    job.setMapperClass(LoggingIdentityMapper.class);
    // 禁用Reducer（setNumReduceTasks(0)表示只有Map阶段）
    job.setNumReduceTasks(0);

    // 提交作业并等待完成
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 程序入口点
   *
   * @param args 命令行参数
   * @throws Exception 异常
   */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LoggingDriver(), args);
    System.exit(exitCode);
  }
}