import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 演示如何在 MapReduce 作业中打日志的驱动类
 *
 * <p>作业本身只是把输入原样输出（identity mapper），
 * 目的是观察 {@code LoggingIdentityMapper} 里两种日志写法的效果
 * <p>日志打在 Map 任务所在的节点上，
 * 因此在集群里要先找到对应的容器日志才能看到
 */
public class LoggingDriver extends Configured implements Tool {
  
  /**
   * 组装并运行一个只有 Map、没有 Reduce 的作业
   *
   * @param args 第一个参数是输入路径，第二个是输出路径
   * @return 0 表示作业成功，1 表示失败，-1 表示参数错误
   * @throws Exception 作业提交或运行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 参数个数不对：先打印用法，再返回错误码
    if (args.length != 2) {
      // 用法信息写到 stderr，stdout 留给正常输出
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      // 再补上通用选项的说明
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    // 作业名会显示在 YARN 的 Web UI 上
    Job job = new Job(getConf(), "Logging job");
    // 告诉框架作业代码在哪个类里，提交时据此定位 jar
    job.setJarByClass(getClass());

    // 输入路径可以一次加多个
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 输出目录必须事先不存在，否则作业会失败
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // 指定 Mapper 实现
    job.setMapperClass(LoggingIdentityMapper.class);
    // 把 Reduce 任务数设为 0：作业退化为「只 Map 不 Reduce」，
    // Mapper 的输出直接写进最终结果文件
    job.setNumReduceTasks(0);
    
    // true 表示把进度打印到控制台；作业失败时返回 1
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * 命令行入口
   *
   * @param args 通用选项，以及输入、输出路径
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    // 同样交给 ToolRunner，以便支持通用选项
    int exitCode = ToolRunner.run(new LoggingDriver(), args);
    System.exit(exitCode);
  }
}