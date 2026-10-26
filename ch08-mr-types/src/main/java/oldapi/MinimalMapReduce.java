package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 最简 MapReduce 驱动程序：MapReduce v1 旧 API
 *
 * <p>与新 API 的例子功能完全相同，区别只在写法：
 * v1 用 {@link org.apache.hadoop.mapred.JobConf} 承载配置，
 * 用 {@link org.apache.hadoop.mapred.JobClient} 提交作业，
 * Map、Reduce 也必须显式指定 {@code MapRunner} / {@code ReduceRunner}
 */
public class MinimalMapReduce extends Configured implements Tool {
  
  /**
   * 解析命令行参数并运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示作业成功完成，-1 表示参数个数不对
   * @throws Exception 作业提交或执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 参数个数必须正好是两个：输入路径 + 输出路径
    if (args.length != 2) {
      // 打印用法提示；第一个 %s 会被替换成类名
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      // 再打印 Hadoop 通用选项（如 -D 设置配置项）的说明
      ToolRunner.printGenericCommandUsage(System.err);
      // 返回 -1 表示用法错误，这是驱动程序约定的失败码
      return -1;
    }
    
    // 构造 v1 的作业配置；第二个参数是作业主类
    JobConf conf = new JobConf(getConf(), getClass());
    // 添加输入路径；可以多次调用以添加多个输入
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    // 设置输出路径；一个作业只能有一个输出路径
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    // 提交作业并阻塞等待它跑完
    JobClient.runJob(conf);
    // 作业跑完即返回成功
    return 0;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项后调用 run
    int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
    // 以退出码结束进程，便于 shell 脚本判断成败
    System.exit(exitCode);
  }
}
