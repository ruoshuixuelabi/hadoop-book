// == MinimalMapReduce The simplest possible MapReduce driver, which uses the defaults
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MinimalMapReduce
/**
 * 最简 MapReduce 驱动程序：完全依赖框架的默认设置
 *
 * <p>本例回答「一个 MapReduce 作业最少要写多少代码」：
 * 只要指定输入路径和输出路径，Map、Reduce、输入输出格式
 * 全部使用 Hadoop 内置的默认实现
 * <p>默认行为是：{@code TextInputFormat} 逐行读入文本，
 * {@code IdentityMapper} 原样输出键值对，
 * {@code TextOutputFormat} 以「键\t值」的文本形式写出
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
    
    // 用当前 Configuration 构造一个作业对象
    Job job = new Job(getConf());
    // 指定作业 jar 包的位置；框架据此定位类与资源
    job.setJarByClass(getClass());
    // 添加输入路径；可以多次调用以添加多个输入
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 设置输出路径；一个作业只能有一个输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // 阻塞等待作业结束并打印进度；成功返回 true，失败返回 false
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * 程序入口
   *
   * <p>交给 {@link ToolRunner} 执行，它会先把 {@code -D} 之类的
   * 通用命令行选项解析进 {@link org.apache.hadoop.conf.Configuration}，
   * 再调用 {@link #run(String[])}
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 解析通用选项后调用 run，返回作业的退出码
    int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
    // 以退出码结束进程，便于 shell 脚本判断成败
    System.exit(exitCode);
  }
}
// ^^ MinimalMapReduce
