package v2;

// cc MaxTemperatureDriverV2 Application to find the maximum temperature

/**
 * 最高温度MapReduce作业驱动器（版本2）
 *
 * 使用Tool接口实现的驱动器，支持GenericOptions标准选项。
 *
 * 功能：
 * - 配置并执行计算每年最高温度的MapReduce作业
 * - 输入：NCDC格式气象数据
 * - 输出：每年最高温度记录
 *
 * 主要配置项：
 * - Mapper: MaxTemperatureMapper（解析气象记录）
 * - Combiner: MaxTemperatureReducer（本地聚合温度）
 * - Reducer: MaxTemperatureReducer（全局聚合）
 * - 输出键类型: Text（年份）
 * - 输出值类型: IntWritable（温度）
 *
 * @see MaxTemperatureMapper
 * @see MaxTemperatureReducer
 * @see org.apache.hadoop.util.Tool
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import v1.MaxTemperatureReducer;
// vv MaxTemperatureDriverV2
public class MaxTemperatureDriver extends Configured implements Tool {

  /**
   * 执行MapReduce作业
   *
   * @param args 命令行参数
   * @return 作业退出码（0成功，非0失败）
   * @throws Exception 作业执行异常
   */
  @Override
  public int run(String[] args) throws Exception {
    // 参数校验：需要输入路径和输出路径
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
              getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    // 创建Job实例并设置作业名称
    Job job = new Job(getConf(), "Max temperature");
    // 设置驱动类（用于确定哪个类包含main方法）
    job.setJarByClass(getClass());
    // 设置输入路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 设置输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // 设置Mapper类
    job.setMapperClass(MaxTemperatureMapper.class);
    // 设置Combiner类（本地Reducer，减少网络传输）
    job.setCombinerClass(MaxTemperatureReducer.class);
    // 设置Reducer类
    job.setReducerClass(MaxTemperatureReducer.class);
    // 设置输出键类型
    job.setOutputKeyClass(Text.class);
    // 设置输出值类型
    job.setOutputValueClass(IntWritable.class);
    // 提交作业并等待完成，返回状态码
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 程序入口点
   *
   * @param args 命令行参数
   * @throws Exception 异常
   */
  public static void main(String[] args) throws Exception {
    // 使用ToolRunner运行，支持GenericOptions
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureDriverV2