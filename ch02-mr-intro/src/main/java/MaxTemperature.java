// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计 NCDC 气象数据中每个气象站（按年份分组）的最高气温
 *
 * <p>这是全书第一个 MapReduce 程序，完整演示了作业的组装方式：
 * 指定输入/输出路径、绑定 Mapper 与 Reducer、声明输出键值类型
 * <p>输入为 NCDC 原始记录文本（每行一条测量），输出为「年份+站号 → 最高气温」
 * <p>注意本类不指定 Combiner；加入 Combiner 以减少网络传输的版本见
 * {@link MaxTemperatureWithCombiner}
 */
public class MaxTemperature {

  /**
   * 作业入口：组装并运行统计最高气温的 MapReduce 作业
   *
   * <p>流程：校验命令行参数 → 创建 Job → 设置输入输出与 Map/Reduce 类
   * → 等待作业完成
   *
   * @param args 恰好两个参数：输入路径、输出路径
   * @throws Exception 作业配置或执行过程中发生 Hadoop 层面的错误
   */
  public static void main(String[] args) throws Exception {
    // 参数个数校验：少于两个时直接退出，避免后面 new Path(null) 抛出难以理解的异常
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    // Job 是作业配置的载体，最终由它向集群提交任务
    Job job = new Job();
    // 指定 jar 包中的主类，集群据此定位作业类
    job.setJarByClass(MaxTemperature.class);
    job.setJobName("Max temperature");

    // 输入与输出路径都可以是 HDFS 路径或本地文件系统路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // 绑定 Map 与 Reduce 的实现类
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    // 显式声明输出键值类型：Hadoop 需要在作业提交前完成序列化器的协商
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // waitForCompletion(true) 打印进度并阻塞直到作业结束；
    // 用退出码 0/1 表达成功失败，便于 shell 脚本判断
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
