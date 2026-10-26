package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * 统计每个气象站最高气温的作业入口（MapReduce v1 旧 API 版本）
 *
 * <p>这是默认包中 {@link org.apache.hadoop.mapreduce.Job} 版本的对照实现，
 * 用于说明 v1 与 v2 API 的差异：
 * <p>配置对象由 {@link Job} 换成了 {@link JobConf}，
 * 作业提交由 {@code job.waitForCompletion()} 换成了 {@link JobClient#runJob}
 * <p>v1 API 已随 Hadoop 1.x 淘汰，保留此版本只为对照学习
 */
public class MaxTemperature {

  /**
   * 作业入口：用 v1 风格的 JobConf 组装并提交作业
   *
   * @param args 恰好两个参数：输入路径、输出路径
   * @throws IOException 配置或运行作业时发生 I/O 错误
   */
  public static void main(String[] args) throws IOException {
    // 参数个数校验
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    // v1 用 JobConf 承载配置；v2 的 Job 内部同样持有一个 Configuration
    JobConf conf = new JobConf(MaxTemperature.class);
    conf.setJobName("Max temperature");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    // 同样需要显式声明输出类型
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // v1 直接提交并等待作业结束，不返回布尔值，故无需处理退出码
    JobClient.runJob(conf);
  }
}
