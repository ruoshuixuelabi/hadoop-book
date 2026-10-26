package v4;

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

//Identical to v3 except for v4 mapper
/**
 * 用 v4 版 Mapper 计算最高气温的驱动类
 *
 * <p>与 v3 唯一的区别是换用了 v4 的 Mapper：
 * v4 只把「格式损坏」的记录算作坏数据，
 * 「温度缺失」的记录仍按正常数据处理
 */
public class MaxTemperatureDriver extends Configured implements Tool {

  /**
   * 组装并运行求最高气温的作业
   *
   * @param args 第一个参数是输入路径，第二个是输出路径
   * @return 0 表示作业成功，1 表示失败，-1 表示参数错误
   * @throws Exception 作业提交或运行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 参数个数不对
    if (args.length != 2) {
      // 用法写到 stderr
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    // 作业名会显示在 YARN 的 Web UI 上
    Job job = new Job(getConf(), "Max temperature");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // 换成 v4 的 Mapper：它只对格式损坏的记录报警
    job.setMapperClass(MaxTemperatureMapper.class);
    // Combiner 与 Reducer 仍沿用 v1 的实现
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // true 表示把进度打印到控制台
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * 命令行入口
   *
   * @param args 第一个参数是输入路径，第二个是输出路径
   * @throws Exception 运行失败
   */
  public static void main(String[] args) throws Exception {
    // 交给 ToolRunner 运行，以便支持通用选项
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}
