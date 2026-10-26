package v2;

// cc MaxTemperatureDriverV2 Application to find the maximum temperature
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
/**
 * 用 v2 版 Mapper/Reducer 计算最高气温的驱动类
 *
 * <p>Driver 负责搭起作业的骨架：指定输入输出、
 * 设置 Mapper/Combiner/Reducer、以及键值类型
 * <p>注意 Combiner 和 Reducer 用的是同一个类：
 * 求最大值这个操作在 Map 端先做一遍和只在 Reduce 端做一遍，结果一样，
 * 但前者能减少 Shuffle 的数据量
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
    // 告诉框架作业代码在哪个类里
    job.setJarByClass(getClass());

    // 指定输入路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 指定输出路径
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // v2 的 Mapper：会先校验温度是否有效，无效的记录直接跳过
    job.setMapperClass(MaxTemperatureMapper.class);
    // Combiner 在 Map 端先做一次归并，能显著减少 Shuffle 的数据量
    job.setCombinerClass(MaxTemperatureReducer.class);
    // Reducer 做的才是真正的汇总
    job.setReducerClass(MaxTemperatureReducer.class);

    // 输出键是年份（Text）
    job.setOutputKeyClass(Text.class);
    // 输出值是气温（IntWritable）
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
// ^^ MaxTemperatureDriverV2
