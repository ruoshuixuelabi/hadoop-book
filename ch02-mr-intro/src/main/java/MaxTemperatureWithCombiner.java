// cc MaxTemperatureWithCombiner Application to find the maximum temperature, using a combiner function for efficiency
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// vv MaxTemperatureWithCombiner
/**
 * 统计每个气象站的最高气温，并使用 Combiner 提前归约以提升效率
 *
 * <p>与 {@link MaxTemperature} 的唯一差别是本类设置了 Combiner：
 * Map 端先在本地做一次局部归约，把发往网络的数据量降到最小
 * <p>Combiner 与 Reducer 用同一个类，是因为「求最大值」既满足结合律又满足交换律，
 * 局部预归约不会改变最终结果；若聚合函数不满足这两个条件，使用 Combiner 会得到错误答案
 */
public class MaxTemperatureWithCombiner {

  /**
   * 作业入口：组装并运行带 Combiner 的最高气温统计作业
   *
   * @param args 恰好两个参数：输入路径、输出路径
   * @throws Exception 作业配置或执行过程中发生 Hadoop 层面的错误
   */
  public static void main(String[] args) throws Exception {
    // 参数个数校验
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
          "<output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(MaxTemperatureWithCombiner.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    /*[*/job.setCombinerClass(MaxTemperatureReducer.class)/*]*/;
    job.setReducerClass(MaxTemperatureReducer.class);

    // 输出键值类型需在提交前声明，Hadoop 据此完成序列化器的协商
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperatureWithCombiner
