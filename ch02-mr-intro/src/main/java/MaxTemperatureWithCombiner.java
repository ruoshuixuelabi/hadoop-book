// cc MaxTemperatureWithCombiner Application to find the maximum temperature, using a combiner function for efficiency
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// vv MaxTemperatureWithCombiner
/**
 * MaxTemperatureWithCombiner - 带Combiner优化的最高温度查找程序
 *
 * <p>功能说明：</p>
 * 此程序是MaxTemperature的优化版本，增加了Combiner函数以减少网络传输数据量。
 *
 * <p>Combiner的作用：</p>
 * <ul>
 *   <li>在Map任务结束后、结果发送到Reducer之前，先在本地进行局部汇总</li>
 *   <li>例如：Mapper输出10条(1949, 100)记录，Combiner可以合并为1条(1949, 100)</li>
 *   <li>显著减少从Mapper节点到Reducer节点的网络传输量</li>
 *   <li>对于求最大值这类操作，Combiner和Reducer可以采用相同的逻辑</li>
 * </ul>
 *
 * <p>执行流程：</p>
 * <pre>
 * 输入数据 → Map阶段 → (本地)Combiner阶段 → Shuffle/Sort → Reducer阶段 → 输出
 * </pre>
 *
 * <p>与MaxTemperature的区别：</p>
 * 本程序在第27行设置了Combiner类：
 * <pre>
 * job.setCombinerClass(MaxTemperatureReducer.class);
 * </pre>
 *
 * <p>使用方法：</p>
 * <pre>
 * hadoop jar app.jar MaxTemperatureWithCombiner <输入路径> <输出路径>
 * </pre>
 *
 * @see MaxTemperature
 * @see MaxTemperatureMapper
 * @see MaxTemperatureReducer
 */
public class MaxTemperatureWithCombiner {

  /**
   * main方法 - 作业入口点
   *
   * <p>配置并执行MapReduce作业，关键区别是设置了CombinerClass。</p>
   *
   * @param args 命令行参数，args[0]为输入路径，args[1]为输出路径
   * @throws Exception 如果作业执行过程中发生错误
   */
  public static void main(String[] args) throws Exception {
    // 验证命令行参数
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
          "<output path>");
      System.exit(-1);
    }

    // 创建并配置Job
    Job job = new Job();
    job.setJarByClass(MaxTemperatureWithCombiner.class);
    job.setJobName("Max temperature");

    // 设置输入输出路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 设置Mapper类
    job.setMapperClass(MaxTemperatureMapper.class);

    // 设置Combiner类 - 这是与普通MaxTemperature的主要区别
    // Combiner在Map端先进行一次局部汇总，减少网络传输
    // 由于是求最大值，Combiner可以使用与Reducer相同的类
    /*[*/job.setCombinerClass(MaxTemperatureReducer.class)/*]*/;

    // 设置Reducer类
    job.setReducerClass(MaxTemperatureReducer.class);

    // 设置输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // 提交作业并等待完成
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperatureWithCombiner
