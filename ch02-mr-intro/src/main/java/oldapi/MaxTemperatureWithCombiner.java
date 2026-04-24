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
 * Hadoop旧API计算最高气温的MapReduce程序,带有Combiner优化.
 *
 * <p>该程序是MaxTemperature的增强版本,在Mapper和Reducer之间
 * 添加了Combiner阶段以减少网络传输的数据量.
 *
 * <p>与标准MaxTemperature的区别:
 * <ul>
 *   <li>标准版: Mapper输出直接发送到Reducer</li>
 *   <li>Combiner版: Mapper输出先在本地合并,再发送到Reducer</li>
 * </ul>
 *
 * <p>Combiner的作用:
 * <ul>
 *   <li>在Map节点本地先对相同key的数据进行reduce操作</li>
 *   <li>减少shuffle阶段需要传输的数据量</li>
 *   <li>降低网络带宽消耗,提升作业执行效率</li>
 * </ul>
 *
 * <p>对于求最大值场景,Combiner和Reducer可以使用相同的类,
 * 因为max操作满足结合律和交换律.
 *
 * <p>用法示例:
 * <pre>
 * hadoop jar hadoop-examples.jar MaxTemperatureWithCombiner /ncdc/input /ncdc/output
 * </pre>
 *
 * @see MaxTemperatureMapper
 * @see MaxTemperatureReducer
 */
public class MaxTemperatureWithCombiner {

  /**
   * 程序主入口,配置并启动带有Combiner的MapReduce作业.
   *
   * <p>作业配置与标准MaxTemperature类似,但额外设置了Combiner类.
   * Combiner使用与Reducer相同的MaxTemperatureReducer类,
   * 因为求最大值操作满足结合律,可以局部合并后再全局合并.
   *
   * <p>配置流程:
   * <ol>
   *   <li>验证命令行参数</li>
   *   <li>创建JobConf并设置作业名称</li>
   *   <li>设置输入输出路径</li>
   *   <li>配置Mapper类</li>
   *   <li>配置Combiner类(关键优化点)</li>
   *   <li>配置Reducer类</li>
   *   <li>设置输出键值类型</li>
   *   <li>提交作业</li>
   * </ol>
   *
   * @param args 命令行参数,args[0]为输入路径,args[1]为输出路径
   * @throws IOException 如果发生IO错误
   */
  public static void main(String[] args) throws IOException {
    // 检查命令行参数个数
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
      		"<output path>");
      System.exit(-1);
    }

    // 创建JobConf配置对象,指定作业名称
    JobConf conf = new JobConf(MaxTemperatureWithCombiner.class);
    conf.setJobName("Max temperature");

    // 设置输入路径和输出路径
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // 配置Mapper类: 解析气象数据
    conf.setMapperClass(MaxTemperatureMapper.class);

    // /*[*/配置Combiner类: 在Map端先做局部合并/*]*/
    // Combiner与Reducer使用相同的类,因为max操作可以局部合并
    // 这可以显著减少shuffle阶段的网络传输量
    conf.setCombinerClass(MaxTemperatureReducer.class);

    // 配置Reducer类: 最终汇总计算年度最高气温
    conf.setReducerClass(MaxTemperatureReducer.class);

    // 设置输出键值类型
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // 提交作业给JobTracker执行
    JobClient.runJob(conf);
  }
}