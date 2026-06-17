package v3;

//Identical to v2 except for v3 mapper

/**
 * 最高温度MapReduce作业驱动器（版本3）
 * <p>
 * 此版本使用v3版本的Mapper，该Mapper增加了计数器功能用于检测超过100度的温度异常值。
 * <p>
 * 功能流程：<br/>
 * 1. 配置MapReduce作业参数<br/>
 * 2. 设置Mapper（带异常检测计数器）<br/>
 * 3. 设置Combiner（本地聚合以减少网络传输）<br/>
 * 4. 设置Reducer（计算全局最高温度）
 * <p>
 * 与v2版本的主要区别：<br/>
 * - v2使用基础Mapper，v3使用带计数器功能的Mapper<br/>
 * - v3的Mapper会检测温度超过100度的情况并记录到Counter
 * <p>
 * Tool接口说明：<br/>
 * 实现Tool接口允许通过ToolRunner统一执行，<br/>
 * 支持GenericOptions（-conf, -D等标准Hadoop选项）
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

/**
 * 最高温度Driver主类
 * <p>
 * 运行方式：<br/>
 * hadoop jar ch06-mr-dev.jar v3.MaxTemperatureDriver [input] [output]
 */
public class MaxTemperatureDriver extends Configured implements Tool {

  /**
   * 执行MapReduce作业
   * <p>
   * 参数校验：必须提供输入和输出路径<br/>
   * 创建作业实例并设置名称<br/>
   * 设置输入输出路径<br/>
   * 设置Mapper、Combiner、Reducer类<br/>
   * 设置输出键值类型<br/>
   * 提交作业并等待完成
   *
   * @param args 命令行参数，args[0]为输入路径，args[1]为输出路径
   * @return 作业成功返回0，失败返回1
   * @throws Exception 作业执行中的异常
   */
  @Override
  public int run(String[] args) throws Exception {
    // 参数校验：必须提供输入和输出路径
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    // 创建作业实例并设置名称
    Job job = new Job(getConf(), "Max temperature");
    // 设置驱动类，用于确定JAR包
    job.setJarByClass(getClass());

    // 设置输入输出路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 设置Mapper、Combiner、Reducer类
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    // 设置输出键值类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // 提交作业并等待完成
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 程序入口点
   * <p>
   * 使用ToolRunner运行，支持GenericOptions
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