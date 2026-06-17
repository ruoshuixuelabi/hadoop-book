// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MaxTemperature - 查找气象数据集中的最高温度
 *
 * 这是Hadoop MapReduce的入门示例程序，用于从气象数据中找出每年的最高温度。
 *
 * <p>使用方法：</p>
 * <pre>
 * hadoop jar hadoop-book-ch02-mr-intro.jar MaxTemperature <输入路径> <输出路径>
 * </pre>
 *
 * <p>输入数据格式：</p>
 * 气象数据采用固定宽度格式，关键字段位置如下：
 * <ul>
 *   <li>位置15-18：年份</li>
 *   <li>位置87-91：气温（可能带前导+号）</li>
 *   <li>位置92：数据质量标记</li>
 * </ul>
 *
 * <p>输出结果：</p>
 * 每行包含年份和该年的最高温度，例如：
 * <pre>
 * 1949    111
 * 1950    22
 * </pre>
 *
 * @author Hadoop Book Examples
 */
public class MaxTemperature {

  /**
   * Main方法 - MapReduce作业入口点
   *
   * <p>此方法负责初始化并执行MapReduce作业：</p>
   * <ol>
   *   <li>验证命令行参数（需要输入路径和输出路径）</li>
   *   <li>创建Job对象并配置作业参数</li>
   *   <li>设置Mapper、Reducer类以及输入输出类型</li>
   *   <li>提交作业并等待完成</li>
   * </ol>
   *
   * @param args 命令行参数，args[0]为输入路径，args[1]为输出路径
   * @throws Exception 如果作业执行过程中发生错误
   */
  public static void main(String[] args) throws Exception {
    // 检查命令行参数数量，确保用户提供了输入和输出路径
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }

    // 创建新的Job作业实例，用于描述MapReduce作业的配置
    Job job = new Job();
    // 设置运行此作业的JAR类，Hadoop会根据这个类找到对应的JAR包
    job.setJarByClass(MaxTemperature.class);
    // 设置作业名称，便于在YARN界面中识别
    job.setJobName("Max temperature");

    // 设置输入路径（可以是文件或目录）
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 设置输出路径（必须是不存在的目录）
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 指定Mapper类：MaxTemperatureMapper负责解析气象记录，提取年份和温度
    job.setMapperClass(MaxTemperatureMapper.class);
    // 指定Reducer类：MaxTemperatureReducer负责找出每个年份的最高温度
    job.setReducerClass(MaxTemperatureReducer.class);

    // 设置输出键的类型为Text（年份字符串，如"1949"）
    job.setOutputKeyClass(Text.class);
    // 设置输出值的类型为IntWritable（温度整数）
    job.setOutputValueClass(IntWritable.class);

    // 提交作业并等待完成，成功返回0，失败返回1
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperature
