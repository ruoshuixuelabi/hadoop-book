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
 * Hadoop旧API计算最高气温的MapReduce程序入口类.
 *
 * <p>该程序用于从气象数据集中找出每年的最高温度。它采用典型的MapReduce模式:
 * <ul>
 *   <li>Map阶段: 解析原始气象记录,提取年份和气温数据</li>
 *   <li>Reduce阶段: 对每年的气温取最大值,得到年度最高气温</li>
 * </ul>
 *
 * <p>输入数据格式为NCDC气象站标准格式,每行包含:
 * <ul>
 *   <li>位置: 第15-18字符为年份</li>
 *   <li>位置: 第87-91字符为气温值(含符号位)</li>
 *   <li>位置: 第92字符为质量码</li>
 * </ul>
 *
 * <p>用法示例:
 * <pre>
 * hadoop jar hadoop-examples.jar MaxTemperature /ncdc/input /ncdc/output
 * </pre>
 *
 * <p>注意: 该类使用org.apache.hadoop.mapred包下的旧API,
 * 新代码应使用org.apache.hadoop.mapreduce包的新API.
 *
 * @see MaxTemperatureMapper
 * @see MaxTemperatureReducer
 */
public class MaxTemperature {

  /**
   * 程序主入口,负责配置并启动MapReduce作业.
   *
   * <p>该方法执行以下操作:
   * <ol>
   *   <li>验证命令行参数个数(需要输入路径和输出路径两个参数)</li>
   *   <li>创建JobConf配置对象,设置作业名称</li>
   *   <li>配置输入输出路径、Mapper、Reducer等作业参数</li>
   *   <li>提交作业给JobClient执行</li>
   * </ol>
   *
   * @param args 命令行参数,args[0]为输入数据路径,args[1]为输出路径
   * @throws IOException 如果发生IO错误(如输入路径不存在)
   */
  public static void main(String[] args) throws IOException {
    // 检查命令行参数个数,必须提供输入路径和输出路径
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }

    // 创建JobConf配置对象,指定作业名称为"Max temperature"
    JobConf conf = new JobConf(MaxTemperature.class);
    conf.setJobName("Max temperature");

    // 设置输入路径: 从文件系统的指定路径读取输入数据
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    // 设置输出路径: MapReduce输出的结果写入到指定路径
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // 指定Map处理类: 解析气象记录,输出<年份, 气温>键值对
    conf.setMapperClass(MaxTemperatureMapper.class);
    // 指定Reduce处理类: 对每个年份找出最高气温
    conf.setReducerClass(MaxTemperatureReducer.class);

    // 设置输出键的类型为Text(年份字符串)
    conf.setOutputKeyClass(Text.class);
    // 设置输出值的类型为IntWritable(气温整数值)
    conf.setOutputValueClass(IntWritable.class);

    // 提交作业给JobTracker执行,阻塞直到作业完成
    JobClient.runJob(conf);
  }
}