package oldapi;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 按站点求最高气温，并用 DistributedCache 里的站点表换出站名（v1 版）
 *
 * <p>输出里想要站名而不是站号，但站名表在 HDFS 上，
 * 每个 Reducer 都去读一次代价太高
 * <p>DistributedCache 可以在作业启动时把一个小文件分发到
 * 每个任务节点的本地磁盘，之后就像读本地文件一样读它
 * <p>v1 里对应的生命周期回调是 {@code configure}，
 * 它在新 API 中叫 {@code setup}
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFile
  extends Configured implements Tool {
  
  /**
   * 以站号为键输出气温的 Mapper
   *
   * <p>与 ch08 的同类 Mapper 一样，只输出有效读数
   */
  static class StationTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {

    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 解析记录并以站号为键输出气温
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param output 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      // 解析这一行记录
      parser.parse(value);
      // 只有读数有效的记录才输出
      if (parser.isValidTemperature()) {
        // 键是站号，值是气温
        output.collect(new Text(parser.getStationId()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }
  
  /**
   * 求最高气温，并用站点表把站号换成站名
   *
   * <p>站点表在 configure 里加载一次，
   * 每个 Reducer 任务只需读一遍这个文件
   */
  static class MaxTemperatureReducerWithStationLookup extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    // 站点元数据表：站号 → 站名
    /*[*/private NcdcStationMetadata metadata;/*]*/
    
    /**
     * 加载站点表
     *
     * <p>DistributedCache 已经把文件分发到任务节点的本地磁盘，
     * 这里按本地文件路径打开即可
     *
     * @param conf 作业配置
     */
    /*[*/@Override
    public void configure(JobConf conf) {
      metadata = new NcdcStationMetadata();
      try {
        metadata.initialize(new File("stations-fixed-width.txt"));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }/*]*/

    /**
     * 求最高气温并以站名输出
     *
     * @param key 分组键，即站号
     * @param values 该站点的所有气温
     * @param output 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      // 用站点表把站号换成站名
      /*[*/String stationName = metadata.getStationName(key.toString());/*]*/
      
      // 初值取最小整数，保证任何合法气温都比它大
      int maxValue = Integer.MIN_VALUE;
      // 遍历本组的所有气温
      while (values.hasNext()) {
        // 保留更大的那个
        maxValue = Math.max(maxValue, values.next().get());
      }
      // 输出的键是站名（不是站号），值是最高气温
      output.collect(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
    }
  }

  /**
   * 运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws IOException 作业执行失败
   */
  @Override
  public int run(String[] args) throws IOException {
    // 公共工具类：解析参数、构造 JobConf 并设置输入输出路径
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    // 返回 null 说明参数个数不对
    if (conf == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 最终输出的键是站名
    conf.setOutputKeyClass(Text.class);
    // 值是最高气温
    conf.setOutputValueClass(IntWritable.class);

    // 指定 Mapper
    conf.setMapperClass(StationTemperatureMapper.class);
    // 在 Map 与 Reduce 之间先做一次合并，减少网络传输量
    conf.setCombinerClass(MaxTemperatureReducer.class);
    // 指定 Reducer：它用站点表把站号换成站名
    conf.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
    
    // v1 由 JobClient 提交并运行作业
    JobClient.runJob(conf);
    // 运行成功
    return 0;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int exitCode = ToolRunner.run(
        new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
