package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.*;

/**
 * 统计温度的分布情况（v1 版）
 *
 * <p>把气温除以 10 就得到「温度段」（例如 250 表示 250 度那一度），
 * 再统计每个温度段出现了多少条记录
 * <p>这样的直方图正是绘制温度分布图所需要的数据
 */
public class TemperatureDistribution extends Configured implements Tool {
  
  /**
   * 以温度段为键、计数为值的 Mapper
   *
   * <p>每条记录都贡献一个计数，值可以复用同一个常量对象
   */
  static class TemperatureCountMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, LongWritable> {
  
    // 每条记录贡献一个计数，这个对象可以复用
    private static final LongWritable ONE = new LongWritable(1);
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 解析记录并以温度段为键输出计数
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param output 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      
      // 解析这一行记录
      parser.parse(value);
      // 只有读数有效的记录才统计
      if (parser.isValidTemperature()) {
        // 键是温度段（气温除以 10），值是计数 1
        output.collect(new IntWritable(parser.getAirTemperature() / 10), ONE);
      }
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
    
    // 指定 Mapper
    conf.setMapperClass(TemperatureCountMapper.class);
    // 在 Map 端先做一次求和，减少网络传输量
    conf.setCombinerClass(LongSumReducer.class);
    // Reducer 也只做求和
    conf.setReducerClass(LongSumReducer.class);
    // 输出键是温度段
    conf.setOutputKeyClass(IntWritable.class);
    // 输出值是该温度段的记录数
    conf.setOutputValueClass(LongWritable.class);

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
    int exitCode = ToolRunner.run(new TemperatureDistribution(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
