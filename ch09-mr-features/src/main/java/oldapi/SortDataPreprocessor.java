package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 把天气数据转换成以气温为键的 SequenceFile（v1 版）
 *
 * <p>这是排序作业的前置步骤：
 * 原始记录是定宽文本，先解析成「气温 → 原始记录」，
 * 后面才能按气温排序
 * <p>本例只需要 Map 阶段（Reduce 任务数设为 0），
 * 因为分组聚合在这里没有意义
 */
public class SortDataPreprocessor extends Configured implements Tool {
  
  /**
   * 清洗数据：以气温为键输出原始记录
   *
   * <p>无效读数（缺测或可疑）直接丢弃
   */
  static class CleanerMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, Text> {
  
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 解析记录并以气温为键输出
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param output 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      
      // 解析这一行记录
      parser.parse(value);
      // 只有读数有效的记录才输出
      if (parser.isValidTemperature()) {
        // 键是气温，值是原始记录
        output.collect(new IntWritable(parser.getAirTemperature()), value);
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
    conf.setMapperClass(CleanerMapper.class);
    // 输出键是气温
    conf.setOutputKeyClass(IntWritable.class);
    // 输出值是原始记录
    conf.setOutputValueClass(Text.class);
    // 不需要 Reduce：本作业只做格式转换
    conf.setNumReduceTasks(0);
    // 输出成 SequenceFile
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    // 打开输出压缩
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    // 用 Gzip 压缩
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    // 按块压缩
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);

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
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
