package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 用二级排序求每年的最高气温（v1 版）
 *
 * <p>Reduce 端只能拿到「键相同」的一组值，
 * 要在组内再挑出最高气温，就得靠排序：
 * 把「年份 + 气温」一起写进复合键，
 * 用一个自定义比较器让年份升序、同年气温降序，
 * 这样每组的第一条就是最高气温
 * <p>v1 里排序与分组分别由
 * {@code setOutputKeyComparatorClass} 与
 * {@code setOutputValueGroupingComparator} 指定
 */
public class MaxTemperatureUsingSecondarySort
  extends Configured implements Tool {
  
  /**
   * 把「年份 + 气温」写进复合键的 Mapper
   *
   * <p>值写成 {@link NullWritable} 是为了省掉无意义的数据传输
   */
  static class MaxTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntPair, NullWritable> {
  
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 解析记录并输出「年份 + 气温」复合键
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param output 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void map(LongWritable key, Text value,
        OutputCollector<IntPair, NullWritable> output, Reporter reporter)
        throws IOException {
      
      // 解析这一行记录
      parser.parse(value);
      // 只有读数有效的记录才输出
      if (parser.isValidTemperature()) {
        // 键的第一段是年份，第二段是气温；值没有意义，用 NullWritable 占位
        /*[*/output.collect(new IntPair(parser.getYearInt(),
            + parser.getAirTemperature()), NullWritable.get());/*]*/
      }
    }
  }
  
  /**
   * 每组只输出第一条记录的 Reducer
   *
   * <p>排序比较器已经把最高气温排在组首，
   * 所以这里只要把键原样写出去
   */
  static class MaxTemperatureReducer extends MapReduceBase
    implements Reducer<IntPair, NullWritable, IntPair, NullWritable> {
  
    /**
     * 输出排序后的第一条记录
     *
     * @param key 复合键，第一段是年份，第二段是最高气温
     * @param values 本组的所有值（本例不用）
     * @param output 结果收集器
     * @param reporter 状态报告器
     * @throws IOException 写出失败
     */
    public void reduce(IntPair key, Iterator<NullWritable> values,
        OutputCollector<IntPair, NullWritable> output, Reporter reporter)
        throws IOException {
      
      // 排序后每组的第一条就是最高气温，原样输出即可
      /*[*/output.collect(key, NullWritable.get());/*]*/
    }
  }
  
  /**
   * 只按年份分区的分区器
   *
   * <p>同一年的气温必须落在同一个分区里，
   * 否则二级排序就无从谈起
   */
  public static class FirstPartitioner
    implements Partitioner<IntPair, NullWritable> {

    /**
     * v1 的分区器也要实现这个回调
     *
     * <p>本例不需要任何初始化，所以方法体是空的
     *
     * @param job 作业配置
     */
    @Override
    public void configure(JobConf job) {}
    
    /**
     * 计算复合键应该分到哪个分区
     *
     * @param key 复合键，第一段是年份
     * @param value 输入的值
     * @param numPartitions 分区总数
     * @return 分区编号
     */
    @Override
    public int getPartition(IntPair key, NullWritable value, int numPartitions) {
      // 只用键的第一段（年份）做哈希
      return Math.abs(key.getFirst() * 127) % numPartitions;
    }
  }
  
  /**
   * 组内排序用的比较器
   *
   * <p>先按年份升序，年份相同时再按气温降序，
   * 于是每年最高的那条会被排在组首
   */
  public static class KeyComparator extends WritableComparator {
    /**
     * 构造比较器：指定键类型，并标记为分组比较器
     */
    protected KeyComparator() {
      super(IntPair.class, true);
    }
    /**
     * 比较两个复合键
     *
     * @param w1 第一个复合键
     * @param w2 第二个复合键
     * @return 小于 0、等于 0 或大于 0，表示先后顺序
     */
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      // 取出要比较的两个复合键
      IntPair ip1 = (IntPair) w1;
      // 第二个复合键
      IntPair ip2 = (IntPair) w2;
      // 先比较第一段：年份
      int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
      // 年份不同就已经分出胜负
      if (cmp != 0) {
        // 直接返回年份的比较结果
        return cmp;
      }
      // 年份相同再比气温，取反实现降序
      return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); //reverse
    }
  }
  
  /**
   * 决定「谁和谁一组」的分组比较器
   *
   * <p>只看年份：年份相同的气温才会被归到同一个分组里
   */
  public static class GroupComparator extends WritableComparator {
    /**
     * 构造比较器：指定键类型，并标记为分组比较器
     */
    protected GroupComparator() {
      super(IntPair.class, true);
    }
    /**
     * 比较两个复合键的年份
     *
     * @param w1 第一个复合键
     * @param w2 第二个复合键
     * @return 小于 0、等于 0 或大于 0
     */
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      // 取出要比较的两个复合键
      IntPair ip1 = (IntPair) w1;
      // 第二个复合键
      IntPair ip2 = (IntPair) w2;
      // 只比较年份，第二段（气温）不参与分组
      return IntPair.compare(ip1.getFirst(), ip2.getFirst());
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
    conf.setMapperClass(MaxTemperatureMapper.class);
    // 只按年份分区，同年的数据必须聚在一起
    /*[*/conf.setPartitionerClass(FirstPartitioner.class);/*]*/
    // 组内排序：先按年份升序，同年再按气温降序
    /*[*/conf.setOutputKeyComparatorClass(KeyComparator.class);/*]*/
    // 分组只看年份，年份相同的气温才会归到一组
    /*[*/conf.setOutputValueGroupingComparator(GroupComparator.class);/*]*/
    // 指定 Reducer
    conf.setReducerClass(MaxTemperatureReducer.class);
    // 输出键是复合键
    conf.setOutputKeyClass(IntPair.class);
    // 输出值没有意义
    conf.setOutputValueClass(NullWritable.class);
    
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
    int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
