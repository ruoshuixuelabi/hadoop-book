package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

/**
 * 把天气记录与站点名称连接起来的作业（v1 版）
 *
 * <p>连接的关键在于两处设置：
 * 分区器只按站号分区，分组比较器也只比较站号——
 * 否则同一站的两类记录会被拆到不同分组里，连接就失败了
 * <p>v1 的配置全靠 {@link JobConf}，
 * 作业也由 {@link JobClient} 提交
 */
public class JoinRecordWithStationName extends Configured implements Tool {
  
  /**
   * 只按站号分区的分区器
   *
   * <p>复合键的第二段（标记）不参与分区，
   * 保证同一站点的天气记录与站点信息一定进同一个分区
   */
  public static class KeyPartitioner implements Partitioner<TextPair, Text> {
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
     * @param key 复合键，第一段是站号
     * @param value 输入的值
     * @param numPartitions 分区总数
     * @return 分区编号
     */
    @Override
    public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
      // 只用键的第一段（站号）做哈希，两类记录才会进同一个分区
      return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
    }
  }
  
  /**
   * 运行作业
   *
   * @param args 依次为 NCDC 输入、站点输入、输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 本例需要两个输入路径加一个输出路径
    if (args.length != 3) {
      // 打印用法提示
      JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
      // 参数错误，返回 -1
      return -1;
    }
    
    // v1 用 JobConf 承载全部配置，第二个参数是作业的主类
    JobConf conf = new JobConf(getConf(), getClass());
    // 设置作业名，会显示在作业监控页面上
    conf.setJobName("Join record with station name");
    
    // NCDC 天气数据目录
    Path ncdcInputPath = new Path(args[0]);
    // 站点元数据目录
    Path stationInputPath = new Path(args[1]);
    // 输出目录
    Path outputPath = new Path(args[2]);
    
    // 第一个输入是天气记录，用 JoinRecordMapper 打标记
    MultipleInputs.addInputPath(conf, ncdcInputPath,
        TextInputFormat.class, JoinRecordMapper.class);
    // 第二个输入是站点信息，用 JoinStationMapper 打标记
    MultipleInputs.addInputPath(conf, stationInputPath,
        TextInputFormat.class, JoinStationMapper.class);
    // 输出路径只能设置一次
    FileOutputFormat.setOutputPath(conf, outputPath);

    // 用自定义分区器：只按站号分区，两类记录才会聚到一起
    // 分组时也只比较键的第一段，即站号
    /*[*/conf.setPartitionerClass(KeyPartitioner.class);
    conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);/*]*/
    
    // Map 输出的键是复合键（站号 + 标记）
    conf.setMapOutputKeyClass(TextPair.class);
    
    // 指定 Reducer：它把站点名与天气记录拼成一行
    conf.setReducerClass(JoinReducer.class);

    // 最终输出的键是站号
    conf.setOutputKeyClass(Text.class);
    
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
    int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
