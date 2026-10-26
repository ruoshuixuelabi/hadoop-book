// cc JoinRecordWithStationName Application to join weather records with station names
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

// vv JoinRecordWithStationName
/**
 * 把天气记录与站点名称连接起来的作业
 *
 * <p>两个输入用不同的 Mapper 打上不同的标记，
 * 但都用同一个复合键「站号 + 标记」输出，
 * 这样同一站点的两类记录就会落到同一个 Reducer 的同一个分组里
 * <p>连接能否成功取决于两处设置：
 * 分区器只按站号分区，分组比较器也只比较站号——
 * 否则同一站的两类记录会被拆到不同分组里，配对就失败了
 */
public class JoinRecordWithStationName extends Configured implements Tool {
  
  /**
   * 只按站号分区的分区器
   *
   * <p>复合键的第二段（标记）不参与分区，
   * 保证同一站点的天气记录与站点信息一定进同一个分区
   */
  public static class KeyPartitioner extends Partitioner<TextPair, Text> {
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
    
    // 第二个参数是作业名，会显示在作业监控页面上
    Job job = new Job(getConf(), "Join weather records with station names");
    // 指定作业 jar 包的位置
    job.setJarByClass(getClass());
    
    // NCDC 天气数据目录
    Path ncdcInputPath = new Path(args[0]);
    // 站点元数据目录
    Path stationInputPath = new Path(args[1]);
    // 输出目录
    Path outputPath = new Path(args[2]);
    
    // 第一个输入是天气记录，用 JoinRecordMapper 打标记
    MultipleInputs.addInputPath(job, ncdcInputPath,
        TextInputFormat.class, JoinRecordMapper.class);
    // 第二个输入是站点信息，用 JoinStationMapper 打标记
    MultipleInputs.addInputPath(job, stationInputPath,
        TextInputFormat.class, JoinStationMapper.class);
    // 输出路径只能设置一次
    FileOutputFormat.setOutputPath(job, outputPath);
    
    // 用自定义分区器：只按站号分区，两类记录才会聚到一起
    // 分组时也只比较键的第一段，即站号
    /*[*/job.setPartitionerClass(KeyPartitioner.class);
    job.setGroupingComparatorClass(TextPair.FirstComparator.class);/*]*/
    
    // Map 输出的键是复合键（站号 + 标记）
    job.setMapOutputKeyClass(TextPair.class);
    
    // 指定 Reducer：它把站点名与天气记录拼成一行
    job.setReducerClass(JoinReducer.class);

    // 最终输出的键是站号
    job.setOutputKeyClass(Text.class);
    
    // 提交作业并等待它跑完
    return job.waitForCompletion(true) ? 0 : 1;
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
// ^^ JoinRecordWithStationName
