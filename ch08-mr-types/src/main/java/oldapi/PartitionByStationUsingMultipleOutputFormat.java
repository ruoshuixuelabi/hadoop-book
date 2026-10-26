package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.*;

/**
 * 用 MultipleTextOutputFormat 把数据按站点拆成多个文件（v1 旧 API）
 *
 * <p>一个 Reduce 任务的输出通常只能写到一个文件里（part-r-xxxxx），
 * 但「每个气象站一个文件」是常见需求
 * <p>v1 的做法是换掉输出格式：继承
 * {@link org.apache.hadoop.mapred.lib.MultipleTextOutputFormat}，
 * 由它来决定每个键值对写到哪个文件
 */
public class PartitionByStationUsingMultipleOutputFormat extends Configured
  implements Tool {
  
  /**
   * 以站号为键的 Mapper
   *
   * <p>它不改变记录内容，只把站号提取出来当键，
   * 好让同一个站的数据在 Reducer 阶段聚到一起
   */
  static class StationMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {
  
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 解析记录并以站号为键输出
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param output 结果收集器，用来把键值对写出
     * @param reporter 状态上报器
     * @throws IOException 写出失败
     */
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      
      // 解析这一行记录，取出站号等字段
      parser.parse(value);
      // 键是站号，值仍是原始记录
      output.collect(new Text(parser.getStationId()), value);
    }
  }
  
  /**
   * 把同一个站点的记录原样写出
   *
   * <p>输出的键是 {@link org.apache.hadoop.io.NullWritable}——
   * 每个站点一个文件，文件名已经承载了分组信息，键不再有意义
   */
  static class StationReducer extends MapReduceBase
    implements Reducer<Text, Text, NullWritable, Text> {

    /**
     * 把本组的每条记录写出去
     *
     * @param key 分组键，即站号
     * @param values 该站点的所有记录
     * @param output 结果收集器
     * @param reporter 状态上报器
     * @throws IOException 写出失败
     */
    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException {
      // 把本组的每条记录都写出去
      while (values.hasNext()) {
        // 输出的键为空：文件名才是分组的依据
        output.collect(NullWritable.get(), values.next());
      }
    }
  }
  
  /**
   * 按站号决定输出文件名的输出格式
   *
   * <p>它继承 {@code MultipleTextOutputFormat}，
   * 只重写「文件名怎么来」这一个方法
   */
  /*[*/static class StationNameMultipleTextOutputFormat
    extends MultipleTextOutputFormat<NullWritable, Text> {
    
    // 解析器实例：用来从记录里取出站号
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 由记录算出输出文件名
     *
     * @param key 输出键（为空）
     * @param value 输出值，即一行 NCDC 记录
     * @param name 默认文件名
     * @return 用站号命名的文件名
     */
    protected String generateFileNameForKeyValue(NullWritable key, Text value,
        String name) {
      // 从记录里解析出站号
      parser.parse(value);
      // 用站号作为文件名
      return parser.getStationId();
    }
  }/*]*/

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
    // 参数不对时该方法会返回 null
    if (conf == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 指定 Mapper
    conf.setMapperClass(StationMapper.class);
    // Map 输出的键是站号
    conf.setMapOutputKeyClass(Text.class);
    // 指定 Reducer
    conf.setReducerClass(StationReducer.class);
    // 最终输出的键为空：文件名才是分组的依据
    conf.setOutputKeyClass(NullWritable.class);
    // 指定输出格式：它决定每个键值对写到哪个文件
    conf.setOutputFormat(StationNameMultipleTextOutputFormat.class);

    // 提交作业并阻塞等待它跑完
    JobClient.runJob(conf);
    // 作业跑完即返回成功
    return 0;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项后调用 run
    int exitCode = ToolRunner.run(
        new PartitionByStationUsingMultipleOutputFormat(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
