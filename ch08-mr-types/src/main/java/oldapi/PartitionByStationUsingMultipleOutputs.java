package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

/**
 * 用 MultipleOutputs 把数据按站点拆成多个文件（v1 旧 API）
 *
 * <p>一个 Reduce 任务的输出通常只能写到一个文件里（part-r-xxxxx），
 * 但「每个气象站一个文件」是常见需求
 * <p>v1 的 {@link org.apache.hadoop.mapred.lib.MultipleOutputs} 允许
 * 按名字登记多套输出，Reducer 再为每个键值对挑一套来写
 */
public class PartitionByStationUsingMultipleOutputs extends Configured
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
   * 把同一个站点的记录写进以站号命名的文件
   *
   * <p>与新 API 的写法不同：v1 要先在 {@code configure} 里创建
   * {@code MultipleOutputs}，再在 {@code reduce} 里按名字取收集器
   */
  static class MultipleOutputsReducer extends MapReduceBase
    implements Reducer<Text, Text, NullWritable, Text> {
    
    // v1 的 MultipleOutputs 实例：在 configure 里创建，close 里关闭
    private MultipleOutputs multipleOutputs;

    /**
     * 创建 MultipleOutputs 实例
     *
     * @param conf 作业配置
     */
    @Override
    public void configure(JobConf conf) {
      // v1 没有 setup 钩子，只能在 configure 里创建
      multipleOutputs = new MultipleOutputs(conf);
    }

    /**
     * 把本组记录写进以站号命名的文件
     *
     * @param key 分组键，即站号
     * @param values 该站点的所有记录
     * @param output 结果收集器
     * @param reporter 状态上报器
     * @throws IOException 写出失败
     */
    @SuppressWarnings("unchecked")
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException {
      
      // 按名字「station」取收集器，第二个参数是文件名
      // 这个键就是 Mapper 输出的站号，格式恒为「6 位 + 短横线 + 5 位」，
      // 去掉短横线后仍是纯数字，不会带进路径分隔符，
      // 因此拼出来的文件名一定直接落在作业的输出目录里。
      OutputCollector collector = multipleOutputs.getCollector("station",
          key.toString().replace("-", ""), reporter);
      // 把本组的每条记录都写出去
      while (values.hasNext()) {
        // 输出的键为空：文件名才是分组的依据
        collector.collect(NullWritable.get(), values.next());        
      }
    }
    
    /**
     * 关闭 MultipleOutputs，刷新尚未写完的文件
     *
     * @throws IOException 关闭失败
     */
    @Override
    public void close() throws IOException {
      // 不关闭的话，缓冲区里的数据可能还没落盘
      multipleOutputs.close();
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
    // 参数不对时该方法会返回 null
    if (conf == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 指定 Mapper
    conf.setMapperClass(StationMapper.class);
    // Map 输出的键是站号
    conf.setMapOutputKeyClass(Text.class);
    // 指定 Reducer：它负责按名字把数据分流到不同文件
    conf.setReducerClass(MultipleOutputsReducer.class);
    // 最终输出的键为空：文件名才是分组的依据
    conf.setOutputKeyClass(NullWritable.class);
    // 真正的输出由 MultipleOutputs 负责，
    // NullOutputFormat 只是用来抑制那个空的结果文件
    conf.setOutputFormat(NullOutputFormat.class); // suppress empty part file
    
    // 登记一套名为「station」的输出：键类型 NullWritable，值类型 Text
    MultipleOutputs.addMultiNamedOutput(conf, "station", TextOutputFormat.class,
        NullWritable.class, Text.class);

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
    int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
        args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
