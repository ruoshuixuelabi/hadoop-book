// == PartitionByStationYearUsingMultipleOutputs
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 按「站点 + 年份」两级目录输出
 *
 * <p>在上一例「每站一个文件」的基础上再细分一层：
 * 输出目录会变成 {@code 站号/年份/part-m-xxxxx} 这样的结构
 * <p>做法相同——只是传给 MultipleOutputs 的标签换成了带路径的字符串
 */
public class PartitionByStationYearUsingMultipleOutputs extends Configured
  implements Tool {
  
  /**
   * 以站号为键的 Mapper
   */
  static class StationMapper
    extends Mapper<LongWritable, Text, Text, Text> {
  
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 解析记录并以站号为键输出
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // 解析这一行记录，取出站号等字段
      parser.parse(value);
      // 键是站号，值仍是原始记录
      context.write(new Text(parser.getStationId()), value);
    }
  }
  
  /**
   * 把记录写进「站号/年份」子目录
   *
   * <p>输出键同样是 {@link org.apache.hadoop.io.NullWritable}，
   * 因为「站号 + 年份」这个组合已经由文件路径表达了
   */
  static class MultipleOutputsReducer
    extends Reducer<Text, Text, NullWritable, Text> {
    
    // MultipleOutputs 实例：setup 里创建，cleanup 里关闭
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    // 解析器实例；这里还要靠它取出年份来拼输出路径
    private NcdcRecordParser parser = new NcdcRecordParser();

    /**
     * 创建 MultipleOutputs 实例
     *
     * <p>它必须绑定到当前任务的 Context，否则写不出结果
     *
     * @param context 任务上下文
     * @throws IOException 创建失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      // 绑定到当前任务的上下文
      multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

// vv PartitionByStationYearUsingMultipleOutputs
    /**
     * 把记录写进「站号/年份」子目录
     *
     * @param key 分组键，即站号
     * @param values 该站点的所有记录
     * @param context 上下文
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // 逐条解析，因为要拿到年份才能拼出目标路径
      for (Text value : values) {
        parser.parse(value);
        // 标签里带斜杠，MultipleOutputs 会把它当成目录层级
        String basePath = String.format("%s/%s/part",
            parser.getStationId(), parser.getYear());
        // 三个参数分别是输出键、输出值和标签（标签决定写到哪个文件）
        multipleOutputs.write(NullWritable.get(), value, basePath);
      }
    }
// ^^ PartitionByStationYearUsingMultipleOutputs
    
    /**
     * 关闭 MultipleOutputs，刷新尚未写完的文件
     *
     * @param context 任务上下文
     * @throws IOException 关闭失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      // 不关闭的话，缓冲区里的数据可能还没落盘
      multipleOutputs.close();
    }
  }

  /**
   * 运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 公共工具类：解析参数、构造 Job 并设置输入输出路径
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    // 参数不对时该方法会返回 null
    if (job == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 指定 Mapper
    job.setMapperClass(StationMapper.class);
    // Map 输出的键是站号
    job.setMapOutputKeyClass(Text.class);
    // 指定 Reducer：它负责按标签把数据分流到不同文件
    job.setReducerClass(MultipleOutputsReducer.class);
    // 最终输出的键为空：文件名才是分组的依据
    job.setOutputKeyClass(NullWritable.class);

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
    int exitCode = ToolRunner.run(new PartitionByStationYearUsingMultipleOutputs(),
        args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
