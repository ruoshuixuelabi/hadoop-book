// cc MaxTemperatureWithCounters Application to run the maximum temperature job, including counting missing and malformed fields and quality codes
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureWithCounters
/**
 * 求最高气温，同时用计数器统计缺测与格式错误的记录
 *
 * <p>MapReduce 的计数器（Counter）由框架在任务里累加，
 * 作业结束后可以从作业历史里读回来
 * <p>本例用它来衡量数据质量：
 * 有多少条记录的温度字段缺测、多少条格式可疑，
 * 再按质量位分组统计各类读数的数量
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {
  
  /**
   * 计数器分组：把「缺测」与「格式错误」分开统计
   */
  enum Temperature {
    MISSING,
    MALFORMED
  }
  
  /**
   * 求最高气温并统计各类计数器
   *
   * <p>有效读数照常输出，同时把可疑数据分类计数
   */
  static class MaxTemperatureMapperWithCounters
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
  
    /**
     * 解析记录并更新计数器
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param context 上下文，用来写出结果并累加计数器
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      // 解析这一行记录
      parser.parse(value);
      // 有效读数：照常输出
      if (parser.isValidTemperature()) {
        // 取出气温
        int airTemperature = parser.getAirTemperature();
        // 键是年份，值是气温
        context.write(new Text(parser.getYear()),
            new IntWritable(airTemperature));
      // 温度字段有值但不可信
      } else if (parser.isMalformedTemperature()) {
        // 把可疑记录打印出来，便于排查
        System.err.println("Ignoring possibly corrupt input: " + value);
        // 计数器加一：格式错误的记录数
        context.getCounter(Temperature.MALFORMED).increment(1);
      // 温度字段缺测
      } else if (parser.isMissingTemperature()) {
        // 计数器加一：缺测的记录数
        context.getCounter(Temperature.MISSING).increment(1);
      }
      
      // dynamic counter
      // 计数器名由质量位拼成，因此每种质量位各有一个计数器
      context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
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
    // 返回 null 说明参数个数不对
    if (job == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 输出键是年份
    job.setOutputKeyClass(Text.class);
    // 输出值是气温
    job.setOutputValueClass(IntWritable.class);

    // 指定带计数器的 Mapper
    job.setMapperClass(MaxTemperatureMapperWithCounters.class);
    // 在 Map 与 Reduce 之间先做一次合并，减少网络传输量
    job.setCombinerClass(MaxTemperatureReducer.class);
    // 指定 Reducer
    job.setReducerClass(MaxTemperatureReducer.class);

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
    int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureWithCounters
