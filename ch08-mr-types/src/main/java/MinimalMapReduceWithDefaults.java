// cc MinimalMapReduceWithDefaults A minimal MapReduce driver, with the defaults explicitly set
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv MinimalMapReduceWithDefaults
/**
 * 把「默认值」显式写出来的 MapReduce 驱动程序
 *
 * <p>上一个例子里默认行为是隐式的——没写代码不代表没生效，
 * 而是框架替你挑了默认实现
 * <p>本例把每一个默认值都显式设置一遍，
 * 这样读代码的人一眼就能看出「不写的话会用什么」
 * <p>注意这些设置项的名字在两代 API 里不同：
 * MapReduce v1（{@code JobConf}）里大多以 {@code mapred.} 开头，
 * v2（{@code Job}）里则去掉了前缀
 */
public class MinimalMapReduceWithDefaults extends Configured implements Tool {
  
  /**
   * 构造作业并显式设置全部默认项
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
    
    // 下面把 Hadoop 的默认设置逐条显式写出来，
    // 目的是让「不写会怎样」变得一目了然
    /*[*/job.setInputFormatClass(TextInputFormat.class);
    
    // 默认的 Mapper：原样输出输入的键值对
    job.setMapperClass(Mapper.class);
    
    // Map 阶段输出键的类型
    job.setMapOutputKeyClass(LongWritable.class);
    // Map 阶段输出值的类型
    job.setMapOutputValueClass(Text.class);
    
    // 默认的分区器：按键的哈希值决定数据去哪个 Reducer
    job.setPartitionerClass(HashPartitioner.class);
    
    // 只启动一个 Reduce 任务
    job.setNumReduceTasks(1);
    // 默认的 Reducer：原样输出 Map 阶段的结果
    job.setReducerClass(Reducer.class);

    // 最终输出键的类型
    job.setOutputKeyClass(LongWritable.class);
    // 最终输出值的类型
    job.setOutputValueClass(Text.class);

    job.setOutputFormatClass(TextOutputFormat.class);/*]*/
    // 默认输出格式：把键值对以「键\t值」的文本行写出
    
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
    int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
// ^^ MinimalMapReduceWithDefaults
