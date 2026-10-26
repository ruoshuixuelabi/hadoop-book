package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

/**
 * 把「默认值」显式写出来的 MapReduce 驱动程序（v1 旧 API）
 *
 * <p>本例把 v1 API 的每一个默认值都显式设置一遍，
 * 这样读代码的人一眼就能看出「不写的话会用什么」
 * <p>注意 v1 的设置项名字大多以 {@code mapred.} 开头，
 * 而新 API（{@code Job}）去掉了这个前缀
 */
public class MinimalMapReduceWithDefaults extends Configured implements Tool {
  
  /**
   * 构造作业并显式设置全部默认项
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
    
    // 下面把 v1 API 的默认设置逐条显式写出来，
    // 目的是让「不写会怎样」变得一目了然
    /*[*/conf.setInputFormat(TextInputFormat.class);
    
    // 只启动一个 Map 任务
    conf.setNumMapTasks(1);
    // 默认的 Mapper：原样输出输入的键值对
    conf.setMapperClass(IdentityMapper.class);
    // v1 里 Map 任务由 MapRunner 驱动，新 API 中没有这一项
    conf.setMapRunnerClass(MapRunner.class);
    
    // Map 阶段输出键的类型
    conf.setMapOutputKeyClass(LongWritable.class);
    // Map 阶段输出值的类型
    conf.setMapOutputValueClass(Text.class);
    
    // 默认的分区器：按键的哈希值决定数据去哪个 Reducer
    conf.setPartitionerClass(HashPartitioner.class);
    
    // 只启动一个 Reduce 任务
    conf.setNumReduceTasks(1);
    // 默认的 Reducer：原样输出 Map 阶段的结果
    conf.setReducerClass(IdentityReducer.class);

    // 最终输出键的类型
    conf.setOutputKeyClass(LongWritable.class);
    // 最终输出值的类型
    conf.setOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);/*]*/
    // 默认输出格式：把键值对以「键\t值」的文本行写出
    
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
    int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
