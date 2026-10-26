package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.*;

/**
 * 把一堆小文件合并成一个 SequenceFile（v1 旧 API）
 *
 * <p>HDFS 上的小文件会带来两个问题：
 * 一是占用大量 NameNode 的元数据，二是每个小文件都要占用一个 Map 任务槽位
 * <p>本例用一次 MapReduce 作业，把每个小文件读成一条记录，
 * 再写进单个 SequenceFile——SequenceFile 是 Hadoop 的二进制记录格式，
 * 顺序读取效率很高
 */
public class SmallFilesToSequenceFileConverter extends Configured
  implements Tool {
  
  /**
   * 把「文件内容」变成「文件名 → 文件内容」的记录
   *
   * <p>用 {@code WholeFileInputFormat} 读入时，
   * 一条记录就是一个完整文件，键为空，
   * 因此要想法把文件名取出来当键
   */
  static class SequenceFileMapper extends MapReduceBase
      implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    
    // 作业配置：v1 里通过 configure 注入
    private JobConf conf;
    
    /**
     * 保存作业配置
     *
     * @param conf 作业配置
     */
    @Override
    public void configure(JobConf conf) {
      // 保存下来，map 阶段要用
      this.conf = conf;
    }

    /**
     * 原样输出「文件名 → 文件内容」
     *
     * @param key 输入的键（为空）
     * @param value 输入的值，即整个文件的内容
     * @param output 结果收集器，用来把键值对写出
     * @param reporter 状态上报器
     * @throws IOException 写出失败
     */
    @Override
    public void map(NullWritable key, BytesWritable value,
        OutputCollector<Text, BytesWritable> output, Reporter reporter)
        throws IOException {
      
      // 框架会把当前输入文件的全路径写进这个配置项
      String filename = conf.get("map.input.file");
      // 键是文件名，值是文件内容
      output.collect(new Text(filename), value);
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
    
    // 以「整文件」为单位读入
    conf.setInputFormat(WholeFileInputFormat.class);
    // 以二进制记录格式写出
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    // 键是文件名
    conf.setOutputKeyClass(Text.class);
    // 值是文件内容
    conf.setOutputValueClass(BytesWritable.class);

    // 指定 Mapper
    conf.setMapperClass(SequenceFileMapper.class);
    // 这里的 Reducer 什么都不做，原样输出 Map 的结果
    conf.setReducerClass(IdentityReducer.class);

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
    int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
