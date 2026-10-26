package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * 用默认的 HashPartitioner 按气温排序（v1 版）
 *
 * <p>默认分区器按键的哈希值决定数据去哪个分区，
 * 落到哪个分区完全是随意的，因此输出并不是全局有序的
 * <p>这一点与本章里那个用 TotalOrderPartitioner 的例子正好相反，
 * 两个例子放在一起看最清楚
 */
public class SortByTemperatureUsingHashPartitioner extends Configured
  implements Tool {
  
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
    
    // 读入的是 SequenceFile
    conf.setInputFormat(SequenceFileInputFormat.class);
    // 输出键是气温
    conf.setOutputKeyClass(IntWritable.class);
    // 输出仍是 SequenceFile
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    // 打开输出压缩
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    // 用 Gzip 压缩
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    // 按块压缩
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);
    
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
    int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(),
        args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
