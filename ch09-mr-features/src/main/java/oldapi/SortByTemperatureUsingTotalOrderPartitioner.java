package oldapi;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

/**
 * 用全序分区器做全局排序（v1 版）
 *
 * <p>要得到全局有序的输出，光靠排序比较器还不够：
 * 每个分区内部有序，但分区之间的先后是任意的
 * <p>全序分区器的做法是：先采样一部分数据，
 * 据此算出每个分区应当包含的键区间，
 * 再让分区器按这个区间把数据分派到各分区——
 * 这样各分区的区间互不重叠且按序排列，拼起来就是全局有序的
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
  implements Tool {
  
  /**
   * 运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
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

    // 用全序分区器：各分区的键区间互不重叠，拼起来才是全局有序
    conf.setPartitionerClass(TotalOrderPartitioner.class);

    // 采样器：随机采样，按 10% 的比例、最多取 10000 条、每个分区分 10 份
    InputSampler.Sampler<IntWritable, Text> sampler =
      new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
    
    // 取出第一个输入路径，采样要从这里读数据
    Path input = FileInputFormat.getInputPaths(conf)[0];
    // 转成带文件系统的完整路径
    input = input.makeQualified(input.getFileSystem(conf));
    
    // 分区边界文件放在输入目录里
    Path partitionFile = new Path(input, "_partitions");
    // 告诉分区器去哪里读边界
    TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
    // 由采样结果算出每个分区的键边界，写成这个文件
    InputSampler.writePartitionFile(conf, sampler);

    // Add to DistributedCache
    // v1 用「路径#符号链接名」的形式指定缓存文件
    URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
    // 把分区文件加入缓存清单
    DistributedCache.addCacheFile(partitionUri, conf);
    // 在任务节点上建立符号链接，任务才能按名字找到它
    DistributedCache.createSymlink(conf);

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
    int exitCode = ToolRunner.run(
        new SortByTemperatureUsingTotalOrderPartitioner(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
