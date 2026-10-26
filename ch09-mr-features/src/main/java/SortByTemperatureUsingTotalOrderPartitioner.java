// cc SortByTemperatureUsingTotalOrderPartitioner A MapReduce program for sorting a SequenceFile with IntWritable keys using the TotalOrderPartitioner to globally sort the data
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.*;

// vv SortByTemperatureUsingTotalOrderPartitioner
/**
 * 用全序分区器做全局排序
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
    // 公共工具类：解析参数、构造 Job 并设置输入输出路径
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    // 返回 null 说明参数个数不对
    if (job == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 读入的是 SequenceFile
    job.setInputFormatClass(SequenceFileInputFormat.class);
    // 输出键是气温
    job.setOutputKeyClass(IntWritable.class);
    // 输出仍是 SequenceFile
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    // 打开输出压缩
    SequenceFileOutputFormat.setCompressOutput(job, true);
    // 用 Gzip 压缩
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    // 按块压缩
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);

    // 用全序分区器：各分区的键区间互不重叠，拼起来才是全局有序
    job.setPartitionerClass(TotalOrderPartitioner.class);

    // 采样器：随机采样，按 10% 的比例、最多取 10000 条、每个分区分 10 份
    InputSampler.Sampler<IntWritable, Text> sampler =
      new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
    
    // 由采样结果算出每个分区的键边界，写成一个文件
    InputSampler.writePartitionFile(job, sampler);

    // Add to DistributedCache
    // 取出作业配置
    Configuration conf = job.getConfiguration();
    // 刚才写出的分区文件路径
    String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
    // 转成 URI 才能加进缓存
    URI partitionUri = new URI(partitionFile);
    // 分发到各个任务节点
    job.addCacheFile(partitionUri);

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
    int exitCode = ToolRunner.run(
        new SortByTemperatureUsingTotalOrderPartitioner(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
// ^^ SortByTemperatureUsingTotalOrderPartitioner
