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
 * 使用TotalOrderPartitioner对温度数据进行全局有序排序(旧API版本).
 *
 * 本程序是SortByTemperatureUsingTotalOrderPartitioner的旧API版本,
 * 使用org.apache.hadoop.mapred包中的旧接口和DistributedCache API.
 *
 * TotalOrderPartitioner原理:
 * 1. 通过采样确定分区边界
 * 2. 每个分区内的数据按键值有序
 * 3. 各分区之间满足:分区i的所有键 < 分区i+1的所有键
 *
 * 采样策略:
 * - RandomSampler:采样0.1%的数据,最多10000条,分10个分区
 * - 采样结果用于生成分区边界文件
 *
 * 分布式缓存:
 * - 使用DistributedCache.addCacheFile将分区文件分发到各节点
 *
 * @see TotalOrderPartitioner
 * @see InputSampler
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
  implements Tool {

    /**
     * 运行MapReduce作业,配置全局排序.
     *
     * @param args 命令行参数,包含输入输出路径
     * @return 作业成功返回0
     * @throws Exception 作业执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (conf == null) {
            return -1;
        }

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf,
            CompressionType.BLOCK);

        // 设置TotalOrderPartitioner分区器
        conf.setPartitionerClass(TotalOrderPartitioner.class);

        // 创建随机采样器:采样10%的数据,最多10000条样本,分为10个分区
        InputSampler.Sampler<IntWritable, Text> sampler =
          new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);

        // 获取输入路径并使其成为完全限定路径
        Path input = FileInputFormat.getInputPaths(conf)[0];
        input = input.makeQualified(input.getFileSystem(conf));

        // 创建分区文件路径
        Path partitionFile = new Path(input, "_partitions");
        TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
        // 采样并生成分区边界文件
        InputSampler.writePartitionFile(conf, sampler);

        // 添加分区文件到分布式缓存
        // 使用符号链接使文件在本地可用
        URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
        DistributedCache.addCacheFile(partitionUri, conf);
        DistributedCache.createSymlink(conf);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * 程序入口.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
            new SortByTemperatureUsingTotalOrderPartitioner(), args);
        System.exit(exitCode);
    }
}