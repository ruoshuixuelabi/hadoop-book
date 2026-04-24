// cc SortByTemperatureUsingTotalOrderPartitioner MapReduce程序:使用TotalOrderPartitioner对SequenceFile中的IntWritable键进行全局排序

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
 * 使用TotalOrderPartitioner对温度数据进行全局有序排序的程序.
 *
 * 本程序演示Hadoop的全局排序功能:
 *
 * 1. TotalOrderPartitioner原理:
 *    - 将数据划分为多个分区,每个分区内的数据按键值有序
 *    - 分区边界由采样数据确定,确保全局有序
 *    - 各分区之间满足:分区i的所有键 < 分区i+1的所有键
 *
 * 2. 采样策略:
 *    - RandomSampler:随机采样,适合数据分布均匀的场景
 *    - 参数:采样频率(0.1)、采样数量(10000)、分区数(10)
 *    - 采样量越大,分区边界越准确,但采样成本也增加
 *
 * 3. 分布式缓存:
 *    - 分区边界文件通过DistributedCache分发到各节点
 *    - 确保每个Mapper/Reducer能正确判断键所属分区
 *
 * 与HashPartitioner的区别:
 * - HashPartitioner:按键hash值均匀分布,但不保证顺序
 * - TotalOrderPartitioner:保证全局有序,但可能有数据倾斜
 *
 * @see TotalOrderPartitioner
 * @see InputSampler
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {
    /**
     * 运行MapReduce作业,配置全局排序.
     *
     * 执行步骤:
     * 1. 解析输入输出路径
     * 2. 设置SequenceFile输入输出格式
     * 3. 配置压缩:BLOCK压缩,Gzip编码
     * 4. 设置TotalOrderPartitioner作为分区器
     * 5. 运行InputSampler采样并生成分区边界文件
     * 6. 将分区边界文件添加到分布式缓存
     *
     * @param args 命令行参数,包含输入输出路径
     * @return 作业成功返回0,否则返回-1
     * @throws Exception 作业执行异常
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 配置SequenceFile块压缩,使用Gzip编码器
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

        // 设置全局排序分区器
        job.setPartitionerClass(TotalOrderPartitioner.class);

        // 创建随机采样器:采样10%的数据,最多10000条样本,分为10个分区
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
        // 采样并生成分区边界文件
        InputSampler.writePartitionFile(job, sampler);

        // 将分区边界文件添加到分布式缓存,供TotalOrderPartitioner使用
        // TotalOrderPartitioner通过Configuration获取分区文件路径
        Configuration conf = job.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUri = new URI(partitionFile);
        // 添加缓存文件:分发分区边界信息到所有计算节点
        job.addCacheFile(partitionUri);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
        System.exit(exitCode);
    }
}
// ^^ SortByTemperatureUsingTotalOrderPartitioner