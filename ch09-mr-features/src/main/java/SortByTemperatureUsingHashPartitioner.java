// cc SortByTemperatureUsingHashPartitioner MapReduce程序:使用默认HashPartitioner对SequenceFile中的IntWritable键进行排序

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// vv SortByTemperatureUsingHashPartitioner

/**
 * 使用HashPartitioner对SequenceFile中的温度数据进行排序的程序.
 *
 * 本程序演示最基本的MapReduce排序场景:
 *
 * 1. 输入格式:SequenceFile
 *    - SequenceFile是Hadoop的二进制键值对文件格式
 *    - 支持压缩,适合存储中等大小的数据
 *
 * 2. 分区策略:HashPartitioner(默认)
 *    - 按键的hashCode值确定分区
 *    - 优点:数据均匀分布
 *    - 缺点:不保证分区内的全局顺序
 *
 * 3. 输出配置:
 *    - 块压缩:将多个记录压缩为一个block,提高压缩效率
 *    - Gzip编码:平衡压缩率和速度
 *
 * 与TotalOrderPartitioner的区别:
 * - 本程序:按键hash值分区,可能跨分区无序
 * - TotalOrderPartitioner:保证全局有序,但可能有负载不均
 *
 * 使用场景:
 * - 数据只需要分区有序,不需要全局有序
 * - 对分区均匀度要求高于顺序要求
 *
 * @see SequenceFileInputFormat
 * @see SequenceFileOutputFormat
 */
public class SortByTemperatureUsingHashPartitioner extends Configured implements Tool {
    /**
     * 运行MapReduce作业,配置基于HashPartitioner的排序.
     *
     * 本程序使用默认的HashPartitioner进行分区排序,
     * 适用于不需要全局有序的场景.
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
        // 设置SequenceFile输入格式
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        // 设置SequenceFile输出格式,支持键值对顺序写入
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 启用输出压缩,使用块压缩和Gzip编码
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 程序入口.
     *
     * @param args 命令行参数:输入路径 输出路径
     * @throws Exception 运行异常
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(), args);
        System.exit(exitCode);
    }
}
// ^^ SortByTemperatureUsingHashPartitioner