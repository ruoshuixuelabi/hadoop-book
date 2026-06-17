// == MaxTemperatureWithMapOutputCompression
// cc MaxTemperatureWithMapOutputCompression 演示如何配置 Map 输出压缩

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MaxTemperatureWithMapOutputCompression - 演示 Map 输出压缩配置
 *
 * <p>此程序配置了 Map 阶段输出的压缩,这与 MaxTemperatureWithCompression
 * (Reduce 输出压缩) 不同。Map 输出压缩主要用于优化 shuffle 阶段。</p>
 *
 * <p><b>Map 输出压缩 vs Reduce 输出压缩:</b></p>
 * <ul>
 *   <li><b>Map 输出压缩</b>: 压缩 Map 写入本地磁盘的数据,优化 shuffle 传输</li>
 *   <li><b>Reduce 输出压缩</b>: 压缩最终的 HDFS 输出文件</li>
 * </ul>
 *
 * <p><b>配置参数:</b></p>
 * <ul>
 *   <li>Job.MAP_OUTPUT_COMPRESS: true 表示启用 Map 输出压缩</li>
 *   <li>Job.MAP_OUTPUT_COMPRESS_CODEC: 指定压缩编解码器类</li>
 * </ul>
 *
 * <p><b>使用场景:</b></p>
 * <ul>
 *   <li>Map 输出数据量大,网络带宽成为瓶颈时</li>
 *   <li>shuffle 阶段数据量超过输入数据量时</li>
 * </ul>
 *
 * @see MaxTemperatureWithCompression
 * @see org.apache.hadoop.mapreduce.Job
 */
public class MaxTemperatureWithMapOutputCompression {

    /**
     * 主方法 - 配置并运行带 Map 输出压缩的 MapReduce 作业
     *
     * <p><b>作业配置:</b></p>
     * <ol>
     *   <li>启用 Map 输出压缩</li>
     *   <li>指定压缩编解码器(GzipCodec)</li>
     *   <li>配置输入输出路径和 Mapper/Reducer</li>
     *   <li>提交作业</li>
     * </ol>
     *
     * <p><b>命令行参数:</b></p>
     * <ul>
     *   <li>args[0]: 输入路径</li>
     *   <li>args[1]: 输出路径</li>
     * </ul>
     *
     * @param args 命令行参数
     * @throws Exception 如果作业执行失败
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
                "<input path> <output path>");
            System.exit(-1);
        }

        // vv MaxTemperatureWithMapOutputCompression
        // 创建 Configuration 对象
        Configuration conf = new Configuration();

        // 启用 Map 输出压缩
        // 这会影响 MapTask 写入 shuffle 数据时使用的压缩设置
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);

        // 指定 Map 输出的压缩编解码器
        // GzipCodec: 压缩比较高,但 CPU 开销较大
        // SnappyCodec: 压缩比较低,但 CPU 开销小,延迟低(适合本地磁盘 I/O)
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class,
            CompressionCodec.class);

        // 创建 Job 并设置配置
        Job job = new Job(conf);
        // ^^ MaxTemperatureWithMapOutputCompression

        // 设置作业主类
        job.setJarByClass(MaxTemperature.class);

        // 设置输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 配置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 配置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置 Mapper 和 Reducer
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        // 提交作业并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
