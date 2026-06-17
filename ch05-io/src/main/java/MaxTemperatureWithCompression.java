// cc MaxTemperatureWithCompression 演示如何运行产生压缩输出的最大温度 MapReduce 作业

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//vv MaxTemperatureWithCompression
/**
 * MaxTemperatureWithCompression - 演示 MapReduce 输出压缩配置
 *
 * <p>此程序是 MaxTemperature 的增强版本,配置了输出压缩。
 * 所有 Reduce 输出都会被 Gzip 压缩后再写入 HDFS。</p>
 *
 * <p><b>压缩配置:</b></p>
 * <ul>
 *   <li>启用输出压缩: FileOutputFormat.setCompressOutput(job, true)</li>
 *   <li>指定压缩编解码器: FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class)</li>
 * </ul>
 *
 * <p><b>压缩格式:</b></p>
 * <ul>
 *   <li>使用 GzipCodec 进行压缩</li>
 *   <li>输出文件扩展名为 .gz</li>
 *   <li>压缩在 Reduce 端进行,不影响 Map 端</li>
 * </ul>
 *
 * <p><b>命令行示例:</b></p>
 * <pre>{@code
 * hadoop jar hadoop-examples.jar MaxTemperatureWithCompression /input/ncdc /output.gz
 * }</pre>
 *
 * @see MaxTemperature
 * @see GzipCodec
 * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
 */
public class MaxTemperatureWithCompression {

    /**
     * 主方法 - 配置并运行带压缩输出的 MapReduce 作业
     *
     * <p><b>作业配置:</b></p>
     * <ol>
     *   <li>设置输入输出路径</li>
     *   <li>配置键值类型</li>
     *   <li>启用输出压缩并指定 GzipCodec</li>
     *   <li>设置 Mapper 和 Reducer</li>
     *   <li>提交作业并等待完成</li>
     * </ol>
     *
     * <p><b>命令行参数:</b></p>
     * <ul>
     *   <li>args[0]: 输入路径 (包含天气数据)</li>
     *   <li>args[1]: 输出路径 (压缩后的结果)</li>
     * </ul>
     *
     * @param args 命令行参数
     * @throws Exception 如果作业执行失败
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
                    "<output path>");
            System.exit(-1);
        }

        // 创建 Job 对象,用于配置 MapReduce 作业
        Job job = new Job();

        // 设置作业 jar 包的主类
        job.setJarByClass(MaxTemperature.class);

        // 配置输入路径(支持通配符)
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 配置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*[*/
        // 启用输出压缩
        // 这会使所有 Reducer 输出都被压缩后再写入
        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩编解码器为 GzipCodec
        // Gzip 是基于 DEFLATE 算法的无损压缩,压缩比较高
        // 其他可选 Codec: BZip2Codec, SnappyCodec, Lz4Codec
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);/*]*/

        // 设置 Mapper 类 - 解析天气数据并提取年份和温度
        job.setMapperClass(MaxTemperatureMapper.class);

        // 设置 Combiner 类 - 在 Map 端进行局部聚合,减少网络传输
        job.setCombinerClass(MaxTemperatureReducer.class);

        // 设置 Reducer 类 - 找出每个年份的最高温度
        job.setReducerClass(MaxTemperatureReducer.class);

        // 提交作业并等待完成,返回是否成功
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//^^ MaxTemperatureWithCompression
