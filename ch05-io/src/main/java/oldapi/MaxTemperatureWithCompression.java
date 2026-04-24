package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * MaxTemperatureWithCompression - 旧版 API 的压缩输出最大温度 MapReduce 程序
 *
 * <p>此程序是 MaxTemperature 的增强版本,配置了 Reduce 输出压缩。
 * 所有 Reducer 输出都会被 Gzip 压缩后再写入 HDFS。</p>
 *
 * <p><b>压缩配置:</b></p>
 * <ul>
 *   <li>FileOutputFormat.setCompressOutput(conf, true): 启用输出压缩</li>
 *   <li>FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class): 使用 Gzip 压缩</li>
 * </ul>
 *
 * <p><b>使用示例:</b></p>
 * <pre>{@code
 * hadoop jar hadoop-examples.jar oldapi.MaxTemperatureWithCompression /input/ncdc /output.gz
 * }</pre>
 *
 * @see MaxTemperatureWithMapOutputCompression (Map 输出压缩版本)
 */
public class MaxTemperatureWithCompression {

  /**
   * 主方法 - 配置并运行 MapReduce 作业
   *
   * @param args 命令行参数 [输入路径, 输出路径]
   * @throws IOException 如果发生 I/O 错误
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
      		"<output path>");
      System.exit(-1);
    }

    // 创建 JobConf,旧版 API 使用 JobConf 配置作业
    JobConf conf = new JobConf(MaxTemperatureWithCompression.class);
    conf.setJobName("Max temperature with output compression");

    // 设置输入路径
    FileInputFormat.addInputPath(conf, new Path(args[0]));

    // 设置输出路径
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // 设置输出键值类型
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    /*[*/
    // 启用输出压缩
    // 所有 Reducer 输出都会被压缩后再写入
    FileOutputFormat.setCompressOutput(conf, true);

    // 设置压缩编解码器为 GzipCodec
    // Gzip 是基于 DEFLATE 的无损压缩,压缩比高
    FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);/*]*/

    // 设置 Mapper 类
    conf.setMapperClass(MaxTemperatureMapper.class);

    // 设置 Combiner 类 (本地聚合)
    conf.setCombinerClass(MaxTemperatureReducer.class);

    // 设置 Reducer 类
    conf.setReducerClass(MaxTemperatureReducer.class);

    // 运行作业
    JobClient.runJob(conf);
  }
}
