// == OldMaxTemperatureWithMapOutputCompression
package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * MaxTemperatureWithMapOutputCompression - 旧版 API 的 Map 输出压缩程序
 *
 * <p>此程序演示在旧版 MapReduce API 中如何配置 Map 中间结果压缩。
 * Map 输出的数据在传输到 Reducer 之前会被压缩,减少网络传输量。</p>
 *
 * <p><b>压缩配置:</b></p>
 * <ul>
 *   <li>conf.setCompressMapOutput(true): 启用 Map 输出压缩</li>
 *   <li>conf.setMapOutputCompressorClass(GzipCodec.class): 指定压缩编解码器</li>
 * </ul>
 *
 * <p><b>Map 输出压缩 vs 输出压缩:</b></p>
 * <ul>
 *   <li>Map 输出压缩: 压缩 Map 到 Reducer 的数据,默认不启用</li>
 *   <li>输出压缩: 压缩最终输出文件,需要显式启用</li>
 * </ul>
 *
 * @see MaxTemperatureWithCompression (最终输出压缩版本)
 */
public class MaxTemperatureWithMapOutputCompression {

  /**
   * 主方法 - 配置并运行 MapReduce 作业
   *
   * @param args 命令行参数 [输入路径, 输出路径]
   * @throws IOException 如果发生 I/O 错误
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
      		"<input path> <output path>");
      System.exit(-1);
    }

    // 创建 JobConf
    JobConf conf = new JobConf(MaxTemperatureWithCompression.class);
    conf.setJobName("Max temperature with map output compression");

    // 设置输入输出路径
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // 设置输出键值类型
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // vv OldMaxTemperatureWithMapOutputCompression
    // 启用 Map 输出压缩
    // 这会使 MapTask 输出的键值对被压缩后再写入本地磁盘
    // 当 Reducer 获取数据时,会自动解压缩
    conf.setCompressMapOutput(true);

    // 设置 Map 输出使用的压缩编解码器
    // GzipCodec 是常用的压缩格式,压缩比较高
    conf.setMapOutputCompressorClass(GzipCodec.class);
    // ^^ OldMaxTemperatureWithMapOutputCompression

    // 设置 Mapper
    conf.setMapperClass(MaxTemperatureMapper.class);

    // 设置 Combiner (在 Map 端进行局部聚合)
    conf.setCombinerClass(MaxTemperatureReducer.class);

    // 设置 Reducer
    conf.setReducerClass(MaxTemperatureReducer.class);

    // 运行作业
    JobClient.runJob(conf);
  }

}
