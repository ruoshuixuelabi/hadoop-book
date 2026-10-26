// == MaxTemperatureWithMapOutputCompression
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
 * 运行「求最高气温」作业，并把 Map 阶段的中间输出压缩
 *
 * <p>与 {@code MaxTemperatureWithCompression} 压缩的是最终结果不同，
 * 本例压缩的是 Map 输出——也就是 Shuffle 阶段要搬运到 Reduce 端的数据。
 * 压缩发生在 Map 端写「溢写文件」时，可以大幅减少本地磁盘占用和网络传输量
 * <p>注意：Combiner 的输出同样属于 Map 输出，会一起被压缩
 */
public class MaxTemperatureWithMapOutputCompression {

  /**
   * 配置并提交带 Map 输出压缩的最高气温作业
   *
   * @param args 第一个参数是输入路径，第二个参数是输出路径
   * @throws Exception 作业提交或执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数个数不对就打印用法并退出，避免后面取 args[1] 时越界
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
        "<input path> <output path>");
      System.exit(-1);
    }

    // vv MaxTemperatureWithMapOutputCompression
    // 先建一份配置对象，用来设置 Map 输出压缩相关的开关
    Configuration conf = new Configuration();
    // 打开 Map 输出压缩，这是新 API 里对应的开关
    conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
    // 指定用哪种编解码器压缩 Map 输出，这里用 Gzip
    conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class,
        CompressionCodec.class);
    // 把配置传给 Job，之后的设置都会写进这份配置里
    Job job = new Job(conf);
    // ^^ MaxTemperatureWithMapOutputCompression
    // 记录作业所在的 jar，提交到集群时框架据此定位类
    job.setJarByClass(MaxTemperature.class);

    // 声明输出键的类型，框架据此选择对应的序列化器
    job.setOutputKeyClass(Text.class);
    // 声明输出值的类型
    job.setOutputValueClass(IntWritable.class);

    // 输入路径，可以是一个目录，框架会递归处理其中的所有文件
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 输出路径，作业成功后会在这里生成 part-r-xxxxx 这样的结果文件
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // 指定 Mapper 类
    job.setMapperClass(MaxTemperatureMapper.class);
    // 指定 Combiner 类：在 Map 端先做一次局部归约，可以显著减少 Shuffle 的网络传输量
    job.setCombinerClass(MaxTemperatureReducer.class);
    // 指定 Reducer 类：做最终的全局归约
    job.setReducerClass(MaxTemperatureReducer.class);
    
    // waitForCompletion(true) 会阻塞并打印进度；成功退出码 0，失败退出码 1
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
