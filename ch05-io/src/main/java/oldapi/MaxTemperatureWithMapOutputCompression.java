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
 * 运行「求最高气温」作业，并把 Map 阶段的中间输出压缩（旧版 MapReduce API 版）
 *
 * <p>本类与 {@code ch05-io/src/main/java/MaxTemperatureWithMapOutputCompression.java} 对应，
 * 区别在于用的是 Hadoop 1.x 的 {@code org.apache.hadoop.mapred} 旧 API：
 * 旧 API 通过 {@link org.apache.hadoop.mapred.JobConf} 上的方法设置压缩，
 * 而不是新 API 里往 {@code Configuration} 里塞键值
 * <p>压缩 Map 输出的意义：Shuffle 阶段要搬运的数据量会明显减少，
 * 既省本地磁盘，也省网络带宽
 */
public class MaxTemperatureWithMapOutputCompression {

  /**
   * 配置并提交带 Map 输出压缩的最高气温作业
   *
   * @param args 第一个参数是输入路径，第二个参数是输出路径
   * @throws IOException 作业提交或执行失败
   */
  public static void main(String[] args) throws IOException {
    // 参数个数不对就打印用法并退出，避免后面取 args[1] 时越界
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
      		"<input path> <output path>");
      System.exit(-1);
    }
    
    // 旧 API 用 JobConf 承载配置，它同时也是作业描述对象
    JobConf conf = new JobConf(MaxTemperatureWithCompression.class);
    // 作业名，会显示在集群的作业列表里
    conf.setJobName("Max temperature with map output compression");

    // 输入路径，可以是一个目录，框架会递归处理其中的所有文件
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    // 输出路径，作业成功后会在这里生成 part-r-xxxxx 这样的结果文件
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    // 声明输出键的类型，框架据此选择对应的序列化器
    conf.setOutputKeyClass(Text.class);
    // 声明输出值的类型
    conf.setOutputValueClass(IntWritable.class);
    
    // vv OldMaxTemperatureWithMapOutputCompression
    // 打开 Map 输出压缩，这是旧 API 里的对应开关
    conf.setCompressMapOutput(true);
    // 指定用哪种编解码器压缩 Map 输出，这里用 Gzip
    conf.setMapOutputCompressorClass(GzipCodec.class);
    // ^^ OldMaxTemperatureWithMapOutputCompression

    // 指定 Mapper 类
    conf.setMapperClass(MaxTemperatureMapper.class);
    // 指定 Combiner 类：在 Map 端先做一次局部归约，可以显著减少 Shuffle 的网络传输量
    conf.setCombinerClass(MaxTemperatureReducer.class);
    // 指定 Reducer 类：做最终的全局归约
    conf.setReducerClass(MaxTemperatureReducer.class);

    // 提交并运行作业；这一步是阻塞的，作业结束后才返回
    JobClient.runJob(conf);
  }

}
