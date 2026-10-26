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
 * 运行「求最高气温」作业，并把最终输出压缩（旧版 MapReduce API 版）
 *
 * <p>本类与 {@code ch05-io/src/main/java/MaxTemperatureWithCompression.java} 对应，
 * 区别在于用的是 Hadoop 1.x 的 {@code org.apache.hadoop.mapred} 旧 API：
 * 旧 API 用 {@link org.apache.hadoop.mapred.JobConf} 承载配置，
 * 作业的提交与执行由 {@link org.apache.hadoop.mapred.JobClient} 负责
 * <p>新 API（{@code org.apache.hadoop.mapreduce}）则用 {@code Job} 对象，
 * 两者思路一致，只是配置方式不同
 */
public class MaxTemperatureWithCompression {

  /**
   * 配置并提交带输出压缩的最高气温作业
   *
   * @param args 第一个参数是输入路径，第二个参数是输出路径
   * @throws IOException 作业提交或执行失败
   */
  public static void main(String[] args) throws IOException {
    // 参数个数不对就打印用法并退出，避免后面取 args[1] 时越界
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
      		"<output path>");
      System.exit(-1);
    }
    
    // 旧 API 用 JobConf 承载配置，它同时也是作业描述对象
    JobConf conf = new JobConf(MaxTemperatureWithCompression.class);
    // 作业名，会显示在集群的作业列表里
    conf.setJobName("Max temperature with output compression");

    // 输入路径，可以是一个目录，框架会递归处理其中的所有文件
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    // 输出路径，作业成功后会在这里生成 part-r-xxxxx 这样的结果文件
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    // 声明输出键的类型，框架据此选择对应的序列化器
    conf.setOutputKeyClass(Text.class);
    // 声明输出值的类型
    conf.setOutputValueClass(IntWritable.class);
    
    // 打开输出压缩并指定用 Gzip；压缩发生在 Reduce 输出写盘时，不影响计算本身
    /*[*/FileOutputFormat.setCompressOutput(conf, true);
    FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);/*]*/

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
