// cc MaxTemperatureWithCompression Application to run the maximum temperature job producing compressed output
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//vv MaxTemperatureWithCompression
/**
 * 运行「求最高气温」作业，并把最终输出压缩
 *
 * <p>作业的逻辑（Mapper/Reducer）与 {@code MaxTemperature} 完全相同，
 * 这里只额外打开了输出压缩：
 * 开启后 Reduce 输出会先压缩再写盘，可以显著减少结果文件的大小
 */
public class MaxTemperatureWithCompression {

  /**
   * 配置并提交带输出压缩的最高气温作业
   *
   * @param args 第一个参数是输入路径，第二个参数是输出路径
   * @throws Exception 作业提交或执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数个数不对就打印用法并退出，避免后面取 args[1] 时越界
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
        "<output path>");
      System.exit(-1);
    }

    // Job 是作业的配置容器：输入输出路径、Mapper/Reducer 类、压缩设置都挂在它上面
    Job job = new Job();
    // 记录作业所在的 jar，提交到集群时框架据此定位类
    job.setJarByClass(MaxTemperature.class);

    // 输入路径，可以是一个目录，框架会递归处理其中的所有文件
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // 输出路径，作业成功后会在这里生成 part-r-xxxxx 这样的结果文件
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // 声明输出键的类型，框架据此选择对应的序列化器
    job.setOutputKeyClass(Text.class);
    // 声明输出值的类型
    job.setOutputValueClass(IntWritable.class);
    
    // 打开输出压缩并指定用 Gzip；压缩发生在 Reduce 输出写盘时，不影响计算本身
    /*[*/FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);/*]*/
    
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
//^^ MaxTemperatureWithCompression
