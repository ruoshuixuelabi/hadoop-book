// cc SortByTemperatureUsingHashPartitioner A MapReduce program for sorting a SequenceFile with IntWritable keys using the default HashPartitioner
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
 * 用默认的 HashPartitioner 按气温排序
 *
 * <p>默认分区器按键的哈希值决定数据去哪个分区，
 * 落到哪个分区完全是随意的，因此输出并不是全局有序的
 * <p>这一点与本章里那个用 TotalOrderPartitioner 的例子正好相反，
 * 两个例子放在一起看最清楚
 */
public class SortByTemperatureUsingHashPartitioner extends Configured
  implements Tool {
  
  /**
   * 运行作业
   *
   * @param args 依次为输入路径与输出路径
   * @return 0 表示成功，-1 表示参数错误
   * @throws Exception 作业执行失败
   */
  @Override
  public int run(String[] args) throws Exception {
    // 公共工具类：解析参数、构造 Job 并设置输入输出路径
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    // 返回 null 说明参数个数不对
    if (job == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 读入的是 SequenceFile
    job.setInputFormatClass(SequenceFileInputFormat.class);
    // 输出键是气温
    job.setOutputKeyClass(IntWritable.class);
    // 输出仍是 SequenceFile
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    // 打开输出压缩
    SequenceFileOutputFormat.setCompressOutput(job, true);
    // 用 Gzip 压缩
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    // 按块压缩
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);
    
    // 提交作业并等待它跑完
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * 程序入口
   *
   * @param args 命令行参数
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner 会解析通用选项并调用 run
    int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(),
        args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
// ^^ SortByTemperatureUsingHashPartitioner
