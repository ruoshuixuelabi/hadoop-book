import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 把按气温排序的结果输出成 MapFile
 *
 * <p>MapFile 在有序的 SequenceFile 之上又加了一份索引，
 * 因此既可以像普通文件那样顺序遍历，也可以按键随机查找
 * <p>本例只输出不排序：排序由上游作业完成，
 * 这里负责把结果转成便于随机查找的格式
 */
public class SortByTemperatureToMapFile extends Configured implements Tool {
  
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
    // 输出成 MapFile：既能随机查找，也能顺序遍历
    job.setOutputFormatClass(MapFileOutputFormat.class);
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
    int exitCode = ToolRunner.run(new SortByTemperatureToMapFile(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
