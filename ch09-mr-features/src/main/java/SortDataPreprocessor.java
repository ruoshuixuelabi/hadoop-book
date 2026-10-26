// cc SortDataPreprocessor A MapReduce program for transforming the weather data into SequenceFile format
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv SortDataPreprocessor
/**
 * 把天气数据转换成以气温为键的 SequenceFile
 *
 * <p>这是排序作业的前置步骤：
 * 原始记录是定宽文本，先解析成「气温 → 原始记录」，
 * 后面才能按气温排序
 * <p>本例只需要 Map 阶段（Reduce 任务数设为 0），
 * 因为分组聚合在这里没有意义
 */
public class SortDataPreprocessor extends Configured implements Tool {
  
  /**
   * 清洗数据：以气温为键输出原始记录
   *
   * <p>无效读数（缺测或可疑）直接丢弃
   */
  static class CleanerMapper
    extends Mapper<LongWritable, Text, IntWritable, Text> {
  
    // 解析器实例：一个 Map 任务只创建一次
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    /**
     * 解析记录并以气温为键输出
     *
     * @param key 输入的键（本例用不到）
     * @param value 输入的值，即一行 NCDC 记录
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      // 解析这一行记录
      parser.parse(value);
      // 只有读数有效的记录才输出
      if (parser.isValidTemperature()) {
        // 键是气温，值是原始记录
        context.write(new IntWritable(parser.getAirTemperature()), value);
      }
    }
  }
  
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

    // 指定 Mapper
    job.setMapperClass(CleanerMapper.class);
    // 输出键是气温
    job.setOutputKeyClass(IntWritable.class);
    // 输出值是原始记录
    job.setOutputValueClass(Text.class);
    // 不需要 Reduce：本作业只做格式转换
    job.setNumReduceTasks(0);
    // 输出成 SequenceFile
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
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
// ^^ SortDataPreprocessor