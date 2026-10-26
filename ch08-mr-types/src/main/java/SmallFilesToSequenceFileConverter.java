// cc SmallFilesToSequenceFileConverter A MapReduce program for packaging a collection of small files as a single SequenceFile
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv SmallFilesToSequenceFileConverter
/**
 * 把一堆小文件合并成一个 SequenceFile
 *
 * <p>HDFS 上的小文件会带来两个问题：
 * 一是占用大量 NameNode 的元数据，二是每个小文件都要占用一个 Map 任务槽位
 * <p>本例用一次 MapReduce 作业，把每个小文件读成一条记录，
 * 再写进单个 SequenceFile——SequenceFile 是 Hadoop 的二进制记录格式，
 * 顺序读取效率很高
 */
public class SmallFilesToSequenceFileConverter extends Configured
    implements Tool {
  
  /**
   * 把「文件内容」变成「文件名 → 文件内容」的记录
   *
   * <p>用 {@code WholeFileInputFormat} 读入时，
   * 一条记录就是一个完整文件，键为空，
   * 因此要在 setup 里把文件名取出来当键
   */
  static class SequenceFileMapper
      extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    
    // 文件名在 setup 阶段确定一次，map 阶段反复使用
    private Text filenameKey;
    
    /**
     * 取出当前 InputSplit 对应的文件名
     *
     * @param context 任务上下文
     * @throws IOException 读取分片失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      // 当前任务负责的这个分片
      InputSplit split = context.getInputSplit();
      // 因为输入格式是 WholeFileInputFormat，分片一定是 FileSplit
      Path path = ((FileSplit) split).getPath();
      // 用文件的完整路径作为键
      filenameKey = new Text(path.toString());
    }
    
    /**
     * 原样输出「文件名 → 文件内容」
     *
     * @param key 输入的键（为空）
     * @param value 输入的值，即整个文件的内容
     * @param context 上下文，用来写出结果
     * @throws IOException 写出失败
     * @throws InterruptedException 任务被中断
     */
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
      // 不做任何加工，原样搬运
      context.write(filenameKey, value);
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
    // 参数不对时该方法会返回 null
    if (job == null) {
      // 用法错误，返回 -1
      return -1;
    }
    
    // 以「整文件」为单位读入
    job.setInputFormatClass(WholeFileInputFormat.class);
    // 以二进制记录格式写出
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    // 键是文件名
    job.setOutputKeyClass(Text.class);
    // 值是文件内容
    job.setOutputValueClass(BytesWritable.class);

    // 指定 Mapper
    job.setMapperClass(SequenceFileMapper.class);

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
    int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
    // 以退出码结束进程
    System.exit(exitCode);
  }
}
// ^^ SmallFilesToSequenceFileConverter