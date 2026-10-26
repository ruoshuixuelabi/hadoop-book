// cc WholeFileInputFormat An InputFormat for reading a whole file as a record
import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

//vv WholeFileInputFormat
/**
 * 把整个文件当作一条记录的输入格式
 *
 * <p>继承 {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}，
 * 只改两处：一是不切分文件，二是自己造 {@link RecordReader}
 * <p>典型用途是把小文件合并成 SequenceFile，
 * 或把压缩包、序列化对象当作整体读入
 */
public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {
  
  /**
   * 声明文件不可切分
   *
   * @param context 作业上下文
   * @param file 待判断的文件
   * @return 固定返回 {@code false}
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    // 不可切分：一个文件对应一个 InputSplit
    return false;
  }

  /**
   * 创建整文件读取器
   *
   * @param split 输入分片
   * @param context 任务上下文
   * @return 读取器实例
   * @throws IOException 创建失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public RecordReader<NullWritable, BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    // 构造读取器
    WholeFileRecordReader reader = new WholeFileRecordReader();
    // 框架随后会调用 initialize 把分片和配置交给读取器
    reader.initialize(split, context);
    // 把读取器交给框架
    return reader;
  }
}
//^^ WholeFileInputFormat
