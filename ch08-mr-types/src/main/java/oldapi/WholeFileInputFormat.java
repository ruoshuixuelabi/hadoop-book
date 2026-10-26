package oldapi;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 把整个文件当作一条记录的输入格式（v1 旧 API）
 *
 * <p>继承 {@link org.apache.hadoop.mapred.FileInputFormat}，
 * 只改两处：一是不切分文件，二是自己造 {@link RecordReader}
 * <p>典型用途是把小文件合并成 SequenceFile，
 * 或把压缩包、序列化对象当作整体读入
 */
public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {
  
  /**
   * 声明文件不可切分
   *
   * @param fs 文件所在文件系统
   * @param filename 待判断的文件
   * @return 固定返回 {@code false}
   */
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    // 不可切分：一个文件对应一个 InputSplit
    return false;
  }

  /**
   * 创建整文件读取器
   *
   * @param split 输入分片
   * @param job 作业配置
   * @param reporter 状态上报器
   * @return 读取器实例
   * @throws IOException 创建失败
   */
  @Override
  public RecordReader<NullWritable, BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {

    // 构造读取器：把分片与配置直接交给它
    return new WholeFileRecordReader((FileSplit) split, job);
  }
}
