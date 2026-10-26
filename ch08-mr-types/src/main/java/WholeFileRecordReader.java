// cc WholeFileRecordReader The RecordReader used by WholeFileInputFormat for reading a whole file as a record
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//vv WholeFileRecordReader
/**
 * 读取整个文件作为一条记录的 {@link RecordReader}
 *
 * <p>与常规的逐行读取不同，这里 {@code nextKeyValue()} 只会被成功调用一次：
 * 第一次调用把整个文件读进内存，之后一律返回 {@code false}
 */
class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
  
  // 当前分片：记录着文件路径与长度
  private FileSplit fileSplit;
  // 作业配置：用来定位 FileSystem
  private Configuration conf;
  // 读出来的文件内容
  private BytesWritable value = new BytesWritable();
  // 是否已经读过——保证整个文件只被读一次
  private boolean processed = false;

  /**
   * 保存分片与配置
   *
   * @param split 输入分片
   * @param context 任务上下文
   * @throws IOException 初始化失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // 分片一定是 FileSplit
    this.fileSplit = (FileSplit) split;
    // 取出配置，供后面定位 FileSystem
    this.conf = context.getConfiguration();
  }
  
  /**
   * 读取整个文件
   *
   * @return 第一次调用返回 {@code true}，之后返回 {@code false}
   * @throws IOException 读取失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // 只在第一次调用时真正读数据
    if (!processed) {
      // 按文件长度一次性分配缓冲区
      byte[] contents = new byte[(int) fileSplit.getLength()];
      // 文件路径
      Path file = fileSplit.getPath();
      // 取得该路径所在的文件系统（HDFS 或本地文件系统）
      FileSystem fs = file.getFileSystem(conf);
      // 提前声明，以便在 finally 里关闭
      FSDataInputStream in = null;
      try {
        // 打开文件输入流
        in = fs.open(file);
        // readFully 保证读满，否则缓冲区里会留下 0 字节
        IOUtils.readFully(in, contents, 0, contents.length);
        // 把读到的内容放进 value
        value.set(contents, 0, contents.length);
      } finally {
        // 无论成功与否都要关闭流
        IOUtils.closeStream(in);
      }
      // 标记为已读，之后的调用直接返回 false
      processed = true;
      // 本次调用产出了一条记录
      return true;
    }
    // 已经读过了，没有更多记录
    return false;
  }
  
  /**
   * 返回当前记录的键
   *
   * <p>整文件模式下没有有意义的键，统一返回 {@link NullWritable}
   *
   * @return 空的键
   * @throws IOException 读取失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    // 整文件模式没有有意义的键
    return NullWritable.get();
  }

  /**
   * 返回当前记录的值，即整个文件的内容
   *
   * @return 文件内容
   * @throws IOException 读取失败
   * @throws InterruptedException 任务被中断
   */
  @Override
  public BytesWritable getCurrentValue() throws IOException,
      InterruptedException {
    // 文件的完整内容
    return value;
  }

  /**
   * 返回进度
   *
   * @return 未读时 0，已读完时 1
   * @throws IOException 读取失败
   */
  @Override
  public float getProgress() throws IOException {
    // 未读时是 0，整份读完就是 1
    return processed ? 1.0f : 0.0f;
  }

  /**
   * 关闭读取器
   *
   * <p>数据在 {@code nextKeyValue()} 里就读完了，这里无需再做清理
   *
   * @throws IOException 关闭失败
   */
  @Override
  public void close() throws IOException {
    // do nothing
  }
}
//^^ WholeFileRecordReader
