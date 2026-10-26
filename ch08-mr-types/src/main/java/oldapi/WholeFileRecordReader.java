package oldapi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 读取整个文件作为一条记录的 {@link RecordReader}（v1 旧 API）
 *
 * <p>与常规的逐行读取不同，这里 {@code next()} 只会被成功调用一次：
 * 第一次调用把整个文件读进内存，之后一律返回 {@code false}
 */
class WholeFileRecordReader implements RecordReader<NullWritable, BytesWritable> {
  
  // 当前分片：记录着文件路径与长度
  private FileSplit fileSplit;
  // 作业配置：用来定位 FileSystem
  private Configuration conf;
  // 是否已经读过——保证整个文件只被读一次
  private boolean processed = false;
  
  /**
   * 构造读取器
   *
   * <p>v1 的 {@code RecordReader} 没有 {@code initialize} 钩子，
   * 分片与配置只能通过构造器传进来
   *
   * @param fileSplit 输入分片
   * @param conf 作业配置
   * @throws IOException 初始化失败
   */
  public WholeFileRecordReader(FileSplit fileSplit, Configuration conf)
      throws IOException {
    // 保存分片
    this.fileSplit = fileSplit;
    // 保存配置，供后面定位 FileSystem
    this.conf = conf;
  }

  /**
   * 创建一个空键
   *
   * <p>整文件模式下没有有意义的键，统一返回 {@link NullWritable}
   *
   * @return 空的键
   */
  @Override
  public NullWritable createKey() {
    // 整文件模式没有有意义的键
    return NullWritable.get();
  }

  /**
   * 创建一个空值对象
   *
   * <p>真正的内容由 {@link #next(NullWritable, BytesWritable)} 填进去
   *
   * @return 空的值对象
   */
  @Override
  public BytesWritable createValue() {
    // 每次返回一个新的空值对象，由 next 填充
    return new BytesWritable();
  }

  /**
   * 返回当前读取位置
   *
   * @return 未读时 0，已读完时是文件长度
   * @throws IOException 读取失败
   */
  @Override
  public long getPos() throws IOException {
    // 已读完就是文件长度，否则是 0
    return processed ? fileSplit.getLength() : 0;
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
   * 读取整个文件
   *
   * @param key 输出的键（内容为空）
   * @param value 输出的值，会被填成整个文件的内容
   * @return 第一次调用返回 {@code true}，之后返回 {@code false}
   * @throws IOException 读取失败
   */
  @Override
  public boolean next(NullWritable key, BytesWritable value) throws IOException {
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
   * 关闭读取器
   *
   * <p>数据在 {@code next()} 里就读完了，这里无需再做清理
   *
   * @throws IOException 关闭失败
   */
  @Override
  public void close() throws IOException {
    // do nothing
  }
}
