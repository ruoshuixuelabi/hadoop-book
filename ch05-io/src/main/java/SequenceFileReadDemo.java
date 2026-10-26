// cc SequenceFileReadDemo Reading a SequenceFile
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

// vv SequenceFileReadDemo
/**
 * 读取 SequenceFile 的演示程序
 *
 * <p>读取时不需要事先知道键值的确切类型：
 * 文件头里记着创建时登记的类型，
 * {@link org.apache.hadoop.io.SequenceFile.Reader#getKeyClass()} 能取出来，
 * 再用反射创建实例即可
 * <p>程序还会打印每条记录之前的字节偏移量，以及该处是否是同步标记，
 * 便于理解 SequenceFile 的物理布局
 */
public class SequenceFileReadDemo {
  
  /**
   * 打开 SequenceFile 并逐条打印其中的记录
   *
   * @param args 第一个参数是源文件 URI
   * @throws IOException 文件系统操作失败
   */
  public static void main(String[] args) throws IOException {
    // 源文件 URI，同样支持本地路径和 hdfs:// 路径
    String uri = args[0];
    // 配置要与写入时保持一致，否则压缩等设置会对不上
    Configuration conf = new Configuration();
    // 按 URI 的 scheme 取得对应的 FileSystem 实现
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    // Path 是 Hadoop 对「路径」的抽象，同一套 API 可用于本地文件和 HDFS
    Path path = new Path(uri);

    // reader 必须在 try 之外声明，finally 里才能关闭它
    SequenceFile.Reader reader = null;
    // try 负责打开文件，finally 负责关闭
    try {
      // 打开 SequenceFile，Reader 会先读文件头
      reader = new SequenceFile.Reader(fs, path, conf);
      // 从文件头读出键类型，再用反射创建实例——这是自定义类型反序列化的通用做法
      Writable key = (Writable)
        ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      // 值类型同理
      Writable value = (Writable)
        ReflectionUtils.newInstance(reader.getValueClass(), conf);
      // 记录当前字节偏移量，用来标出这条记录在文件里的位置
      long position = reader.getPosition();
      // next 把下一条记录读进 key/value，返回 false 表示已到文件末尾
      while (reader.next(key, value)) {
        // syncSeen 为真表示上一条记录之后紧跟着一个同步标记
        String syncSeen = reader.syncSeen() ? "*" : "";
        // 把偏移量、同步标记和键值一起打印出来
        System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
        // 更新偏移量，指向下一条记录的开头
        position = reader.getPosition(); // beginning of next record
      }
    // finally 保证 reader 一定会被关闭
    } finally {
      // 关闭 Reader，释放底层文件句柄
      IOUtils.closeStream(reader);
    }
  }
}
// ^^ SequenceFileReadDemo