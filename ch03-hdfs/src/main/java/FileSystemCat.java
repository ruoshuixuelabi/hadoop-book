// cc FileSystemCat Displays files from a Hadoop filesystem on standard output by using the FileSystem directly
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemCat
/**
 * 通过 FileSystem API 把 HDFS 上的文件内容打印到标准输出
 *
 * <p>这是 Hadoop 提供的 "hadoop fs -cat" 命令的等价实现，
 * 用于演示最基础的 HDFS 读取流程：取得 FileSystem 实例 → 打开文件 → 复制字节流
 * <p>路径可以是 HDFS 路径，也可以是本地文件路径或任意 scheme，
 * 具体由 {@link FileSystem#get} 按 URI 的 scheme 分派到对应实现
 */
public class FileSystemCat {

  /**
   * 打印指定路径的文件内容
   *
   * @param args 第一个参数为要读取的路径
   * @throws Exception 打开文件或读取过程中发生 I/O 错误
   */
  public static void main(String[] args) throws Exception {
    String uri = args[0];
    // Configuration 会加载 core-site.xml 等配置文件，是访问集群的必备上下文
    Configuration conf = new Configuration();
    // 按 URI 的 scheme 选出对应的 FileSystem 实现（HDFS / 本地 / S3 ...）
    // 并建立与 NameNode 的连接
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    InputStream in = null;
    try {
      in = fs.open(new Path(uri));
      // 缓冲区 4096 字节；最后一个参数 false 表示用完不关闭 System.out
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      // 放在 finally 中确保异常路径下也能释放连接
      IOUtils.closeStream(in);
    }
  }
}
// ^^ FileSystemCat
