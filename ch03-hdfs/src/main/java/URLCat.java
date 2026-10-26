// cc URLCat Displays files from a Hadoop filesystem on standard output using a URLStreamHandler
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

// vv URLCat
/**
 * 用标准 {@link URL} 机制读取并打印 HDFS 上的文件
 *
 * <p>这是 {@link FileSystemCat} 之外的另一种写法：不直接使用 FileSystem API，
 * 而是把 HDFS 路径当作 URL 处理
 * <p>关键在静态初始化块：注册 {@link FsUrlStreamHandlerFactory} 作为全局的
 * URL 协议处理器工厂，JVM 之后才能识别 {@code hdfs://} 这类非标准 scheme，
 * 并把请求转交给 FileSystem 处理
 */
public class URLCat {

  static {
    // 必须在任何 URL 被解析之前完成注册：setURLStreamHandlerFactory 全局只允许设置一次
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
  }
  
  /**
   * 打印指定 URL 指向的文件内容
   *
   * @param args 第一个参数为要读取的 URL
   * @throws Exception 打开或读取流时发生错误
   */
  public static void main(String[] args) throws Exception {
    InputStream in = null;
    try {
      // 因为已注册 FsUrlStreamHandlerFactory，这里能直接打开 hdfs:// 路径
      in = new URL(args[0]).openStream();
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
// ^^ URLCat
