// cc URLCat Displays files from a Hadoop filesystem on standard output using a URLStreamHandler

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

// vv URLCat
/**
 * 通过URLStreamHandler读取HDFS文件内容的示例程序。
 *
 * <p>此类演示如何使用Java标准的URL类配合Hadoop的FsUrlStreamHandlerFactory
 * 来读取HDFS文件系统中的文件内容，类似于读取本地文件一样简单。</p>
 *
 * <p>使用方式：</p>
 * <pre>
 *   hadoop jar your.jar hdfs://localhost/user/foo/file.txt
 * </pre>
 *
 * <p>技术原理：</p>
 * <ul>
 *   <li>通过静态初始化块注册Hadoop的URLStreamHandlerFactory</li>
 *   <li>该Factory支持hdfs://等Hadoop文件系统的URL协议</li>
 *   <li>使用标准的URL.openStream()打开输入流</li>
 *   <li>通过IOUtils.copyBytes将数据流复制到标准输出</li>
 * </ul>
 *
 * <p>注意：静态块在类加载时执行，确保URLStreamHandlerFactory只被注册一次。
 * JVM只允许设置一次URLStreamHandlerFactory，后续设置会无效。</p>
 *
 * @see FileSystemCat 使用FileSystem API读取文件的另一种方式
 */
public class URLCat {

  /**
   * 静态初始化块：注册Hadoop的FsUrlStreamHandlerFactory。
   *
   * <p>此静态块在类加载时执行，用于注册Hadoop的URL流处理器工厂。
   * 这使得URL类能够识别hdfs://等Hadoop文件系统协议。</p>
   *
   * <p>重要：每个JVM只能设置一次URLStreamHandlerFactory，
   * 如果之前已有其他Factory注册过，则此设置不会生效。</p>
   */
  static {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
  }

  /**
   * 主方法：读取并显示HDFS文件内容。
   *
   * <p>从命令行参数获取HDFS文件URL，打开输入流并将其内容
   * 复制到标准输出。</p>
   *
   * <p>使用示例：</p>
   * <pre>
   *   java URLCat hdfs://localhost/user/foo/file.txt
   * </pre>
   *
   * @param args 命令行参数，第一个参数为HDFS文件URL（hdfs://...）
   * @throws Exception 如果文件读取发生错误
   */
  public static void main(String[] args) throws Exception {
    InputStream in = null;
    try {
      // 使用Java标准URL类打开HDFS文件的输入流
      // URL格式如：hdfs://localhost/user/foo/file.txt
      in = new URL(args[0]).openStream();
      // 复制输入流内容到标准输出，使用4KB缓冲区
      // 最后一个参数false表示操作完成后不关闭输入流（由finally块处理）
      IOUtils.copyBytes(in, System.out, 4096, false);
    }
    finally {
      // 确保输入流被关闭，释放系统资源
      IOUtils.closeStream(in);
    }
  }
}
// ^^ URLCat
