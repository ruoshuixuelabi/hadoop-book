// cc FileSystemCat Displays files from a Hadoop filesystem on standard output by using the FileSystem directly

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemCat
/**
 * 使用FileSystem API读取HDFS文件内容并输出到标准输出的示例程序。
 *
 * <p>此类通过Hadoop的FileSystem API直接读取HDFS文件系统中的文件，
 * 这是Hadoop官方推荐的文件系统操作方式，比URLCat更灵活，
 * 支持更多配置选项。</p>
 *
 * <p>使用方式：</p>
 * <pre>
 *   hadoop jar your.jar hdfs://localhost/user/foo/file.txt
 * </pre>
 *
 * <p>与URLCat的区别：</p>
 * <ul>
 *   <li>FileSystem API是Hadoop原生的文件系统接口，更加强大和灵活</li>
 *   <li>支持更多的文件系统配置选项</li>
 *   <li>支持多种文件系统（HDFS、本地文件系统、S3等）</li>
 *   <li>可以获取更多的文件系统元信息</li>
 * </ul>
 *
 * @see URLCat 使用URLStreamHandler读取文件的简化方式
 * @see FileSystemDoubleCat 读取文件内容并输出两次的示例
 */
public class FileSystemCat {

  /**
   * 主方法：读取并显示HDFS文件内容。
   *
   * <p>程序流程：</p>
   * <ol>
   *   <li>从命令行参数获取HDFS文件URI</li>
   *   <li>创建Configuration对象加载Hadoop配置</li>
   *   <li>获取对应的FileSystem实例</li>
   *   <li>打开文件输入流并复制内容到标准输出</li>
   *   <li>关闭输入流释放资源</li>
   * </ol>
   *
   * @param args 命令行参数，第一个参数为HDFS文件URI
   * @throws Exception 如果文件读取发生错误
   */
  public static void main(String[] args) throws Exception {
    String uri = args[0];
    // 创建Configuration对象，默认从classpath加载hadoop配置
    Configuration conf = new Configuration();
    // 根据URI获取对应的FileSystem实例
    // FileSystem.get()会根据URI的scheme（hdfs://, file://等）选择合适的文件系统
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    InputStream in = null;
    try {
      // 打开文件输入流
      in = fs.open(new Path(uri));
      // 使用4KB缓冲区复制数据到标准输出
      // copyBytes内部会处理循环读取和写入直到数据全部传输完成
      IOUtils.copyBytes(in, System.out, 4096, false);
    }
    finally {
      // 关闭输入流，确保资源被释放
      IOUtils.closeStream(in);
    }
  }
}
// ^^ FileSystemCat
