// cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemDoubleCat
/**
 * 演示如何使用seek操作在HDFS文件中来回移动的示例程序。
 *
 * <p>此类读取指定的HDFS文件两次：第一次从头到尾读取整个文件内容，
 * 然后使用seek(0)将文件指针重置到文件开头，再次从头到尾读取。
 * 这展示了HDFS随机读取的能力。</p>
 *
 * <p>HDFS与本地文件系统的重要区别：</p>
 * <ul>
 *   <li>HDFS支持高效的seek操作，因为数据块分布在多个节点上</li>
 *   <li>seek操作可能涉及网络通信（对于远程数据块）</li>
 *   <li>HDFS读取是流式的，不支持本地文件系统那样的指针随机移动</li>
 * </ul>
 *
 * <p>使用方式：</p>
 * <pre>
 *   hadoop jar your.jar hdfs://localhost/user/foo/file.txt
 * </pre>
 *
 * <p>输出：文件内容会被打印两次。</p>
 *
 * @see FileSystemCat 单次读取文件的示例
 */
public class FileSystemDoubleCat {

  /**
   * 主方法：读取并显示HDFS文件内容两次。
   *
   * <p>程序流程：</p>
   * <ol>
   *   <li>获取FileSystem实例并打开文件</li>
   *   <li>第一次复制文件内容到标准输出（从位置0到文件末尾）</li>
   *   <li>调用seek(0)将文件指针重置到文件开头</li>
   *   <li>第二次复制文件内容到标准输出</li>
   *   <li>关闭输入流</li>
   * </ol>
   *
   * <p>注意：</p>
   * <ul>
   *   <li>使用FSDataInputStream（而非普通的InputStream）以支持seek操作</li>
   *   <li>seek(0)是相对昂贵的操作，涉及重新定位数据块</li>
   * </ul>
   *
   * @param args 命令行参数，第一个参数为HDFS文件URI
   * @throws Exception 如果文件读取发生错误
   */
  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    FSDataInputStream in = null;
    try {
      // 打开文件，返回FSDataInputStream支持seek操作
      in = fs.open(new Path(uri));
      // 第一次复制文件内容到标准输出（缓冲区大小4096字节）
      IOUtils.copyBytes(in, System.out, 4096, false);
      // 将文件指针重置到文件开头位置0，准备第二次读取
      // 这是HDFS随机访问的演示
      in.seek(0);
      // 第二次复制文件内容到标准输出
      IOUtils.copyBytes(in, System.out, 4096, false);
    }
    finally {
      IOUtils.closeStream(in);
    }
  }
}
// ^^ FileSystemDoubleCat
