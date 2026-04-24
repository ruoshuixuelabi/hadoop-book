// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

// vv FileCopyWithProgress
/**
 * 将本地文件复制到HDFS并显示复制进度的示例程序。
 *
 * <p>此类演示了Hadoop文件复制操作的基本用法，包括：</p>
 * <ul>
 *   <li>从本地文件系统读取数据</li>
 *   <li>写入到HDFS文件系统</li>
 *   <li>使用Progressable接口报告复制进度</li>
 *   <li>带进度回调的文件创建方式</li>
 * </ul>
 *
 * <p>使用方式：</p>
 * <pre>
 *   hadoop jar your.jar /local/path/file.txt hdfs://localhost/user/foo/dest.txt
 * </pre>
 *
 * <p>技术要点：</p>
 * <ul>
 *   <li>FileSystem.create(Path, Progressable)创建文件时可以传入进度回调</li>
 *   <li>Progressable.progress()在每个数据块写入后被调用</li>
 *   <li>使用BufferedInputStream包装本地文件输入流以提高性能</li>
 *   <li>IOUtils.copyBytes的最后一个参数为true表示复制完成后关闭输出流</li>
 * </ul>
 *
 * @see FileSystemCat 文件读取示例
 * @see org.apache.hadoop.util.Progressable 进度回调接口
 */
public class FileCopyWithProgress {
  /**
   * 主方法：复制本地文件到HDFS。
   *
   * <p>程序流程：</p>
   * <ol>
   *   <li>解析命令行参数（源本地文件路径和目标HDFS路径）</li>
   *   <li>创建本地文件的缓冲输入流</li>
   *   <li>获取HDFS FileSystem实例</li>
   *   <li>创建HDFS输出流，带进度回调（每写入一个数据块打印一个点）</li>
   *   <li>使用IOUtils.copyBytes复制数据</li>
   * </ol>
   *
   * <p>进度显示：</p>
   * <p>在复制过程中，每写入一个数据块（通常为4KB），控制台会打印一个"."字符，
   * 让用户了解复制操作的进行状态。这对于大文件传输特别有用。</p>
   *
   * @param args 命令行参数，args[0]为本地源文件路径，args[1]为HDFS目标路径
   * @throws Exception 如果文件复制发生错误
   */
  public static void main(String[] args) throws Exception {
    String localSrc = args[0];
    String dst = args[1];
    // 使用BufferedInputStream包装以提高本地文件读取效率
    InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(dst), conf);
    // 创建HDFS输出流，并注册进度回调
    // 每当一个数据块被写入时，progress()方法会被调用
    OutputStream out = fs.create(new Path(dst), new Progressable() {
      /**
       * 进度回调方法。
       *
       * <p>此方法在每个数据块写入成功后被Hadoop调用，
       * 用于向用户报告复制进度。</p>
       */
      public void progress() {
        // 打印一个点表示一个数据块已传输
        System.out.print(".");
      }
    });
    // 复制本地文件到HDFS
    // 参数：输入流、输出流、缓冲区大小、关闭输出流标志
    // 最后一个参数为true表示复制完成后关闭输出流
    IOUtils.copyBytes(in, out, 4096, true);
  }
}
// ^^ FileCopyWithProgress
