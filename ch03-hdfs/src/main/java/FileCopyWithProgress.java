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
 * 把本地文件复制到 HDFS，并在复制过程中显示进度
 *
 * <p>演示 HDFS 写入 API 的一个实用扩展点：{@link Progressable}
 * <p>{@code FileSystem.create} 接受一个 Progressable 回调，
 * Hadoop 在传输数据期间会周期性调用 {@code progress()}，
 * 借此可以向用户反馈「任务仍在进行」，避免长时间无响应造成的困惑
 */
public class FileCopyWithProgress {
  /**
   * 复制本地文件到 HDFS 并打印进度点
   *
   * @param args 第一个参数为本地源文件路径，第二个参数为目标路径
   * @throws Exception 读取本地文件或写入 HDFS 时发生错误
   */
  public static void main(String[] args) throws Exception {
    String localSrc = args[0];
    String dst = args[1];
    
    // 加缓冲包装：默认的 FileInputStream 每次读只取少量字节，
    // 缓冲后能显著减少系统调用次数
    InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(dst), conf);
    // create 的第二个参数是 Progressable，Hadoop 会周期性地回调它报告进度
    OutputStream out = fs.create(new Path(dst), new Progressable() {
      public void progress() {
        // 每回调一次打印一个点，形成最简单的进度条
        System.out.print(".");
      }
    });
    
    // 最后一个参数 true 表示复制结束后同时关闭输入流与输出流
    IOUtils.copyBytes(in, out, 4096, true);
  }
}
// ^^ FileCopyWithProgress
