// cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemDoubleCat
/**
 * 通过 FileSystem API 把 HDFS 文件内容打印两遍，第二次利用流的可随机访问特性
 *
 * <p>与 {@link FileSystemCat} 的唯一区别是：这里用的是
 * {@link FSDataInputStream} 而非普通 {@code InputStream}，
 * 因此支持 {@code seek} 定位，能倒回文件开头重新读一遍
 * <p>普通 {@code InputStream} 只保证顺序读取，一旦读完就无法回退，
 * 想再读一遍只能重新打开文件（多一次 NameNode RPC）
 */
public class FileSystemDoubleCat {

  /**
   * 打印指定文件的内容两遍
   *
   * @param args 第一个参数为要读取的路径
   * @throws Exception 打开文件、定位或读取过程中发生 I/O 错误
   */
  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    // 声明为 FSDataInputStream 而非 InputStream，才能使用 seek
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
      in.seek(0); // go back to the start of the file
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
// ^^ FileSystemDoubleCat
