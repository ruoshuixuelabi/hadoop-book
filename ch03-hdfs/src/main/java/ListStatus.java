// cc ListStatus Shows the file statuses for a collection of paths in a Hadoop filesystem 
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

// vv ListStatus
/**
 * 列出给定路径下的文件状态
 *
 * <p>演示 HDFS 的元数据访问：{@link FileSystem#listStatus} 返回的是
 * {@link FileStatus} 对象列表，包含文件名、大小、权限、属主、修改时间等元数据
 * <p>与 "hadoop fs -ls" 等价，但演示的是编程方式而非命令行
 */
public class ListStatus {

  /**
   * 打印各路径下文件的路径
   *
   * @param args 第一个参数用于确定 FileSystem 实例，其余为要列出的路径
   * @throws Exception 访问文件系统元数据时发生错误
   */
  public static void main(String[] args) throws Exception {
    // 第一个参数决定使用哪个 FileSystem 实现
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    
    // 把命令行参数转成 Path 数组；listStatus 支持一次传入多个路径
    Path[] paths = new Path[args.length];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path(args[i]);
    }
    
    // 一次 RPC 取回所有路径下的文件状态，避免逐个调用带来的往返开销
    FileStatus[] status = fs.listStatus(paths);
    // FileUtil 提供从 FileStatus 数组提取 Path 数组的便捷方法
    Path[] listedPaths = FileUtil.stat2Paths(status);
    for (Path p : listedPaths) {
      System.out.println(p);
    }
  }
}
// ^^ ListStatus
