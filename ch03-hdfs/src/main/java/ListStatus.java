// cc ListStatus Shows the file statuses for a collection of paths in a Hadoop filesystem

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

// vv ListStatus
/**
 * 列出指定路径集合的文件状态信息。
 *
 * <p>此类演示如何获取HDFS中多个路径的文件状态（FileStatus）信息，
 * 并将FileStatus数组转换为Path数组以便显示。</p>
 *
 * <p>FileStatus包含的信息：</p>
 * <ul>
 *   <li>路径（Path）</li>
 *   <li>是否为目录（isDirectory()）</li>
 *   <li>文件长度（getLen()）</li>
 *   <li>修改时间（getModificationTime()）</li>
 *   <li>副本数（getReplication()）</li>
 *   <li>块大小（getBlockSize()）</li>
 *   <li>所有者（getOwner()）</li>
 *   <li>权限（getPermission()）</li>
 * </ul>
 *
 * <p>使用方式：</p>
 * <pre>
 *   hadoop jar your.jar hdfs://localhost/user/foo hdfs://localhost/user/bar
 * </pre>
 *
 * <p>此命令会列出user/foo和user/bar两个路径下所有文件/目录的路径。</p>
 *
 * @see ShowFileStatusTest 测试类，展示更详细的FileStatus信息
 */
public class ListStatus {
  /**
   * 主方法：列出指定路径的文件状态。
   *
   * <p>程序流程：</p>
   * <ol>
   *   <li>解析命令行参数获取URI和路径数组</li>
   *   <li>创建Configuration和FileSystem</li>
   *   <li>将命令行路径转换为Path对象数组</li>
   *   <li>调用FileSystem.listStatus()获取状态数组</li>
   *   <li>使用FileUtil.stat2Paths()将FileStatus数组转换为Path数组</li>
   *   <li>打印每个路径</li>
   * </ol>
   *
   * <p>listStatus与globStatus的区别：</p>
   * <ul>
   *   <li>listStatus：返回指定路径的直接内容（不递归）</li>
   *   <li>globStatus：支持通配符模式匹配</li>
   * </ul>
   *
   * @param args 命令行参数，args[0]为文件系统URI，后续参数为要查询的路径
   * @throws Exception 如果操作发生错误
   */
  public static void main(String[] args) throws Exception {
    // 第一个参数：文件系统URI（如 hdfs://localhost/）
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    // 将所有命令行路径转换为Path对象数组
    Path[] paths = new Path[args.length];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path(args[i]);
    }

    // 列出所有指定路径的状态
    FileStatus[] status = fs.listStatus(paths);

    // 将FileStatus数组转换为Path数组
    // stat2Paths是一个便捷方法，用于提取每个FileStatus中的路径
    Path[] listedPaths = FileUtil.stat2Paths(status);

    // 打印每个路径
    for (Path p : listedPaths) {
      System.out.println(p);
    }
  }
}
// ^^ ListStatus
