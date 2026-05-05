import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

public class MapFileFixer {

  /**
   * 主方法 - 修复 MapFile 索引
   *
   * <p><b>命令行参数:</b></p>
   * <ul>
   *   <li>args[0]: MapFile 的 URI (目录路径)</li>
   * </ul>
   *
   * <p><b>处理流程:</b></p>
   * <ol>
   *   <li>获取 FileSystem 实例</li>
   *   <li>打开 data 文件并读取键值类型</li>
   *   <li>调用 MapFile.fix() 重建索引文件</li>
   *   <li>打印创建的索引条目数量</li>
   * </ol>
   *
   * @param args 命令行参数
   * @throws Exception 如果发生错误
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    // 获取 MapFile 的 URI
    String mapUri = args[0];

    // 创建 Configuration 对象
    Configuration conf = new Configuration();

    // 获取 FileSystem 实例
    FileSystem fs = FileSystem.get(URI.create(mapUri), conf);

    // 创建 Path 对象
    Path map = new Path(mapUri);

    // 创建 data 文件的路径
    // MapFile 的 data 文件名是固定的: "data"
    Path mapData = new Path(map, MapFile.DATA_FILE_NAME);

    // 从 data sequence file 中读取键和值的类型
    // 这对于重建索引是必要的,因为索引需要知道键值类型
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);

    // 获取键和值的 Class 对象
    // 这些信息存储在 SequenceFile 的头部元数据中
    Class keyClass = reader.getKeyClass();
    Class valueClass = reader.getValueClass();
    reader.close();  // 关闭 reader,我们只需要类型信息

    // 调用 MapFile.fix() 修复/重建索引
    // 参数说明:
    //   fs: FileSystem 实例
    //   map: MapFile 目录路径
    //   keyClass: 键类型
    //   valueClass: 值类型
    //   delete拳旧索引?: false 表示保留旧索引(如果存在)
    //   conf: Configuration
    // 返回值: 索引包含的条目数量
    long entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf);

    // 打印结果
    System.out.printf("Created MapFile %s with %d entries\n", map, entries);
  }
}
