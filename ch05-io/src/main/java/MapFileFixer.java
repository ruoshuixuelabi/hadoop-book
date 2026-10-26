import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

/**
 * 修复 MapFile 索引文件的工具
 *
 * <p>MapFile 由两个文件组成：数据文件（{@code .data}）和索引文件（{@code .index}）。
 * 有时数据文件是好的，但索引文件丢失或损坏，
 * 此时用 {@link org.apache.hadoop.io.MapFile#fix} 重新扫描数据文件生成索引即可
 */
public class MapFileFixer {

  /**
   * 读取数据文件的键值类型，并据此重建索引文件
   *
   * @param args 第一个参数是 MapFile 的目录 URI
   * @throws Exception 文件系统操作失败
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    // MapFile 的目录 URI
    String mapUri = args[0];
    
    // Configuration 汇总了 core-site.xml、hdfs-site.xml 等配置文件里的设置
    Configuration conf = new Configuration();
    
    // FileSystem.get 会按 URI 的 scheme 缓存实例，重复调用不会重复建立连接
    FileSystem fs = FileSystem.get(URI.create(mapUri), conf);
    // map 指向 MapFile 目录本身
    Path map = new Path(mapUri);
    // 数据文件在 MapFile 目录下的固定名字是 data
    Path mapData = new Path(map, MapFile.DATA_FILE_NAME);
    
    // 数据文件本质上是 SequenceFile，打开它就能读到文件头里登记的类型
    // Get key and value types from data sequence file
    // 以只读方式打开数据文件
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);
    // 从文件头取出键的类型
    Class keyClass = reader.getKeyClass();
    // 从文件头取出值的类型
    Class valueClass = reader.getValueClass();
    // 类型取到就关掉，后续不再需要这个 Reader
    reader.close();
    
    // 下面重新扫描数据文件，生成索引文件
    // Create the map file index file
    // fix 会顺序扫描数据文件、重建索引，并返回重建出的条目数
    long entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf);
    // 打印重建结果，条目数应与数据文件里的记录数一致
    System.out.printf("Created MapFile %s with %d entries\n", map, entries);
  }
}
