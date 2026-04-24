/**
 * MapFileFixer - 修复损坏的 MapFile 索引文件
 *
 * <p>MapFile 是一种带有索引的 SequenceFile 变体,用于支持快速键值查找。
 * 索引文件(index)默认每 128 条记录创建一个索引项,用于快速定位。
 * 如果索引文件损坏或丢失,可以使用 MapFile.fix() 方法重建。</p>
 *
 * <p><b>MapFile 结构:</b></p>
 * <ul>
 *   <li>data 文件: 存储实际的键值对序列,按键排序</li>
 *   <li>index 文件: 存储索引,包含键和文件位置,用于快速随机访问</li>
 * </ul>
 *
 * <p><b>使用场景:</b></p>
 * <ul>
 *   <li>索引文件意外删除或损坏</li>
 *   <li>需要重新生成索引以优化查找性能</li>
 *   <li>MapReduce 任务中断导致索引不完整</li>
 * </ul>
 *
 * <p><b>注意事项:</b></p>
 * <ul>
 *   <li>data 文件必须存在且完整</li>
 *   <li>键值对必须按键排序</li>
 *   <li>重建索引会遍历整个 data 文件</li>
 * </ul>
 *
 * @see MapFile
 * @see MapFile.Writer
 * @see MapFile.Reader
 */
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
