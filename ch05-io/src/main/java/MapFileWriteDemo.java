import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

/**
 * 向 MapFile 写入键值对的演示程序
 *
 * <p>{@link org.apache.hadoop.io.MapFile} 是 Hadoop 的一种随机访问文件格式：
 * 它在 SequenceFile 的基础上额外维护一份索引，
 * 使得可以按 key 直接定位到对应记录，而不必从头顺序扫描
 * <p>代价是 key 必须按字典序递增写入，
 * 因为索引是靠「相邻 key 的偏移量」建立起来的
 */
public class MapFileWriteDemo {
  
  /** 循环写入的样例文本，让键与值形成有意义的组合 */
  private static final String[] DATA = {
    "One, two, buckle my shoe",
    "Three, four, shut the door",
    "Five, six, pick up sticks",
    "Seven, eight, lay them straight",
    "Nine, ten, a big fat hen"
  };
  
  /**
   * 创建 MapFile 并按递增的键写入 1024 条记录
   *
   * @param args 第一个参数是目标文件 URI
   * @throws IOException 文件系统操作失败
   */
  public static void main(String[] args) throws IOException {
    // 目标文件 URI，注意 MapFile 是一个「目录」，不是一个文件
    String uri = args[0];
    // Configuration 汇总了 core-site.xml、hdfs-site.xml 等配置文件里的设置
    Configuration conf = new Configuration();
    // FileSystem.get 会按 URI 的 scheme 缓存实例，重复调用不会重复建立连接
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    // key 和 value 的类型在创建 Writer 时登记，读取方才知道该反序列化成什么
    IntWritable key = new IntWritable();
    // value 用 Text 存放样例文本
    Text value = new Text();
    // writer 必须在 try 之外声明，finally 里才能关闭它
    MapFile.Writer writer = null;
    // try 负责创建 writer，finally 负责关闭，出异常时也不会泄漏文件句柄
    try {
      // MapFile 的 Writer 同时负责维护索引文件
      writer = new MapFile.Writer(conf, fs, uri,
          key.getClass(), value.getClass());
      
      // 循环写入 1024 条记录
      for (int i = 0; i < 1024; i++) {
        // 键必须严格递增，否则 MapFile 无法建立正确的索引
        key.set(i + 1);
        // 取模循环使用样例文本，避免数组越界
        value.set(DATA[i % DATA.length]);
        // append 把一对 key/value 追加写入文件，索引随之更新
        writer.append(key, value);
      }
    // finally 保证 writer 一定会被关闭
    } finally {
      // 关闭时会把索引写完并落盘
      IOUtils.closeStream(writer);
    }
  }
}
