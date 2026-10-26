// cc SequenceFileWriteDemo Writing a SequenceFile
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

// vv SequenceFileWriteDemo
/**
 * 向 SequenceFile 写入键值对的演示程序
 *
 * <p>{@link org.apache.hadoop.io.SequenceFile} 是 Hadoop 的二进制文件格式：
 * 它把「键、值」成对地连续写入一条记录，
 * 记录之间还会插入同步标记（sync marker），
 * 使得读取时可以在任意记录边界上快速定位
 * <p>相比纯文本文件，SequenceFile 的好处是：
 * 类型信息随数据一起保存，读取时不需要重新解析；
 * 并且可以压缩，节省存储空间
 */
public class SequenceFileWriteDemo {
  
  /** 循环写入的样例文本，让键与值形成有意义的组合 */
  private static final String[] DATA = {
    "One, two, buckle my shoe",
    "Three, four, shut the door",
    "Five, six, pick up sticks",
    "Seven, eight, lay them straight",
    "Nine, ten, a big fat hen"
  };
  
  /**
   * 创建 SequenceFile 并写入 100 条记录
   *
   * @param args 第一个参数是目标文件 URI
   * @throws IOException 文件系统操作失败
   */
  public static void main(String[] args) throws IOException {
    // 由 URI 决定用哪个 FileSystem：本地路径走 LocalFileSystem，hdfs:// 走 HDFS
    String uri = args[0];
    // Configuration 汇总了 core-site.xml、hdfs-site.xml 等配置文件里的设置
    Configuration conf = new Configuration();
    // FileSystem.get 会按 URI 的 scheme 缓存实例，重复调用不会重复建立连接
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    // Path 是 Hadoop 对「路径」的抽象，同一套 API 可用于本地文件和 HDFS
    Path path = new Path(uri);

    // key 和 value 的类型在创建 Writer 时登记，读取方才知道该反序列化成什么
    IntWritable key = new IntWritable();
    // value 用 Text 存放样例文本
    Text value = new Text();
    // writer 必须在 try 之外声明，这样 finally 里才能关闭它
    SequenceFile.Writer writer = null;
    // try 负责创建 writer，finally 负责关闭，出异常时也不会泄漏文件句柄
    try {
      // 把 key/value 的类型传进去，文件头里就会记下这两个类型
      writer = SequenceFile.createWriter(fs, conf, path,
          key.getClass(), value.getClass());
      
      // 循环写入 100 条记录
      for (int i = 0; i < 100; i++) {
        // 键递减写入，读取时若已排序，看起来就是升序
        key.set(100 - i);
        // 取模循环使用样例文本，避免数组越界
        value.set(DATA[i % DATA.length]);
        // getLength 是当前已写入的字节数，可以用来观察记录在文件里的位置
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
        // append 把一对 key/value 追加写入文件
        writer.append(key, value);
      }
    // finally 保证 writer 一定会被关闭
    } finally {
      // closeStream 内部已做 null 判断，这里不必再判空
      IOUtils.closeStream(writer);
    }
  }
}
// ^^ SequenceFileWriteDemo
