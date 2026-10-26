import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.*;

/**
 * 验证 {@link org.apache.hadoop.io.MapFile} 的随机查找能力
 *
 * <p>{@code MapFile} 是按 key 排序存放键值对的文件，
 * 内部维护一份索引，因此可以像数据库一样按 key 直接定位，
 * 不必从头顺序扫描
 * <p>本测试先写出样例文件，再验证「按 key 取值」与「定位到某条记录后继续顺序读」
 */
public class MapFileSeekTest {
  
  /** 样例 MapFile 的路径 */
  private static final String MAP_URI = "test.numbers.map";
  /** 打开样例文件的文件系统句柄 */
  private FileSystem fs;
  /** 读取 MapFile 的 reader */
  private MapFile.Reader reader;
  /** 读记录时复用的 key 对象 */
  private WritableComparable<?> key;
  /** 读记录时复用的 value 对象 */
  private Writable value;

  /**
   * 写出样例 MapFile 并打开读取端
   *
   * @throws IOException 写文件或打开文件失败
   */
  @Before
  public void setUp() throws IOException {
    // 复用示例程序来生成样例数据，避免在测试里重复造数据
    MapFileWriteDemo.main(new String[] { MAP_URI });

    Configuration conf = new Configuration();
    // 按 URI 的 scheme 取得文件系统；此处没有 scheme，用的是本地实现
    fs = FileSystem.get(URI.create(MAP_URI), conf);

    reader = new MapFile.Reader(fs, MAP_URI, conf);
    // 键类型是从文件头读出来的，用反射创建对应实例
    key = (WritableComparable<?>)
      ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    // 值类型同理
    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
  }

  /**
   * 删掉样例文件，避免测试之间互相干扰
   *
   * @throws IOException 删除失败
   */
  @After
  public void tearDown() throws IOException {
    // 第二个参数 true 表示递归删除整个目录
    fs.delete(new Path(MAP_URI), true);
  }
  
  /**
   * 验证按 key 直接取值的正确性
   *
   * @throws Exception 读取失败
   */
  @Test
  public void get() throws Exception {
    Text value = new Text();
    // get 直接按 key 定位，命中后把值填进 value
    reader.get(new IntWritable(496), value);
    // 496 对应的就是这一行童谣
    assertThat(value.toString(), is("One, two, buckle my shoe"));
  }
  
  /**
   * 验证定位到某条记录后可以继续顺序往下读
   *
   * @throws Exception 读取失败
   */
  @Test
  public void seek() throws Exception {
    // seek 定位到第一个不小于给定 key 的记录
    assertThat(reader.seek(new IntWritable(496)), is(true));
    // next 读出下一条记录，返回 true 表示还有记录可读
    assertThat(reader.next(key, value), is(true));
    // 定位到 496 之后，下一条就是 497
    assertThat(((IntWritable) key).get(), is(497));
    assertThat(((Text) value).toString(), is("Three, four, shut the door"));
  }
  
  

}
