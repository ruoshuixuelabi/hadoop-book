// == SequenceFileSeekAndSyncTest
// == SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
// == SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
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
 * 验证 {@link org.apache.hadoop.io.SequenceFile} 的定位与同步点机制
 *
 * <p>{@code SequenceFile} 是按记录顺序存放的文件，
 * 每条记录前都有长度信息，可以在记录内部随意定位；
 * 文件里还定期插入「同步标记」，
 * 用来标记一个可以安全停下来的位置
 * <p>本测试覆盖三种情况：
 * 定位到记录边界、定位到记录中间（应当失败）、
 * 以及从记录中间同步到下一个同步点
 */
public class SequenceFileSeekAndSyncTest {
  
  /** 样例 SequenceFile 的路径 */
  private static final String SF_URI = "test.numbers.seq";
  /** 打开样例文件的文件系统句柄 */
  private FileSystem fs;
  /** 读取 SequenceFile 的 reader */
  private SequenceFile.Reader reader;
  /** 读记录时复用的 key 对象 */
  private Writable key;
  /** 读记录时复用的 value 对象 */
  private Writable value;

  /**
   * 写出样例 SequenceFile 并打开读取端
   *
   * @throws IOException 写文件或打开文件失败
   */
  @Before
  public void setUp() throws IOException {
    // 复用示例程序生成样例数据
    SequenceFileWriteDemo.main(new String[] { SF_URI });
    
    Configuration conf = new Configuration();
    fs = FileSystem.get(URI.create(SF_URI), conf);
    Path path = new Path(SF_URI);

    reader = new SequenceFile.Reader(fs, path, conf);
    // 键类型从文件头读出，用反射创建实例
    key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
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
    // 递归删除整个目录
    fs.delete(new Path(SF_URI), true);
  }

  /**
   * 验证定位到记录边界后可以正常读出记录
   *
   * @throws IOException 读取失败
   */
  @Test
  public void seekToRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest
    // 359 恰好落在一条记录的起始位置
    reader.seek(359);
    assertThat(reader.next(key, value), is(true));
    // 读出的键值与 359 处那条记录吻合
    assertThat(((IntWritable) key).get(), is(95));
    // ^^ SequenceFileSeekAndSyncTest
  }
  
  /**
   * 验证定位到记录中间会抛出 {@link IOException}
   *
   * <p>{@code expected} 属性声明了本方法应当抛出的异常类型，
   * 测试框架据此判定用例通过
   *
   * @throws IOException 读取时按预期抛出
   */
  @Test(expected=IOException.class)
  public void seekToNonRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
    // 360 落在记录内部，读不出完整的记录，按预期抛异常
    reader.seek(360);
    reader.next(key, value); // fails with IOException
    // ^^ SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
  }
  
  /**
   * 验证从记录中间可以同步到下一个同步点
   *
   * @throws IOException 读取失败
   */
  @Test
  public void syncFromNonRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
    // sync 会跳过当前位置，落到下一个同步标记上
    reader.sync(360);
    // 同步点固定在 2021 这个字节偏移处
    assertThat(reader.getPosition(), is(2021L));
    assertThat(reader.next(key, value), is(true));
    // 从同步点接着读，读到的是 59
    assertThat(((IntWritable) key).get(), is(59));
    // ^^ SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
  }
  
  /**
   * 验证在最后一个同步点之后同步会直接落到文件末尾
   *
   * @throws IOException 读取失败
   */
  @Test
  public void syncAfterLastSyncPoint() throws IOException {
    // 位置已经越过最后一个同步点
    reader.sync(4557);
    // 于是直接跳到文件末尾
    assertThat(reader.getPosition(), is(4788L));
    // 没有记录可读了，next 返回 false
    assertThat(reader.next(key, value), is(false));
  }

}
