// == BytesWritableTest
// == BytesWritableTest-Capacity
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.BytesWritable} 的序列化格式与容量行为
 *
 * <p>{@code BytesWritable} 是 Hadoop 里最常用的二进制载体，
 * 内部维护一个 {@code byte[]} 缓冲区和一个「有效长度」
 * <p>它的序列化格式是「4 字节大端长度 + 有效字节」，
 * 这个格式在 {@code SequenceFile}、{@code MapFile} 里都能见到
 */
public class BytesWritableTest extends WritableTestBase {
  
  /**
   * 验证字节数组的序列化格式，以及扩容后有效长度不被改变
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    // vv BytesWritableTest
    // 构造时传入的就是有效内容，长度为 2
    BytesWritable b = new BytesWritable(new byte[] { 3, 5 });
    // 序列化结果 = 4 字节大端长度（00000002）+ 2 个有效字节（0305）
    byte[] bytes = serialize(b);
    assertThat(StringUtils.byteToHexString(bytes), is("000000020305"));
    // ^^ BytesWritableTest
    
    // vv BytesWritableTest-Capacity
    // 扩容只把缓冲区变大，不动有效长度，逻辑上仍是那 2 个字节
    b.setCapacity(11);
    assertThat(b.getLength(), is(2));
    // 但底层缓冲区确实变成了 11 字节——这就是「长度」与「容量」的区别
    assertThat(b.getBytes().length, is(11));
    // ^^ BytesWritableTest-Capacity
  }
}
