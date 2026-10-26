// == VIntWritableTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.VIntWritable} 的可变长整数编码
 *
 * <p>{@code VIntWritable} 用可变长整数保存 int：
 * 数值小的时候只占 1 字节，越大占用的字节越多，
 * 最多 5 字节
 * <p>它比定长的 {@code IntWritable} 更省空间，
 * 代价是取值时需要多一步解码
 */
public class VIntWritableTest extends WritableTestBase {
  
  /**
   * 验证 163 的可变长编码
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    // vv VIntWritableTest
    byte[] data = serialize(new VIntWritable(163));
    // 0x8f 的最高位是 1，表示「后面还有字节」；0xa3 是末字节
    assertThat(StringUtils.byteToHexString(data), is("8fa3"));
    // ^^ VIntWritableTest
  }
  
  /**
   * 验证可变长整数在各个数值区间上占用的字节数
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void testSizes() throws IOException {
    assertThat(serializeToString(new VIntWritable(1)), is("01")); // 1 byte
    assertThat(serializeToString(new VIntWritable(-112)), is("90")); // 1 byte
    // 最高位为 0 表示只有这一个字节，范围是 -112 ~ 127
    assertThat(serializeToString(new VIntWritable(127)), is("7f")); // 1 byte
    assertThat(serializeToString(new VIntWritable(128)), is("8f80")); // 2 byte
    // 超过 127 就需要第 2 个字节了
    assertThat(serializeToString(new VIntWritable(163)), is("8fa3")); // 2 byte
    // 32 位整数最多 5 字节
    assertThat(serializeToString(new VIntWritable(Integer.MAX_VALUE)),
        is("8c7fffffff")); // 5 byte
    // 负数同样最多 5 字节
    assertThat(serializeToString(new VIntWritable(Integer.MIN_VALUE)),
        is("847fffffff")); // 5 byte
  }
}
