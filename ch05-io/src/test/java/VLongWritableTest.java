import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.VLongWritable} 的可变长整数编码
 *
 * <p>它是 {@code VIntWritable} 的 64 位版本，
 * 编码思路完全相同，只是最多可占 9 字节
 */
public class VLongWritableTest extends WritableTestBase {
  
  /**
   * 验证可变长整数在各个数值区间上占用的字节数
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    assertThat(serializeToString(new VLongWritable(1)), is("01")); // 1 byte
    // 最高位为 0 表示单字节，范围是 -112 ~ 127
    assertThat(serializeToString(new VLongWritable(127)), is("7f")); // 1 byte
    assertThat(serializeToString(new VLongWritable(128)), is("8f80")); // 2 byte
    // 超过 127 就需要第 2 个字节
    assertThat(serializeToString(new VLongWritable(163)), is("8fa3")); // 2 byte
    // 64 位整数最多 9 字节
    assertThat(serializeToString(new VLongWritable(Long.MAX_VALUE)), is("887fffffffffffffff")); // 9 byte
    // 负数同样最多 9 字节
    assertThat(serializeToString(new VLongWritable(Long.MIN_VALUE)), is("807fffffffffffffff")); // 9 byte
  }
}
