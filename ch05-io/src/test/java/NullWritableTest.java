import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.NullWritable} 的「零长度」特性
 *
 * <p>{@code NullWritable} 不携带任何数据，
 * 序列化后长度为 0 字节
 * <p>它的典型用途是作 MapReduce 的「空键」，
 * 或者在 {@code MapWritable} 里表示「只关心键、不关心值」
 */
public class NullWritableTest extends WritableTestBase {
  
  /**
   * 验证序列化结果长度为 0
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    // get() 返回单例，因为这个类型本来就没有任何状态
    NullWritable writable = NullWritable.get();
    // 长度为 0 正是「不携带数据」的直接体现
    assertThat(serialize(writable).length, is(0));
  }
}
