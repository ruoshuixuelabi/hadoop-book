import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.BooleanWritable} 的序列化格式
 *
 * <p>{@code BooleanWritable} 用一个字节表示真假：
 * {@code 0} 表示 false，{@code 1} 表示 true
 */
public class BooleanWritableTest extends WritableTestBase {
  
  /**
   * 验证真假值序列化后恰好占一个字节，且能原样读回
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    BooleanWritable src = new BooleanWritable(true);
    BooleanWritable dest = new BooleanWritable();
    // "01" 是十六进制串：00 表示长度 1，01 才是那个真值字节
    assertThat(writeTo(src, dest), is("01"));
    // 读回来的值必须与原值一致，说明反序列化定位正确
    assertThat(dest.get(), is(src.get()));
  }
}
