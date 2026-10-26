import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.ObjectWritable} 能包装任意 Java 对象
 *
 * <p>{@code ObjectWritable} 用「类名 + 对象内容」来序列化，
 * 因此它能装下任何实现了 {@code Serializable} 的对象，
 * 代价是体积大、速度慢
 * <p>本例传入的是 {@code Integer.TYPE}（基本类型 int 的 Class 对象），
 * 说明基本类型同样可以被包装
 */
public class ObjectWritableTest extends WritableTestBase {
  
  /**
   * 验证被包装的值能原样克隆回来
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    // 第一个参数是值的实际类型，第二个是被包装的值
    ObjectWritable src = new ObjectWritable(Integer.TYPE, 163);
    ObjectWritable dest = new ObjectWritable();
    // 反序列化时靠写入的类型信息把对象还原出来
    WritableUtils.cloneInto(dest, src);
    // 取值时需要转型成原本的类型
    assertThat((Integer) dest.get(), is(163));
  }
}
