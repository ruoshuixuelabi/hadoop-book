import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.GenericWritable} 能承载不同类型的元素
 *
 * <p>{@code GenericWritable} 装的是任意 {@code Writable}，
 * 序列化时先写「类型编号」再写「内容」，
 * 反序列化时按编号找回类型再读内容
 * <p>本测试先后装入 {@code Text} 与 {@code BytesWritable}，
 * 验证同一个对象在换类型后依然能正确往返
 */
public class GenericWritableTest extends WritableTestBase {
  
  /**
   * 验证同一容器先后承载文本与二进制两种元素都能正确克隆
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    BinaryOrTextWritable src = new BinaryOrTextWritable();
    // 先装一个文本元素
    src.set(new Text("text"));
    BinaryOrTextWritable dest = new BinaryOrTextWritable();
    // 克隆时元素类型编号会被一并写入
    WritableUtils.cloneInto(dest, src);
    assertThat((Text) dest.get(), is(new Text("text")));
    
    // 再换成二进制元素，类型编号随之改变
    src.set(new BytesWritable(new byte[] {3, 5}));
    // 第二次克隆覆盖第一次的结果
    WritableUtils.cloneInto(dest, src);
    assertThat(((BytesWritable) dest.get()).getLength(), is(2)); // TODO proper assert
  }
}
