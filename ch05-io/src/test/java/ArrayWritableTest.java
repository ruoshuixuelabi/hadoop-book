// == ArrayWritableTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.ArrayWritable} 的填充、克隆与取出
 *
 * <p>{@code ArrayWritable} 是「一组同类型 Writable」的容器：
 * 构造时指定元素类型，之后用 {@code set} 整体替换数组
 * <p>本测试同时用到 {@code TextArrayWritable}
 * （{@code ArrayWritable} 的具体子类），
 * 演示 Hadoop 里常见的「抽象容器 + 具体实现」设计
 */
public class ArrayWritableTest extends WritableTestBase {
  
  /**
   * 验证数组容器能被正确填充、克隆并逐个取出元素
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    // vv ArrayWritableTest
    // 构造时只需说明元素类型，此刻数组本身还是空的
    ArrayWritable writable = new ArrayWritable(Text.class);
    // ^^ ArrayWritableTest
    // set 是整体替换语义，一次换掉全部元素，而不是逐个填
    writable.set(new Text[] { new Text("cat"), new Text("dog") });
    
    // 接收端用具体子类，元素才能被读回成 Text
    TextArrayWritable dest = new TextArrayWritable();
    // 克隆走的是「序列化 + 反序列化」这条路，元素个数应当不变
    WritableUtils.cloneInto(dest, writable);
    // 克隆后元素个数不变
    assertThat(dest.get().length, is(2));
    // TODO: fix cast, also use single assert
    // get() 返回的是 Object[]，要取元素得先转型成 Text
    assertThat((Text) dest.get()[0], is(new Text("cat")));
    assertThat((Text) dest.get()[1], is(new Text("dog")));
    
    // toArray() 的返回类型是 Object[]，同样需要转型
    Text[] copy = (Text[]) dest.toArray();
    assertThat(copy[0], is(new Text("cat")));
    assertThat(copy[1], is(new Text("dog")));
  }
}
