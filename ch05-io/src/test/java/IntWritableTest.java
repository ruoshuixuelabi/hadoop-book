// == IntWritableTest
// == IntWritableTest-ValueConstructor
// == IntWritableTest-SerializedLength
// == IntWritableTest-SerializedBytes
// == IntWritableTest-Deserialization
// == IntWritableTest-Comparator
// == IntWritableTest-ObjectComparison
// == IntWritableTest-BytesComparison
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

/**
 * 验证 {@link org.apache.hadoop.io.IntWritable} 的序列化与比较
 *
 * <p>{@code IntWritable} 是最基础的 Writable：
 * 一个 int 用固定 4 字节大端序表示
 * <p>本测试覆盖无参构造、带值构造、序列化长度、
 * 序列化字节、反序列化，以及比较器的对象级与字节级两条路径
 */
public class IntWritableTest extends WritableTestBase {
  
  /**
   * 走一遍「无参构造 → set → 序列化 → 反序列化」的完整流程
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void walkthroughWithNoArgsConstructor() throws IOException {
    // vv IntWritableTest
    IntWritable writable = new IntWritable();
    // 163 的十六进制是 0xa3，是后面断言序列化字节的根据
    writable.set(163);
    // ^^ IntWritableTest
    // 与带值构造走同一套校验逻辑
    checkWalkthrough(writable);
  }

  /**
   * 验证带值构造与无参构造 + set 的结果完全一致
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void walkthroughWithValueConstructor() throws IOException {
    // vv IntWritableTest-ValueConstructor
    IntWritable writable = new IntWritable(163);
    // ^^ IntWritableTest-ValueConstructor
    checkWalkthrough(writable);
  }

  /**
   * 校验一个 {@code IntWritable} 的长度、字节与反序列化结果
   *
   * @param writable 待校验的对象
   * @throws IOException 序列化失败
   */
  private void checkWalkthrough(IntWritable writable) throws IOException {
    // vv IntWritableTest-SerializedLength
    byte[] bytes = serialize(writable);
    // int 固定占 4 字节，所以序列化结果一定是 4 字节
    assertThat(bytes.length, is(4));
    // ^^ IntWritableTest-SerializedLength
    
    // vv IntWritableTest-SerializedBytes
    // 4 字节大端序：高位补零，低位是 0xa3
    assertThat(StringUtils.byteToHexString(bytes), is("000000a3"));
    // ^^ IntWritableTest-SerializedBytes
    
    // vv IntWritableTest-Deserialization
    IntWritable newWritable = new IntWritable();
    // 用一个新的空对象来接收，验证反序列化是「填充」而非「依赖已有值」
    deserialize(newWritable, bytes);
    // 读回来的值应当与原值一致
    assertThat(newWritable.get(), is(163));
    // ^^ IntWritableTest-Deserialization
  }
  
  /**
   * 验证 {@code IntWritable} 比较器的对象级与字节级两条路径
   *
   * @throws IOException 序列化失败
   */
  @Test
  @SuppressWarnings("unchecked")
  public void comparator() throws IOException {
    // vv IntWritableTest-Comparator
    // Hadoop 为每个 Writable 类型注册了默认比较器，这里直接取用
    RawComparator<IntWritable> comparator =
        WritableComparator.get(IntWritable.class);
    // ^^ IntWritableTest-Comparator
    
    // vv IntWritableTest-ObjectComparison
    IntWritable w1 = new IntWritable(163);
    IntWritable w2 = new IntWritable(67);
    // 163 > 67，对象级比较应当返回正数
    assertThat(comparator.compare(w1, w2), greaterThan(0));
    // ^^ IntWritableTest-ObjectComparison
    
    // vv IntWritableTest-BytesComparison
    byte[] b1 = serialize(w1);
    byte[] b2 = serialize(w2);
    // 字节级比较也必须给出同样的结论
    assertThat(comparator.compare(b1, 0, b1.length, b2, 0, b2.length),
        greaterThan(0));
    // ^^ IntWritableTest-BytesComparison
  }
  
  /**
   * 验证序列化字节的十六进制形式
   *
   * @throws IOException 序列化失败
   */
  @Test
  public void test() throws IOException {
    IntWritable src = new IntWritable(163);
    IntWritable dest = new IntWritable();
    // 163 = 0x000000a3
    assertThat(writeTo(src, dest), is("000000a3"));
    assertThat(dest.get(), is(src.get()));
  }

}
