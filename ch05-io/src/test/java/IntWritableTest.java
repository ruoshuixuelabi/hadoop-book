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
import static org.junit.jupiter.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.Test;

/**
 * IntWritableTest - IntWritable 的单元测试
 *
 * <p>测试 IntWritable 的各种创建方式、序列化格式和比较功能。</p>
 *
 * <p><b>测试内容:</b></p>
 * <ul>
 *   <li>无参构造和值构造</li>
 *   <li>序列化长度 (固定 4 字节)</li>
 *   <li>序列化字节格式</li>
 *   <li>反序列化正确性</li>
 *   <li>RawComparator 比较</li>
 * </ul>
 *
 * @see IntWritable
 * @see WritableTestBase
 */
public class IntWritableTest extends WritableTestBase {

    /**
     * walkthroughWithNoArgsConstructor - 测试无参构造方式
     *
     * <p>使用无参构造然后 set() 方法设置值。</p>
     */
    @Test
    public void walkthroughWithNoArgsConstructor() throws IOException {
        // vv IntWritableTest
        // 创建空的 IntWritable
        IntWritable writable = new IntWritable();
        // 使用 set() 设置值
        writable.set(163);
        // ^^ IntWritableTest
        // 验证
        checkWalkthrough(writable);
    }

    /**
     * walkthroughWithValueConstructor - 测试值构造方式
     *
     * <p>使用带参数的构造函数直接设置值。</p>
     */
    @Test
    public void walkthroughWithValueConstructor() throws IOException {
        // vv IntWritableTest-ValueConstructor
        // 直接在构造时设置值
        IntWritable writable = new IntWritable(163);
        // ^^ IntWritableTest-ValueConstructor
        // 验证
        checkWalkthrough(writable);
    }

    /**
     * checkWalkthrough - 验证 IntWritable 的序列化/反序列化
     *
     * <p><b>验证步骤:</b></p>
     * <ol>
     *   <li>验证序列化长度为 4 字节</li>
     *   <li>验证序列化字节格式 (big-endian)</li>
     *   <li>验证反序列化后值不变</li>
     * </ol>
     *
     * @param writable 要验证的 IntWritable
     * @throws IOException 如果发生 I/O 错误
     */
    private void checkWalkthrough(IntWritable writable) throws IOException {
        // vv IntWritableTest-SerializedLength
        // 序列化
        byte[] bytes = serialize(writable);
        // IntWritable 序列化后固定 4 字节
        assertThat(bytes.length, is(4));
        // ^^ IntWritableTest-SerializedLength

        // vv IntWritableTest-SerializedBytes
        // 验证序列化字节
        // 163 的十六进制是 0xa3, big-endian 表示为 00 00 00 a3
        assertThat(StringUtils.byteToHexString(bytes), is("000000a3"));
        // ^^ IntWritableTest-SerializedBytes

        // vv IntWritableTest-Deserialization
        // 反序列化
        IntWritable newWritable = new IntWritable();
        deserialize(newWritable, bytes);
        // 验证值不变
        assertThat(newWritable.get(), is(163));
        // ^^ IntWritableTest-Deserialization
    }

    /**
     * comparator - 测试 IntWritable 的 RawComparator
     *
     * <p>验证 RawComparator 能够直接比较字节数组,无需反序列化。</p>
     */
    @Test
    @SuppressWarnings("unchecked")
    public void comparator() throws IOException {
        // vv IntWritableTest-Comparator
        // 获取 IntWritable 的 RawComparator
        RawComparator<IntWritable> comparator =
            WritableComparator.get(IntWritable.class);
        // ^^ IntWritableTest-Comparator

        // vv IntWritableTest-ObjectComparison
        // 创建两个 IntWritable 对象
        IntWritable w1 = new IntWritable(163);
        IntWritable w2 = new IntWritable(67);
        // 比较对象: 163 > 67, 所以结果 > 0
        assertThat(comparator.compare(w1, w2), greaterThan(0));
        // ^^ IntWritableTest-ObjectComparison

        // vv IntWritableTest-BytesComparison
        // 序列化
        byte[] b1 = serialize(w1);
        byte[] b2 = serialize(w2);
        // 直接比较字节数组
        assertThat(comparator.compare(b1, 0, b1.length, b2, 0, b2.length),
            greaterThan(0));
        // ^^ IntWritableTest-BytesComparison
    }

    /**
     * test - 基本序列化测试
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        IntWritable src = new IntWritable(163);
        IntWritable dest = new IntWritable();
        assertThat(writeTo(src, dest), is("000000a3"));
        assertThat(dest.get(), is(src.get()));
    }

}
