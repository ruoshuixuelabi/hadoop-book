// == BytesWritableTest
// == BytesWritableTest-Capacity
import static org.hamcrest.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.Test;

/**
 * BytesWritableTest - BytesWritable 的单元测试
 *
 * <p>测试 BytesWritable 的序列化和容量管理功能。
 * BytesWritable 是 Hadoop 中字节数组的 Writable 实现。</p>
 *
 * <p><b>测试内容:</b></p>
 * <ul>
 *   <li>基本序列化格式</li>
 *   <li>容量扩展行为</li>
 *   <li>getLength() vs getBytes().length 的区别</li>
 * </ul>
 *
 * @see BytesWritable
 * @see WritableTestBase
 */
public class BytesWritableTest extends WritableTestBase {

    /**
     * test - 测试 BytesWritable 的序列化和容量
     *
     * <p><b>测试步骤:</b></p>
     * <ol>
     *   <li>创建 BytesWritable 并设置字节数组</li>
     *   <li>验证序列化格式</li>
     *   <li>扩展容量但不改变实际长度</li>
     *   <li>验证 getLength() 和 getBytes().length 的区别</li>
     * </ol>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        // vv BytesWritableTest
        // 创建 BytesWritable,包含 2 个字节: 0x03, 0x05
        BytesWritable b = new BytesWritable(new byte[] { 3, 5 });

        // 序列化并验证格式
        // 序列化格式: [长度(4字节)][字节数据]
        // 长度 = 2, 所以字节为 00 00 00 02 03 05
        byte[] bytes = serialize(b);
        assertThat(StringUtils.byteToHexString(bytes), is("000000020305"));
        // ^^ BytesWritableTest

        // vv BytesWritableTest-Capacity
        // 扩展容量到 11 字节
        // 注意: 这不会改变 getLength(),只改变内部数组的长度
        b.setCapacity(11);

        // getLength() 返回实际使用的字节数
        assertThat(b.getLength(), is(2));

        // getBytes().length 返回内部数组的长度(容量)
        assertThat(b.getBytes().length, is(11));
        // ^^ BytesWritableTest-Capacity
    }
}
