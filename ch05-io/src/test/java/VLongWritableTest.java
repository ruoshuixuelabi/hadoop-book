import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

/**
 * VLongWritableTest - VLongWritable 的单元测试
 *
 * <p>测试 VLongWritable 的变长 long 序列化格式。
 * VLongWritable 使用变长编码,小数字节更少。</p>
 *
 * <p><b>与 VIntWritable 的区别:</b></p>
 * <ul>
 *   <li>VIntWritable: 变长 int (1-5 字节)</li>
 *   <li>VLongWritable: 变长 long (1-9 字节)</li>
 * </ul>
 *
 * @see VLongWritable
 * @see VIntWritable
 * @see WritableTestBase
 */
public class VLongWritableTest extends WritableTestBase {

    /**
     * test - 测试 VLongWritable 序列化
     *
     * <p><b>编码示例:</b></p>
     * <ul>
     *   <li>1: 01 (1 字节)</li>
     *   <li>127: 7f (1 字节)</li>
     *   <li>128: 8f 80 (2 字节)</li>
     *   <li>163: 8f a3 (2 字节)</li>
     *   <li>MAX: 88 7fffffffffffffff (9 字节)</li>
     *   <li>MIN: 80 7fffffffffffffff (9 字节)</li>
     * </ul>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        assertThat(serializeToString(new VLongWritable(1)), is("01")); // 1 byte
        assertThat(serializeToString(new VLongWritable(127)), is("7f")); // 1 byte
        assertThat(serializeToString(new VLongWritable(128)), is("8f80")); // 2 byte
        assertThat(serializeToString(new VLongWritable(163)), is("8fa3")); // 2 byte
        assertThat(serializeToString(new VLongWritable(Long.MAX_VALUE)), is("887fffffffffffffff")); // 9 byte
        assertThat(serializeToString(new VLongWritable(Long.MIN_VALUE)), is("807fffffffffffffff")); // 9 byte
    }
}
