// == VIntWritableTest
import static org.hamcrest.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.Test;

/**
 * VIntWritableTest - VIntWritable 的单元测试
 *
 * <p>测试 VIntWritable 的变长整数序列化格式。
 * VIntWritable 使用变长编码,小数字节更少。</p>
 *
 * <p><b>变长编码规则:</b></p>
 * <ul>
 *   <li>如果值在 [-112, 127],用 1 字节编码</li>
 *   <li>其他值用 2-5 字节编码</li>
 *   <li>正数和负数使用不同的编码方式</li>
 * </ul>
 *
 * @see VIntWritable
 * @see VLongWritable
 * @see WritableTestBase
 */
public class VIntWritableTest extends WritableTestBase {

    /**
     * test - 测试 VIntWritable 序列化格式
     */
    @Test
    public void test() throws IOException {
        // vv VIntWritableTest
        // 163 需要 2 字节: 8f a3
        byte[] data = serialize(new VIntWritable(163));
        assertThat(StringUtils.byteToHexString(data), is("8fa3"));
        // ^^ VIntWritableTest
    }

    /**
     * testSizes - 测试不同值的变长编码大小
     *
     * <p><b>编码示例:</b></p>
     * <ul>
     *   <li>1: 01 (1 字节,直接在 [-112, 127] 范围)</li>
     *   <li>-112: 90 (1 字节)</li>
     *   <li>127: 7f (1 字节)</li>
     *   <li>128: 8f 80 (2 字节,超出正数范围)</li>
     *   <li>163: 8f a3 (2 字节)</li>
     *   <li>MAX: 8c 7fffffff (5 字节)</li>
     *   <li>MIN: 84 7fffffff (5 字节)</li>
     * </ul>
     */
    @Test
    public void testSizes() throws IOException {
        assertThat(serializeToString(new VIntWritable(1)), is("01")); // 1 byte
        assertThat(serializeToString(new VIntWritable(-112)), is("90")); // 1 byte
        assertThat(serializeToString(new VIntWritable(127)), is("7f")); // 1 byte
        assertThat(serializeToString(new VIntWritable(128)), is("8f80")); // 2 byte
        assertThat(serializeToString(new VIntWritable(163)), is("8fa3")); // 2 byte
        assertThat(serializeToString(new VIntWritable(Integer.MAX_VALUE)),
            is("8c7fffffff")); // 5 byte
        assertThat(serializeToString(new VIntWritable(Integer.MIN_VALUE)),
            is("847fffffff")); // 5 byte
    }
}
