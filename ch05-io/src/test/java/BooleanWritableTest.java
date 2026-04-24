import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

/**
 * BooleanWritableTest - BooleanWritable 的单元测试
 *
 * <p>测试 BooleanWritable 的序列化和反序列化功能。
 * BooleanWritable 是 Hadoop 中布尔值的 Writable 实现。</p>
 *
 * <p><b>测试内容:</b></p>
 * <ul>
 *   <li>序列化格式验证</li>
 *   <li>反序列化后值的一致性</li>
 * </ul>
 *
 * @see BooleanWritable
 * @see WritableTestBase
 */
public class BooleanWritableTest extends WritableTestBase {

    /**
     * test - 测试 BooleanWritable 的序列化和反序列化
     *
     * <p><b>测试步骤:</b></p>
     * <ol>
     *   <li>创建 BooleanWritable(true)</li>
     *   <li>序列化后检查字节表示</li>
     *   <li>反序列化到新的 BooleanWritable</li>
     *   <li>验证值相等</li>
     * </ol>
     *
     * <p><b>序列化格式:</b></p>
     * <ul>
     *   <li>true: 1 字节 (0x01)</li>
     *   <li>false: 1 字节 (0x00)</li>
     * </ul>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        // 创建 BooleanWritable,值为 true
        BooleanWritable src = new BooleanWritable(true);

        // 创建目标 BooleanWritable
        BooleanWritable dest = new BooleanWritable();

        // 序列化并验证字节表示
        // true 的序列化结果为 "01" (十六进制)
        assertThat(writeTo(src, dest), is("01"));

        // 验证反序列化后的值与原值相等
        assertThat(dest.get(), is(src.get()));
    }
}
