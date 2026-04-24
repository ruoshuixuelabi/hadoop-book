import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

/**
 * NullWritableTest - NullWritable 的单元测试
 *
 * <p>测试 NullWritable 的序列化格式。
 * NullWritable 是 Hadoop 中表示"无值"的 Writable 实现。</p>
 *
 * <p><b>NullWritable 特性:</b></p>
 * <ul>
 *   <li>序列化后长度为 0</li>
 *   <li>单例模式: 使用 get() 获取实例</li>
 *   <li>常用于不需要值的场景 (如 Map-side join)</li>
 * </ul>
 *
 * @see NullWritable
 * @see WritableTestBase
 */
public class NullWritableTest extends WritableTestBase {

    /**
     * test - 测试 NullWritable 的序列化
     *
     * <p><b>验证点:</b></p>
     * <ul>
     *   <li>NullWritable.get() 返回单例</li>
     *   <li>序列化后字节数组长度为 0</li>
     * </ul>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        // 获取 NullWritable 单例
        NullWritable writable = NullWritable.get();

        // 序列化后长度为 0
        assertThat(serialize(writable).length, is(0));
    }
}
