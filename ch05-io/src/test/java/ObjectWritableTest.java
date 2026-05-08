import static org.hamcrest.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.jupiter.api.Test;

/**
 * ObjectWritableTest - ObjectWritable 的单元测试
 *
 * <p>测试 ObjectWritable 的动态类型包装功能。
 * ObjectWritable 可以包装任意 Java 对象或 Writable 对象。</p>
 *
 * <p><b>ObjectWritable 用途:</b></p>
 * <ul>
 *   <li>动态类型字段</li>
 *   <li>通用键值类型</li>
 *   <li>处理 Java 原始类型和 Writable 类型混合</li>
 * </ul>
 *
 * @see ObjectWritable
 * @see WritableTestBase
 */
public class ObjectWritableTest extends WritableTestBase {

    /**
     * test - 测试 ObjectWritable 包装原始类型
     *
     * <p><b>测试步骤:</b></p>
     * <ol>
     *   <li>创建 ObjectWritable 包装 Integer.TYPE (int)</li>
     *   <li>克隆到目标 ObjectWritable</li>
     *   <li>验证值正确恢复</li>
     * </ol>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        // 创建 ObjectWritable,包装 Java int 类型
        // 第一个参数是类型,第二个参数是值
        ObjectWritable src = new ObjectWritable(Integer.TYPE, 163);

        // 创建目标
        ObjectWritable dest = new ObjectWritable();

        // 克隆
        WritableUtils.cloneInto(dest, src);

        // 验证值
        assertThat((Integer) dest.get(), is(163));
    }
}
