import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.jupiter.api.Test;

/**
 * GenericWritableTest - GenericWritable 的单元测试
 *
 * <p>测试 GenericWritable 的动态类型能力和克隆功能。
 * GenericWritable 允许在运行时存储和恢复多种 Writable 类型。</p>
 *
 * <p><b>测试内容:</b></p>
 * <ul>
 *   <li>存储 Text 类型</li>
 *   <li>存储 BytesWritable 类型</li>
 *   <li>使用 WritableUtils.cloneInto 进行深度克隆</li>
 * </ul>
 *
 * @see GenericWritable
 * @see BinaryOrTextWritable
 * @see WritableUtils
 */
public class GenericWritableTest extends WritableTestBase {

    /**
     * test - 测试 GenericWritable 的类型存储和克隆
     *
     * <p><b>测试步骤:</b></p>
     * <ol>
     *   <li>创建 BinaryOrTextWritable 并设置为 Text</li>
     *   <li>克隆到目标对象</li>
     *   <li>验证 Text 类型的正确恢复</li>
     *   <li>重复 BytesWritable 类型</li>
     * </ol>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        // 创建 BinaryOrTextWritable 并设置为 Text 类型
        BinaryOrTextWritable src = new BinaryOrTextWritable();
        src.set(new Text("text"));

        // 创建目标 BinaryOrTextWritable
        BinaryOrTextWritable dest = new BinaryOrTextWritable();

        // 使用 WritableUtils.cloneInto 进行深度克隆
        // 这会序列化 src 并反序列化到 dest
        WritableUtils.cloneInto(dest, src);

        // 验证 Text 类型被正确恢复
        assertThat((Text) dest.get(), is(new Text("text")));

        // 切换到 BytesWritable 类型
        src.set(new BytesWritable(new byte[] {3, 5}));

        // 再次克隆
        WritableUtils.cloneInto(dest, src);

        // 验证 BytesWritable 类型被正确恢复
        // 注意: 这里用 getLength() 验证而不是完整的字节比较
        assertThat(((BytesWritable) dest.get()).getLength(), is(2)); // TODO proper assert
    }
}
