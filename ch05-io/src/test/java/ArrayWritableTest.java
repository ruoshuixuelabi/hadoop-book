// == ArrayWritableTest
// cc ArrayWritableTest 测试 ArrayWritable 和 TextArrayWritable 的功能

import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.jupiter.api.Test;

/**
 * ArrayWritableTest - ArrayWritable 相关功能的单元测试
 *
 * <p>测试内容:</p>
 * <ul>
 *   <li>ArrayWritable 的基本创建和设置</li>
 *   <li>TextArrayWritable 的类型安全性</li>
 *   <li>使用 WritableUtils.cloneInto 进行深度克隆</li>
 *   <li>toArray() 方法返回正确的数组类型</li>
 * </ul>
 *
 * <p><b>测试策略:</b></p>
 * <ol>
 *   <li>创建 ArrayWritable 并设置 Text 数组</li>
 *   <li>使用 WritableUtils.cloneInto 克隆到 TextArrayWritable</li>
 *   <li>验证克隆后的数组长度和元素</li>
 *   <li>验证 toArray() 返回正确的 Text[] 类型</li>
 * </ol>
 *
 * @see ArrayWritable
 * @see TextArrayWritable
 * @see WritableUtils
 * @see WritableTestBase
 */
public class ArrayWritableTest extends WritableTestBase {

    /**
     * test - 测试 ArrayWritable 和 TextArrayWritable 的基本功能
     *
     * <p><b>测试步骤:</b></p>
     * <ol>
     *   <li>创建 ArrayWritable(Text.class) 并设置两个元素</li>
     *   <li>创建 TextArrayWritable 作为目标</li>
     *   <li>使用 WritableUtils.cloneInto 克隆数据</li>
     *   <li>验证克隆后的数组长度和元素值</li>
     *   <li>验证 toArray() 返回正确的类型</li>
     * </ol>
     *
     * <p><b>验证点:</b></p>
     * <ul>
     *   <li>克隆后数组长度为 2</li>
     *   <li>克隆后元素正确: "cat", "dog"</li>
     *   <li>toArray() 返回 Text[] 类型</li>
     * </ul>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void test() throws IOException {
        // vv ArrayWritableTest
        // 创建 ArrayWritable,指定元素类型为 Text
        // 注意: ArrayWritable 的元素类型必须在构造时指定
        ArrayWritable writable = new ArrayWritable(Text.class);
        // ^^ ArrayWritableTest

        // 设置 Text 数组作为元素
        writable.set(new Text[] { new Text("cat"), new Text("dog") });

        // 创建 TextArrayWritable 作为克隆目标
        // TextArrayWritable 继承 ArrayWritable,元素类型固定为 Text
        TextArrayWritable dest = new TextArrayWritable();

        // 使用 WritableUtils.cloneInto 进行深度克隆
        // 这会序列化和反序列化整个数组,实现真正的复制而非引用
        WritableUtils.cloneInto(dest, writable);

        // 验证克隆后的数组长度
        assertThat(dest.get().length, is(2));

        // TODO: fix cast, also use single assert
        // 验证第一个元素
        assertThat((Text) dest.get()[0], is(new Text("cat")));

        // 验证第二个元素
        assertThat((Text) dest.get()[1], is(new Text("dog")));

        // 验证 toArray() 返回正确的类型
        // toArray() 会根据元素类型创建正确类型的数组
        Text[] copy = (Text[]) dest.toArray();

        // 验证数组内容与原始数据一致
        assertThat(copy[0], is(new Text("cat")));
        assertThat(copy[1], is(new Text("dog")));
    }
}
