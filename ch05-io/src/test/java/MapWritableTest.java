// == MapWritableTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

/**
 * MapWritableTest - MapWritable 的单元测试
 *
 * <p>测试 MapWritable 的键值存储和类型管理功能。
 * MapWritable 是 Hadoop 中 Map 类型的 Writable 实现。</p>
 *
 * <p><b>测试内容:</b></p>
 * <ul>
 *   <li>存储不同 Writable 类型的键值对</li>
 *   <li>使用 WritableUtils.cloneInto 进行克隆</li>
 *   <li>containsKey() 方法验证</li>
 * </ul>
 *
 * @see MapWritable
 * @see WritableTestBase
 */
public class MapWritableTest extends WritableTestBase {

    /**
     * mapWritable - 测试基本的 MapWritable 功能
     *
     * <p><b>测试步骤:</b></p>
     * <ol>
     *   <li>创建 MapWritable</li>
     *   <li>存入 IntWritable->Text 和 VIntWritable->LongWritable</li>
     *   <li>克隆到另一个 MapWritable</li>
     *   <li>验证值正确恢复</li>
     * </ol>
     *
     * <p><b>键类型混合:</b></p>
     * <ul>
     *   <li>IntWritable(1) 和 VIntWritable(2) 作为键</li>
     *   <li>注意: MapWritable 将不同包装类型视为不同类型</li>
     * </ul>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void mapWritable() throws IOException {
        // vv MapWritableTest
        // 创建源 MapWritable
        MapWritable src = new MapWritable();

        // 存入键值对
        // 键: IntWritable(1), 值: Text("cat")
        src.put(new IntWritable(1), new Text("cat"));

        // 键: VIntWritable(2), 值: LongWritable(163)
        // 注意: VIntWritable 和 IntWritable 是不同类型
        src.put(new VIntWritable(2), new LongWritable(163));

        // 创建目标 MapWritable 并克隆
        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest, src);

        // 验证克隆后的值
        assertThat((Text) dest.get(new IntWritable(1)), is(new Text("cat")));
        assertThat((LongWritable) dest.get(new VIntWritable(2)),
            is(new LongWritable(163)));
        // ^^ MapWritableTest
    }

    /**
     * setWritableEmulation - 测试 NullWritable 存储
     *
     * <p>NullWritable 可用于表示空值或占位符。</p>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @Test
    public void setWritableEmulation() throws IOException {
        // 创建 MapWritable
        MapWritable src = new MapWritable();

        // 存入 NullWritable 作为值
        src.put(new IntWritable(1), NullWritable.get());
        src.put(new IntWritable(2), NullWritable.get());

        // 克隆
        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest, src);

        // 验证 containsKey
        assertThat(dest.containsKey(new IntWritable(1)), is(true));
    }
}
