import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * MapFileSeekTest - MapFile 随机访问的单元测试
 *
 * <p>测试 MapFile.Reader 的 seek() 和 get() 方法,
 * 验证 MapFile 的快速随机访问能力。</p>
 *
 * <p><b>测试内容:</b></p>
 * <ul>
 *   <li>使用 get() 方法按键查找值</li>
 *   <li>使用 seek() 方法定位然后调用 next()</li>
 *   <li>验证查找的正确性</li>
 * </ul>
 *
 * @see MapFile
 * @see MapFile.Reader
 * @see MapFileWriteDemo
 */
public class MapFileSeekTest {

    /** 测试用的 MapFile URI */
    private static final String MAP_URI = "test.numbers.map";

    /** FileSystem 实例 */
    private FileSystem fs;
    /** MapFile Reader */
    private MapFile.Reader reader;
    /** 键对象 */
    private WritableComparable<?> key;
    /** 值对象 */
    private Writable value;

    /**
     * setUp - 测试前准备
     *
     * <p>创建测试用的 MapFile。</p>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @BeforeEach
    public void setUp() throws IOException {
        // 先创建 MapFile (使用 MapFileWriteDemo)
        MapFileWriteDemo.main(new String[] { MAP_URI });

        // 获取 FileSystem
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(MAP_URI), conf);

        // 创建 MapFile.Reader
        reader = new MapFile.Reader(fs, MAP_URI, conf);

        // 使用 ReflectionUtils 动态创建键值对象
        // 从 Reader 获取类型信息
        key = (WritableComparable<?>)
          ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
    }

    /**
     * tearDown - 测试后清理
     *
     * @throws IOException 如果删除失败
     */
    @AfterEach
    public void tearDown() throws IOException {
        // 删除测试文件
        fs.delete(new Path(MAP_URI), true);
    }

    /**
     * get - 测试 MapFile.Reader.get() 方法
     *
     * <p>get() 方法可以直接按键查找,无需遍历。</p>
     *
     * <p><b>测试数据:</b></p>
     * <ul>
     *   <li>键 496 对应值 "One, two, buckle my shoe"</li>
     * </ul>
     *
     * @throws Exception 如果查找失败
     */
    @Test
    public void get() throws Exception {
        // 创建 Text 对象接收结果
        Text value = new Text();

        // 直接按键查找
        // MapFile 会使用索引快速定位
        reader.get(new IntWritable(496), value);

        // 验证值正确
        assertThat(value.toString(), is("One, two, buckle my shoe"));
    }

    /**
     * seek - 测试 seek() 和 next() 的组合使用
     *
     * <p>seek() 定位到指定键,next() 从该位置读取下一条记录。</p>
     *
     * <p><b>测试逻辑:</b></p>
     * <ul>
     *   <li>seek(496) 定位到键 496</li>
     *   <li>next() 读取下一条,即键 497</li>
     *   <li>验证键为 497,值为 "Three, four, shut the door"</li>
     * </ul>
     *
     * @throws Exception 如果查找失败
     */
    @Test
    public void seek() throws Exception {
        // seek 到键 496
        assertThat(reader.seek(new IntWritable(496)), is(true));

        // next() 读取下一条记录
        assertThat(reader.next(key, value), is(true));

        // 验证读取的是键 497
        assertThat(((IntWritable) key).get(), is(497));

        // 验证值
        assertThat(((Text) value).toString(), is("Three, four, shut the door"));
    }

}
