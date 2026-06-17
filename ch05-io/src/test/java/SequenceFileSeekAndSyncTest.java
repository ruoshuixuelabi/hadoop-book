// == SequenceFileSeekAndSyncTest
// == SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
// == SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
import static org.hamcrest.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

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
 * SequenceFileSeekAndSyncTest - SequenceFile 定位和同步点的单元测试
 *
 * <p>测试 SequenceFile.Reader 的 seek() 和 sync() 方法,
 * 验证在记录边界和非记录边界的定位行为。</p>
 *
 * <p><b>同步点概念:</b></p>
 * <ul>
 *   <li>SequenceFile 定期插入同步标记</li>
 *   <li>sync() 跳转到下一个同步点</li>
 *   <li>用于 MapReduce 任务切分</li>
 * </ul>
 *
 * @see SequenceFile
 * @see SequenceFile.Reader
 * @see SequenceFileWriteDemo
 */
public class SequenceFileSeekAndSyncTest {

    /** 测试用的 SequenceFile URI */
    private static final String SF_URI = "test.numbers.seq";

    /** FileSystem 实例 */
    private FileSystem fs;
    /** SequenceFile Reader */
    private SequenceFile.Reader reader;
    /** 键对象 */
    private Writable key;
    /** 值对象 */
    private Writable value;

    /**
     * setUp - 测试前准备
     *
     * <p>创建测试用的 SequenceFile。</p>
     *
     * @throws IOException 如果发生 I/O 错误
     */
    @BeforeEach
    public void setUp() throws IOException {
        // 先创建 SequenceFile (使用 SequenceFileWriteDemo)
        SequenceFileWriteDemo.main(new String[] { SF_URI });

        // 获取 FileSystem
        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(SF_URI), conf);
        Path path = new Path(SF_URI);

        // 创建 SequenceFile.Reader
        reader = new SequenceFile.Reader(fs, path, conf);

        // 使用 ReflectionUtils 动态创建键值对象
        key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
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
        fs.delete(new Path(SF_URI), true);
    }

    /**
     * seekToRecordBoundary - 测试 seek 到记录边界
     *
     * <p>seek() 只能在记录边界使用,否则会抛出异常。</p>
     */
    @Test
    public void seekToRecordBoundary() throws IOException {
        // vv SequenceFileSeekAndSyncTest
        // seek 到字节位置 359 (正好是某个记录的起始)
        reader.seek(359);
        // next() 应该成功
        assertThat(reader.next(key, value), is(true));
        // 验证键值正确
        assertThat(((IntWritable) key).get(), is(95));
        // ^^ SequenceFileSeekAndSyncTest
    }

    /**
     * seekToNonRecordBoundary - 测试 seek 到非记录边界
     *
     * <p>seek() 到非记录边界会抛出 IOException。</p>
     */
    @Test(expected=IOException.class)
    public void seekToNonRecordBoundary() throws IOException {
        // vv SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
        // seek 到字节位置 360 (不是记录边界)
        reader.seek(360);
        // next() 会抛出 IOException
        reader.next(key, value); // fails with IOException
        // ^^ SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
    }

    /**
     * syncFromNonRecordBoundary - 测试从非记录边界同步
     *
     * <p>sync() 忽略当前位置,跳转到下一个同步点。</p>
     */
    @Test
    public void syncFromNonRecordBoundary() throws IOException {
        // vv SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
        // sync 到字节位置 360 (任意位置)
        // sync() 会跳转到下一个同步点
        reader.sync(360);

        // 验证位置跳转到同步点
        assertThat(reader.getPosition(), is(2021L));

        // next() 应该成功
        assertThat(reader.next(key, value), is(true));

        // 验证键值
        assertThat(((IntWritable) key).get(), is(59));
        // ^^ SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
    }

    /**
     * syncAfterLastSyncPoint - 测试在最后一个同步点之后同步
     *
     * <p>在最后同步点之后 sync(),会标记文件末尾。</p>
     */
    @Test
    public void syncAfterLastSyncPoint() throws IOException {
        // sync 到文件末尾之后的位置
        reader.sync(4557);

        // 位置应该跳转到文件末尾
        assertThat(reader.getPosition(), is(4788L));

        // next() 应该返回 false (没有更多记录)
        assertThat(reader.next(key, value), is(false));
    }

}
