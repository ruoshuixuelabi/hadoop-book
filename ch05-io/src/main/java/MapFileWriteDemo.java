// cc MapFileWriteDemo 演示如何向 MapFile 写入数据

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

/**
 * MapFileWriteDemo - 演示如何创建和写入 MapFile
 *
 * <p>MapFile 是 SequenceFile 的索引变体,它在 SequenceFile 基础上
 * 额外维护了一个索引文件(index),用于快速定位键值对。</p>
 *
 * <p><b>MapFile vs SequenceFile:</b></p>
 * <ul>
 *   <li><b>MapFile</b>: 键必须严格递增排序,提供快速随机访问</li>
 *   <li><b>SequenceFile</b>: 键值对按顺序存储,不支持随机访问</li>
 * </ul>
 *
 * <p><b>文件结构:</b></p>
 * <ul>
 *   <li>data 文件: 存储实际的键值对序列</li>
 *   <li>index 文件: 存储索引(每 N 条记录一个索引项),默认 N=128</li>
 * </ul>
 *
 * <p><b>使用场景:</b></p>
 * <ul>
 *   <li>需要按键快速查找的场景</li>
 *   <li>作为 MapReduce 的输出格式(支持随机访问)</li>
 * </ul>
 *
 * @see MapFile
 * @see SequenceFile
 */
public class MapFileWriteDemo {

    /**
     * 测试数据 - 童谣片段
     */
    private static final String[] DATA = {
        "One, two, buckle my shoe",
        "Three, four, shut the door",
        "Five, six, pick up sticks",
        "Seven, eight, lay them straight",
        "Nine, ten, a big fat hen"
    };

    /**
     * 主方法 - 创建并写入 MapFile
     *
     * <p><b>写入流程:</b></p>
     * <ol>
     *   <li>创建 MapFile.Writer</li>
     *   <li>确保键值按递增顺序写入</li>
     *   <li>关闭 Writer(同时关闭 index 和 data 文件)</li>
     * </ol>
     *
     * <p><b>重要约束:</b></p>
     * <ul>
     *   <li>键必须严格递增,否则抛出 IOException</li>
     *   <li>索引文件会自动维护(每 128 条记录一个索引项)</li>
     * </ul>
     *
     * <p><b>命令行参数:</b></p>
     * <ul>
     *   <li>args[0]: MapFile 的 HDFS 目录路径</li>
     * </ul>
     *
     * @param args 命令行参数
     * @throws IOException 如果发生 I/O 错误或键值顺序不正确
     */
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        // 创建键值对象
        IntWritable key = new IntWritable();
        Text value = new Text();

        MapFile.Writer writer = null;
        try {
            // 创建 MapFile.Writer
            // 注意: MapFile 使用目录作为路径,data 和 index 文件在该目录下
            // 参数: Configuration, FileSystem, URI路径, 键类, 值类
            writer = new MapFile.Writer(conf, fs, uri,
                key.getClass(), value.getClass());

            // 写入 1024 条记录,键为 1-1024,值为 DATA 数组中的元素
            // 键必须严格递增: 1, 2, 3, ..., 1024
            for (int i = 0; i < 1024; i++) {
                key.set(i + 1);
                value.set(DATA[i % DATA.length]);

                // append 方法同时写入 data 文件和 index 文件
                writer.append(key, value);
            }
        } finally {
            // 关闭 Writer,同时刷新索引并关闭所有文件
            IOUtils.closeStream(writer);
        }
    }
}
