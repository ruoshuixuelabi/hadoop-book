// cc SequenceFileWriteDemo 演示如何向 SequenceFile 写入数据

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

// vv SequenceFileWriteDemo
/**
 * SequenceFileWriteDemo - 演示如何向 SequenceFile 写入键值对
 *
 * <p>SequenceFile 是 Hadoop 用于存储二进制键值对的平面文件格式。
 * 它支持三种压缩类型:无压缩、记录压缩、块压缩。</p>
 *
 * <p><b>SequenceFile 特性:</b></p>
 * <ul>
 *   <li>持久化存储任意 Writable 类型的数据</li>
 *   <li>支持顺序访问和同步点(用于 MapReduce 任务切分)</li>
 *   <li>可配置压缩模式以节省存储空间</li>
 * </ul>
 *
 * <p><b>使用示例:</b></p>
 * <pre>{@code
 * hadoop jar hadoop-examples.jar SequenceFileWriteDemo /path/to/file
 * }</pre>
 *
 * @see SequenceFile
 * @see SequenceFile.Reader
 * @see org.apache.hadoop.io.SequenceFile.Writer
 */
public class SequenceFileWriteDemo {

    /**
     * 测试数据 - 童谣 "One, Two, Buckle My Shoe"
     */
    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    /**
     * 主方法 - 创建并写入 SequenceFile
     *
     * <p>此程序演示 SequenceFile 的基本写入操作:</p>
     * <ol>
     *   <li>创建 FileSystem 和 Path 对象</li>
     *   <li>创建 SequenceFile.Writer</li>
     *   <li>循环写入 100 条键值对记录</li>
     *   <li>关闭 Writer</li>
     * </ol>
     *
     * <p><b>命令行参数:</b></p>
     * <ul>
     *   <li>args[0]: SequenceFile 的 HDFS 路径 (如 /path/to/file)</li>
     * </ul>
     *
     * @param args 命令行参数
     * @throws IOException 如果发生 I/O 错误
     */
    public static void main(String[] args) throws IOException {
        // 获取要创建的 SequenceFile 的 URI 路径
        String uri = args[0];

        // 创建 Configuration 对象,Hadoop 组件共享配置
        Configuration conf = new Configuration();

        // 获取对应的 FileSystem 实例(可能是 HDFS、本地文件系统等)
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        // 创建 Path 对象表示 SequenceFile 路径
        Path path = new Path(uri);

        // 创建键值对象(必须是 Writable 类型)
        IntWritable key = new IntWritable();
        Text value = new Text();

        // SequenceFile.Writer 实例,用于写入键值对
        SequenceFile.Writer writer = null;
        try {
            // 创建 Writer
            // 参数: FileSystem, Configuration, Path, 键类, 值类
            writer = SequenceFile.createWriter(fs, conf, path,
                    key.getClass(), value.getClass());

            // 循环写入 100 条记录,键为 100-i,值为 DATA 数组中的元素
            for (int i = 0; i < 100; i++) {
                // 设置键值
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);

                // 打印当前文件位置(偏移量)和键值对内容
                // writer.getLength() 返回当前写入位置(同步点)
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);

                // append 方法将键值对追加到 SequenceFile
                writer.append(key, value);
            }
        }
        finally {
            // 确保 Writer 被正确关闭,释放资源
            IOUtils.closeStream(writer);
        }
    }
}
// ^^ SequenceFileWriteDemo
