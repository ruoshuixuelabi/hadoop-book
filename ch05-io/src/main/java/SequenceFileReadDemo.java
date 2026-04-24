// cc SequenceFileReadDemo 演示如何从 SequenceFile 读取数据

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

// vv SequenceFileReadDemo
/**
 * SequenceFileReadDemo - 演示如何从 SequenceFile 读取键值对
 *
 * <p>此程序与 SequenceFileWriteDemo 配合使用,用于读取之前写入的 SequenceFile。
 * 展示了 SequenceFile 的核心读取 API。</p>
 *
 * <p><b>读取特性:</b></p>
 * <ul>
 *   <li>使用 ReflectionUtils 创建键值实例(动态类型发现)</li>
 *   <li>支持同步点(Sync)检测,用于 MapReduce 任务切分</li>
 *   <li>按顺序遍历所有记录</li>
 * </ul>
 *
 * <p><b>同步点(Sync):</b></p>
 * <ul>
 *   <li>SequenceFile 定期插入同步标记</li>
 *   <li>当 reader 移动到同步点时,syncSeen() 返回 true</li>
 *   <li>MapReduce 使用同步点进行任务切分</li>
 * </ul>
 *
 * @see SequenceFile
 * @see SequenceFile.Writer
 */
public class SequenceFileReadDemo {

    /**
     * 主方法 - 读取 SequenceFile 中的所有键值对
     *
     * <p><b>读取流程:</b></p>
     * <ol>
     *   <li>创建 SequenceFile.Reader</li>
     *   <li>使用 ReflectionUtils 动态创建键值对象</li>
     *   <li>循环调用 next() 读取每条记录</li>
     *   <li>检测同步点并标记</li>
     * </ol>
     *
     * <p><b>命令行参数:</b></p>
     * <ul>
     *   <li>args[0]: SequenceFile 的 HDFS 路径</li>
     * </ul>
     *
     * @param args 命令行参数
     * @throws IOException 如果发生 I/O 错误
     */
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        try {
            // 创建 Reader
            // Reader 会从 SequenceFile 头部的元数据中获取键值类型
            reader = new SequenceFile.Reader(fs, path, conf);

            // 使用 ReflectionUtils 动态创建键和值的实例
            // reader.getKeyClass() 和 reader.getValueClass() 返回 Class 对象
            // 这使得程序能够读取任意 Writable 类型的 SequenceFile
            Writable key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable)
                    ReflectionUtils.newInstance(reader.getValueClass(), conf);

            // 记录当前文件位置(读取开始位置)
            long position = reader.getPosition();

            // 循环读取所有记录
            // next() 方法: 如果返回 true,表示成功读取一条记录,键值被填充
            // next() 方法: 如果返回 false,表示已到达文件末尾
            while (reader.next(key, value)) {
                // 检测是否刚经过一个同步点
                // 同步点用于 MapReduce 任务切分,每个 MapTask 从一个同步点开始
                String syncSeen = reader.syncSeen() ? "*" : "";

                // 打印位置、同步标记、键和值
                // position 是本条记录开始的位置(同步点位置)
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);

                // 更新位置为下一条记录开始的位置
                position = reader.getPosition(); // beginning of next record
            }
        }
        finally {
            // 确保 Reader 被正确关闭
            IOUtils.closeStream(reader);
        }
    }
}
// ^^ SequenceFileReadDemo
