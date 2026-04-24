// cc WholeFileRecordReader WholeFileInputFormat 使用的 RecordReader，将整个文件作为一条记录读取

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//vv WholeFileRecordReader
/**
 * 将整个文件内容作为一条记录读取的 RecordReader。
 *
 * <p>与默认的 LineRecordReader（按行读取）不同，
 * WholeFileRecordReader 一次性将整个文件的内容读入内存，
 * 作为一条记录输出。
 *
 * <p>工作流程：
 * <ol>
 *   <li>initialize: 初始化，获取 FileSplit 和 Configuration</li>
 *   <li>nextKeyValue: 读取整个文件内容，设置键值对</li>
 *   <li>getCurrentKey/getCurrentValue: 返回当前的键值对</li>
 *   <li>getProgress: 返回读取进度</li>
 *   <li>close: 关闭资源</li>
 * </ol>
 *
 * <p>输出键值对：
 * <ul>
 *   <li>键：NullWritable.get()（固定值，不表示任何意义）</li>
 *   <li>值：BytesWritable（文件内容的字节数组）</li>
 * </ul>
 *
 * <p>注意：
 * <ul>
 *   <li>由于需要将整个文件读入内存，不适合处理过大的文件</li>
 *   <li>只会被调用一次 nextKeyValue（因为只有一个文件）</li>
 *   <li>processed 标志用于确保只读取一次</li>
 * </ul>
 *
 * @see WholeFileInputFormat
 */
class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    // 当前处理的文件分片
    private FileSplit fileSplit;
    // Hadoop 配置
    private Configuration conf;
    // 存储文件内容的 BytesWritable 对象（复用以减少GC）
    private BytesWritable value = new BytesWritable();
    // 是否已处理完当前文件（防止重复读取）
    private boolean processed = false;

    /**
     * 初始化 RecordReader。
     *
     * <p>在 Task 开始时调用，用于设置分片信息和配置。
     *
     * @param split   输入分片，类型为 FileSplit
     * @param context Task 尝试上下文
     * @throws IOException 如果初始化失败
     * @throws InterruptedException 如果被中断
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 将通用 InputSplit 转换为 FileSplit 以获取文件路径和长度
        this.fileSplit = (FileSplit) split;
        // 获取配置以用于后续打开文件系统
        this.conf = context.getConfiguration();
    }

    /**
     * 读取下一条记录。
     *
     * <p>本方法一次性读取整个文件的内容。
     * 由于整个文件是一条记录，只会返回一次 true。
     *
     * @return 如果成功读取记录返回 true，否则返回 false
     * @throws IOException 如果读取过程中发生 I/O 错误
     * @throws InterruptedException 如果读取过程中被中断
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 如果还未处理过，则读取整个文件
        if (!processed) {
            // 创建足够大的字节数组存储文件内容
            byte[] contents = new byte[(int) fileSplit.getLength()];
            // 获取文件路径
            Path file = fileSplit.getPath();
            // 获取对应的文件系统
            FileSystem fs = file.getFileSystem(conf);
            // 用于读取文件的输入流
            FSDataInputStream in = null;
            try {
                // 打开文件
                in = fs.open(file);
                // 将整个文件内容读入字节数组
                IOUtils.readFully(in, contents, 0, contents.length);
                // 设置 BytesWritable 的值（从 0 到实际读取长度）
                value.set(contents, 0, contents.length);
            } finally {
                // 确保输入流被关闭
                IOUtils.closeStream(in);
            }
            // 标记为已处理
            processed = true;
            // 返回 true 表示成功读取了一条记录
            return true;
        }
        // 已经处理过，直接返回 false
        return false;
    }

    /**
     * 获取当前记录的键。
     *
     * <p>由于 WholeFileInputFormat 的键是 NullWritable，
     * 始终返回 NullWritable.get()。
     *
     * @return NullWritable
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取当前记录的值。
     *
     * <p>返回包含整个文件内容的 BytesWritable。
     *
     * @return BytesWritable，文件内容
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 获取读取进度。
     *
     * <p>由于文件只有两种状态：未处理（0%）和已处理（100%），
     * 返回 0.0f 或 1.0f。
     *
     * @return 进度，0.0f 表示未开始，1.0f 表示已完成
     */
    @Override
    public float getProgress() throws IOException {
        // 如果已处理返回 1.0f（100%），否则返回 0.0f（0%）
        return processed ? 1.0f : 0.0f;
    }

    /**
     * 关闭 RecordReader，释放资源。
     *
     * <p>本实现不需要关闭任何资源，因为文件已在 finally 块中关闭。
     */
    @Override
    public void close() throws IOException {
        // 无需额外资源清理
        // do nothing
    }
}
//^^ WholeFileRecordReader
