// cc WholeFileInputFormat 一种将整个文件作为一条记录的 InputFormat

import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

//vv WholeFileInputFormat
/**
 * 将整个文件作为一条记录读取的 InputFormat。
 *
 * <p>默认的 TextInputFormat 按行读取文件，每一行是一条独立的记录。
 * WholeFileInputFormat 则将整个文件的内容作为一条记录，
 * 文件名作为键（使用 NullWritable），文件内容作为值（BytesWritable）。
 *
 * <p>使用场景：
 * <ul>
 *   <li>将大量小文件合并为 SequenceFile（参见 {@link SmallFilesToSequenceFileConverter}）</li>
 *   <li>处理不能按行分割的二进制文件</li>
 *   <li>处理需要整体上下文的数据（如完整的图像、音频文件）</li>
 *   <li>处理压缩文件（某些压缩格式不支持分片）</li>
 * </ul>
 *
 * <p>注意：由于整个文件作为一条记录，该 InputFormat 设置文件为不可分割
 *（参见 {@link #isSplittable(JobContext, Path)}）。
 * 这意味着一个大文件只会被一个 Map 任务处理。
 *
 * <p>输出类型：
 * <ul>
 *   <li>键：NullWritable（不使用）</li>
 *   <li>值：BytesWritable（整个文件内容）</li>
 * </ul>
 *
 * @see WholeFileRecordReader
 * @see SmallFilesToSequenceFileConverter
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    /**
     * 判断文件是否可分割。
     *
     * <p>本方法返回 false，表示整个文件作为一个单独的输入分片。
     * 这样确保每个文件只由一个 Map 任务处理，文件内容不会被分割。
     *
     * <p>返回 false 的影响：
     * <ul>
     *   <li>文件不会被根据块大小进行分割</li>
     *   <li>一个大文件只会有一个 Map 任务处理它</li>
     *   <li>可以保证文件的完整性和顺序处理</li>
     * </ul>
     *
     * @param context 作业上下文
     * @param file    文件路径
     * @return 始终返回 false，表示文件不可分割
     */
    @Override
    protected boolean isSplittable(JobContext context, Path file) {
        return false;
    }

    /**
     * 创建用于读取文件的 RecordReader。
     *
     * <p>返回一个自定义的 WholeFileRecordReader 实例，
     * 负责将整个文件读取为一条记录。
     *
     * @param split   输入分片
     * @param context Task 尝试上下文
     * @return 读取文件的 RecordReader
     * @throws IOException 如果创建 RecordReader 时发生 I/O 错误
     * @throws InterruptedException 如果创建过程中被中断
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 创建自定义的 WholeFileRecordReader
        WholeFileRecordReader reader = new WholeFileRecordReader();
        // 初始化 RecordReader，传入分片和上下文
        reader.initialize(split, context);
        return reader;
    }
}
//^^ WholeFileInputFormat
