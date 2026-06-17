package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 将整个文件作为一条记录读取的 InputFormat（旧 API）。
 *
 * <p>与默认的 TextInputFormat 按行读取不同，
 * WholeFileInputFormat 将整个文件的内容作为一条记录。
 *
 * <p>使用场景：
 * <ul>
 *   <li>将大量小文件合并为 SequenceFile</li>
 *   <li>处理不能按行分割的二进制文件</li>
 *   <li>处理需要保持完整性的文件</li>
 * </ul>
 *
 * <p>输出类型：
 * <ul>
 *   <li>键：NullWritable</li>
 *   <li>值：BytesWritable（文件内容）</li>
 * </ul>
 */
public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {

    /**
     * 判断文件是否可分割。
     *
     * @param fs       文件系统
     * @param filename 文件路径
     * @return 始终返回 false，表示文件不可分割
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    /**
     * 创建用于读取文件的 RecordReader。
     *
     * @param split    输入分片
     * @param job      JobConf 配置
     * @param reporter 进度报告器
     * @return WholeFileRecordReader 实例
     * @throws IOException 如果创建失败
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {

        return new WholeFileRecordReader((FileSplit) split, job);
    }
}
