package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * 将整个文件内容作为一条记录读取的 RecordReader（旧 API）。
 *
 * <p>一次性将整个文件读入内存，作为一条记录输出。
 */
class WholeFileRecordReader implements RecordReader<NullWritable, BytesWritable> {

    // 当前处理的文件分片
    private FileSplit fileSplit;
    // Hadoop 配置
    private Configuration conf;
    // 是否已处理完当前文件
    private boolean processed = false;

    /**
     * 构造函数，初始化分片和配置。
     *
     * @param fileSplit 文件分片
     * @param conf      配置
     * @throws IOException 如果初始化失败
     */
    public WholeFileRecordReader(FileSplit fileSplit, Configuration conf)
        throws IOException {
        this.fileSplit = fileSplit;
        this.conf = conf;
    }

    /**
     * 创建键对象。
     */
    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    /**
     * 创建值对象。
     */
    @Override
    public BytesWritable createValue() {
        return new BytesWritable();
    }

    /**
     * 获取当前读取位置。
     */
    @Override
    public long getPos() throws IOException {
        return processed ? fileSplit.getLength() : 0;
    }

    /**
     * 获取读取进度。
     */
    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    /**
     * 读取下一条记录。
     */
    @Override
    public boolean next(NullWritable key, BytesWritable value) throws IOException {
        if (!processed) {
            // 创建字节数组存储文件内容
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                // 读取整个文件内容
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }

    /**
     * 关闭 RecordReader。
     */
    @Override
    public void close() throws IOException {
        // 无需额外资源清理
    }
}
