package oldapi;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * 不可分割的文本输入格式（旧 API）。
 *
 * <p>继承自 TextInputFormat，覆盖 isSplittable 方法返回 false，
 * 使得整个文件作为一个单独的输入分片。
 */
public class NonSplittableTextInputFormat extends TextInputFormat {

    /**
     * 判断文件是否可分割。
     *
     * @param fs    文件系统
     * @param file  文件路径
     * @return 始终返回 false，表示文件不可分割
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }
}
