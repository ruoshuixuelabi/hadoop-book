// == NonSplittableTextInputFormat 不可分割的文本输入格式

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 不可分割的文本输入格式。
 *
 * <p>继承自 TextInputFormat，但覆盖了 isSplittable 方法返回 false，
 * 使得整个文件作为一个单独的输入分片，不会被分割。
 *
 * <p>适用场景：
 * <ul>
 *   <li>处理压缩文件（某些压缩格式不支持分割）</li>
 *   <li>处理需要整体上下文的数据（如 XML、JSON 部分）</li>
 *   <li>处理小文件（避免过多小分片带来的开销）</li>
 *   <li>处理需要保持完整性的记录（如完整的日志块）</li>
 * </ul>
 *
 * <p>注意：当文件不可分割时，一个文件只会被一个 Map 任务处理，
 * 如果文件很大，会导致该任务处理时间较长。
 */
public class NonSplittableTextInputFormat extends TextInputFormat {
    /**
     * 判断文件是否可分割。
     *
     * @param context 作业上下文
     * @param file     文件路径
     * @return 始终返回 false，表示文件不可分割
     */
    @Override
    protected boolean isSplittable(JobContext context, Path file) {
        // 返回 false 使得整个文件作为一个分片
        // 不会根据文件大小或块大小进行分割
        return false;
    }
}
