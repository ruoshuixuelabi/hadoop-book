// == NonSplittableTextInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 不可切分的文本输入格式
 *
 * <p>默认的 {@link TextInputFormat} 会按字节偏移把大文件切成若干片，
 * 每片交给一个 Map 任务
 * <p>若输入不是「一行一条记录」的普通文本——比如是压缩包、
 * 序列化对象或带内部结构的文档——切断就会破坏记录，
 * 此时需要像本例这样禁止切分：整个文件作为一片交给一个 Map 任务
 */
public class NonSplittableTextInputFormat extends TextInputFormat {
  /**
   * 声明本输入格式不可切分
   *
   * @param context 作业上下文
   * @param file 待判断的文件
   * @return 固定返回 {@code false}，即永远不切分
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    // 返回 false 表示整个文件只会生成一个 InputSplit
    return false;
  }
}
