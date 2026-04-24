import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 基于日期范围的PathFilter实现类。
 *
 * <p>此类用于HDFS路径过滤，通过正则表达式匹配路径中的日期部分，
 * 并判断该日期是否在指定的日期区间内。路径格式必须符合 "yyyy/MM/dd" 的日期模式。</p>
 *
 * <p>典型应用场景：</p>
 * <ul>
 *   <li>按日期过滤HDFS日志文件目录</li>
 *   <li>选择特定时间范围内的数据文件</li>
 *   <li>处理按日期组织的HDFS目录结构</li>
 * </ul>
 *
 * <p>使用示例：</p>
 * <pre>
 *   Date start = new Date();
 *   Date end = new Date();
 *   PathFilter filter = new DateRangePathFilter(start, end);
 * </pre>
 *
 * @see PathFilter Hadoop路径过滤器接口
 */
public class DateRangePathFilter implements PathFilter {

  // 正则表达式：匹配路径中的日期部分，格式为 yyyy/MM/dd
  // 捕获组 (\d\d\d\d/\d\d/\d\d) 用于提取具体的日期字符串
  private final Pattern PATTERN = Pattern.compile("^.*/(\\d\\d\\d\\d/\\d\\d/\\d\\d).*$");

  // 过滤的起始日期和结束日期（闭区间）
  private final Date start, end;

  /**
   * 构造一个日期范围路径过滤器。
   *
   * @param start 日期区间的起始日期（包含）
   * @param end   日期区间的结束日期（包含）
   */
  public DateRangePathFilter(Date start, Date end) {
    // 创建副本以防止外部修改影响内部状态
    this.start = new Date(start.getTime());
    this.end = new Date(end.getTime());
  }

  /**
   * 判断指定路径是否在日期范围内。
   *
   * <p>该方法首先使用正则表达式匹配路径，提取路径中的日期部分，
   * 然后判断该日期是否在 [start, end] 区间内。</p>
   *
   * @param path 要检查的HDFS路径
   * @return 如果路径中的日期在指定范围内返回true，否则返回false
   */
  public boolean accept(Path path) {
    Matcher matcher = PATTERN.matcher(path.toString());
    if (matcher.matches()) {
      // 使用yyyy/MM/dd格式解析路径中的日期
      DateFormat format = new SimpleDateFormat("yyyy/MM/dd");
      try {
        return inInterval(format.parse(matcher.group(1)));
      } catch (ParseException e) {
        // 日期解析失败时，拒绝该路径
        return false;
      }
    }
    // 路径不匹配日期格式时，拒绝该路径
    return false;
  }

  /**
   * 检查给定日期是否在闭区间 [start, end] 内。
   *
   * @param date 要检查的日期
   * @return 如果日期在区间内返回true，否则返回false
   */
  private boolean inInterval(Date date) {
    // 条件等价于：date >= start && date <= end
    // date.before(start) 返回true表示date在start之前
    // date.after(end) 返回true表示date在end之后
    return !date.before(start) && !date.after(end);
  }

}
